package eddy

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// runtime is what the workers share: config, the merged handlers, object
// locks and the middleware-wrapped handler.
type runtime struct {
	cfg      *config
	pool     *pgxpool.Pool
	handlers map[string]handler
	resyncs  map[string]resync
	locks    objectLocker
	chain    Handler
}

func newRuntime(cfg *config, pool *pgxpool.Pool) (*runtime, error) {
	handlers, resyncs, err := cfg.plan()
	if err != nil {
		return nil, err
	}
	rt := &runtime{cfg: cfg, pool: pool, handlers: handlers, resyncs: resyncs}
	if cfg.objectLocks {
		rt.locks = &advisoryLocks{pool: pool, local: newProcessLocks()}
	} else {
		rt.locks = newProcessLocks()
	}
	h := rt.reconcile
	for i := len(cfg.middleware) - 1; i >= 0; i-- {
		h = cfg.middleware[i](h)
	}
	rt.chain = h
	return rt, nil
}

type jobKey struct{}

// reconcile is the innermost handler: it runs the kind's reconciler and
// records the outcome.
func (rt *runtime) reconcile(ctx context.Context, ref Ref) error {
	h, ok := rt.handlers[ref.kind]
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownKind, ref.kind)
	}
	job, _ := ctx.Value(jobKey{}).(*river.Job[reconcileJob])
	start := time.Now()
	err := h(ctx, rt.pool, job.Args)
	rt.record(ctx, ref, job, err, time.Since(start))
	return err
}

func (rt *runtime) record(ctx context.Context, ref Ref, job *river.Job[reconcileJob], err error, took time.Duration) {
	rec, ok := rt.cfg.recorders[ref.kind]
	if !ok {
		return
	}
	o := Outcome{Attempt: job.Attempt, Duration: took}
	if after, retry := RetryDelay(err); retry && !IsPermanent(err) {
		o.RetryAfter = after
	} else {
		o.Err = err
		o.Final = err == nil || IsPermanent(err) || job.Attempt >= job.MaxAttempts
	}
	// Record even when the reconcile timed out or was cancelled.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if rerr := rec(rctx, ref.id, o); rerr != nil {
		rt.cfg.logger.WarnContext(ctx, "eddy: record status failed", "ref", ref.String(), "error", rerr)
	}
}

// reconcileWorker is the River worker for reconcile jobs.
type reconcileWorker struct {
	river.WorkerDefaults[reconcileJob]
	rt *runtime
}

func (w *reconcileWorker) Timeout(*river.Job[reconcileJob]) time.Duration {
	return w.rt.cfg.jobTimeout
}

func (w *reconcileWorker) Work(ctx context.Context, job *river.Job[reconcileJob]) error {
	ref := job.Args.ref()

	release, busy, err := w.rt.locks.acquire(ctx, ref)
	if err != nil {
		return err
	}
	if busy {
		return river.JobSnooze(busyRetry)
	}
	ctx = withAttempt(ctx, job.Attempt)
	ctx = context.WithValue(ctx, jobKey{}, job)
	err = w.rt.chain(ctx, ref)
	release()

	if err == nil {
		return nil
	}
	if IsPermanent(err) {
		w.rt.cfg.logger.WarnContext(ctx, "eddy: reconcile failed for good", "ref", ref.String(), "error", err)
		return river.JobCancel(err)
	}
	if after, retry := RetryDelay(err); retry {
		return river.JobSnooze(after)
	}
	w.rt.cfg.logger.WarnContext(ctx, "eddy: reconcile failed", "ref", ref.String(), "attempt", job.Attempt, "error", err)
	return fmt.Errorf("reconcile %s: %w", ref, err)
}

// resyncWorker enqueues every object of one kind.
type resyncWorker struct {
	river.WorkerDefaults[resyncJob]
	rt *runtime
}

func (w *resyncWorker) Work(ctx context.Context, job *river.Job[resyncJob]) error {
	kind := job.Args.Object
	rs, ok := w.rt.resyncs[kind]
	if !ok {
		return river.JobCancel(fmt.Errorf("%w %q", ErrUnknownKind, kind))
	}
	ids, err := rs.list(ctx, w.rt.pool)
	if err != nil {
		return fmt.Errorf("resync %s: list: %w", kind, err)
	}
	client := river.ClientFromContext[pgx.Tx](ctx)
	for start := 0; start < len(ids); start += insertBatch {
		batch := ids[start:min(start+insertBatch, len(ids))]
		params := make([]river.InsertManyParams, len(batch))
		for i, id := range batch {
			j := reconcileJob{Object: kind, ID: id, set: w.rt.cfg.settings[kind]}
			params[i] = river.InsertManyParams{Args: j, InsertOpts: j.insertOpts(w.rt.cfg.queue)}
		}
		if _, err := client.InsertMany(ctx, params); err != nil {
			return fmt.Errorf("resync %s: enqueue: %w", kind, err)
		}
	}
	w.rt.cfg.logger.DebugContext(ctx, "eddy: resync enqueued", "kind", kind, "count", len(ids))
	return nil
}

// addWorkers registers eddy's workers on ws.
func (rt *runtime) addWorkers(ws *river.Workers) {
	river.AddWorker(ws, &reconcileWorker{rt: rt})
	river.AddWorker(ws, &resyncWorker{rt: rt})
}

// queues returns the shared queue plus one per kind declared with
// MaxWorkers.
func (rt *runtime) queues() map[string]river.QueueConfig {
	qs := map[string]river.QueueConfig{rt.cfg.queue: {MaxWorkers: rt.cfg.workers}}
	for kind := range rt.handlers {
		if set := rt.cfg.settings[kind]; set.workers > 0 {
			qs[queueFor(rt.cfg.queue, kind, set)] = river.QueueConfig{MaxWorkers: set.workers}
		}
	}
	return qs
}

// periodicJobs returns one resync job per resynced kind.
func (rt *runtime) periodicJobs() []*river.PeriodicJob {
	jobs := make([]*river.PeriodicJob, 0, len(rt.resyncs))
	for kind, rs := range rt.resyncs {
		jobs = append(jobs, river.NewPeriodicJob(
			river.PeriodicInterval(rs.every),
			func() (river.JobArgs, *river.InsertOpts) {
				return resyncJob{Object: kind}, &river.InsertOpts{Queue: rt.cfg.queue}
			},
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
	return jobs
}
