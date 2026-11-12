package eddy

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// ErrUnknownKind is returned (and retried) for a job whose kind has no
// handler, so jobs enqueued ahead of a rollout are not lost.
var ErrUnknownKind = errors.New("eddy: no reconciler registered for kind")

// runtime is what the workers share: config, object locks and the
// middleware-wrapped handler.
type runtime struct {
	cfg   *config
	pool  *pgxpool.Pool
	locks objectLocker
	chain Handler
}

func newRuntime(cfg *config, pool *pgxpool.Pool) *runtime {
	rt := &runtime{cfg: cfg, pool: pool}
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
	return rt
}

// reconcile is the innermost handler: it runs the kind's reconciler and
// records the outcome.
func (rt *runtime) reconcile(ctx context.Context, ref Ref) (Result, error) {
	h, ok := rt.cfg.handlers[ref.kind]
	if !ok {
		return Done, fmt.Errorf("%w %q", ErrUnknownKind, ref.kind)
	}
	start := time.Now()
	res, err := h(ctx, rt.pool, ref.id)
	rt.record(ctx, ref, res, err, time.Since(start))
	return res, err
}

func (rt *runtime) record(ctx context.Context, ref Ref, res Result, err error, took time.Duration) {
	rec, ok := rt.cfg.recorders[ref.kind]
	if !ok {
		return
	}
	attempt := Attempt(ctx)
	o := Outcome{
		Err:      err,
		Result:   res,
		Attempt:  attempt,
		Final:    err == nil || IsPermanent(err) || attempt >= maxAttempts(ctx),
		Duration: took,
	}
	// Record even when the reconcile timed out or was cancelled.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if rerr := rec(rctx, ref.id, o); rerr != nil {
		rt.cfg.logger.WarnContext(ctx, "eddy: record status failed", "ref", ref.String(), "error", rerr)
	}
}

type maxAttemptsKey struct{}

func maxAttempts(ctx context.Context) int {
	n, ok := ctx.Value(maxAttemptsKey{}).(int)
	if !ok {
		return river.MaxAttemptsDefault
	}
	return n
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
	log := w.rt.cfg.logger

	release, busy, err := w.rt.locks.acquire(ctx, ref)
	if err != nil {
		return err
	}
	if busy {
		return river.JobSnooze(busyRetry)
	}
	ctx = withAttempt(ctx, job.Attempt)
	ctx = context.WithValue(ctx, maxAttemptsKey{}, job.MaxAttempts)
	res, err := w.rt.chain(ctx, ref)
	release()

	if err != nil {
		log.WarnContext(ctx, "eddy: reconcile failed", "ref", ref.String(), "attempt", job.Attempt, "error", err)
		if IsPermanent(err) {
			return river.JobCancel(err)
		}
		return fmt.Errorf("reconcile %s: %w", ref, err)
	}
	if res.RequeueAfter > 0 {
		client := river.ClientFromContext[pgx.Tx](ctx)
		_, err := client.Insert(ctx, job.Args, &river.InsertOpts{
			Queue:       w.rt.cfg.queue,
			ScheduledAt: time.Now().Add(res.RequeueAfter),
		})
		if err != nil {
			return fmt.Errorf("requeue %s: %w", ref, err)
		}
	}
	return nil
}

// resyncWorker enqueues every object of one kind.
type resyncWorker struct {
	river.WorkerDefaults[resyncJob]
	rt *runtime
}

func (w *resyncWorker) Work(ctx context.Context, job *river.Job[resyncJob]) error {
	cfg := w.rt.cfg
	rs, ok := cfg.resyncs[job.Args.Object]
	if !ok {
		return river.JobCancel(fmt.Errorf("%w %q", ErrUnknownKind, job.Args.Object))
	}
	ids, err := rs.list(ctx, w.rt.pool)
	if err != nil {
		return fmt.Errorf("resync %s: list: %w", job.Args.Object, err)
	}
	client := river.ClientFromContext[pgx.Tx](ctx)
	for start := 0; start < len(ids); start += defaultResyncBatch {
		end := min(start+defaultResyncBatch, len(ids))
		params := make([]river.InsertManyParams, 0, end-start)
		for _, id := range ids[start:end] {
			params = append(params, river.InsertManyParams{
				Args:       reconcileJob{Object: job.Args.Object, ID: id},
				InsertOpts: &river.InsertOpts{Queue: cfg.queue},
			})
		}
		if _, err := client.InsertMany(ctx, params); err != nil {
			return fmt.Errorf("resync %s: enqueue: %w", job.Args.Object, err)
		}
	}
	cfg.logger.DebugContext(ctx, "eddy: resync enqueued", "kind", job.Args.Object, "count", len(ids))
	return nil
}

// addWorkers registers eddy's workers on ws.
func (rt *runtime) addWorkers(ws *river.Workers) {
	river.AddWorker(ws, &reconcileWorker{rt: rt})
	river.AddWorker(ws, &resyncWorker{rt: rt})
}

// periodicJobs returns one resync job per Resync option.
func (rt *runtime) periodicJobs() []*river.PeriodicJob {
	jobs := make([]*river.PeriodicJob, 0, len(rt.cfg.resyncs))
	for kind, rs := range rt.cfg.resyncs {
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
