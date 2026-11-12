package eddy

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// Run is the controller: it campaigns for leadership and, while leading,
// works the queue with the reconcilers given in opts. Start it on any number
// of replicas; only the leader reconciles. When leadership is lost it drains
// and campaigns again. It returns nil once ctx is done and in-flight
// reconciles have finished or timed out.
//
//	db.Run(ctx,
//		eddy.Reconcile(Apps, reconcileApp),
//		eddy.Teardown(Apps, teardownApp),
//	)
func (db *DB) Run(ctx context.Context, opts ...Option) error {
	cfg, err := db.cfg.with(opts)
	if err != nil {
		return err
	}
	rt, err := newRuntime(cfg, db.pool)
	if err != nil {
		return err
	}
	elector := cfg.elector
	if elector == nil {
		elector = AdvisoryLock(db.pool, cfg.queue)
	}
	for {
		lead, release, err := elector.Campaign(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("eddy: campaign: %w", err)
		}
		err = db.lead(lead, rt)
		release()
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		cfg.logger.Warn("eddy: lost leadership, campaigning again")
	}
}

// Leading reports whether Run on this DB currently leads. Wire it to a
// readiness probe if only the leader should count as ready.
func (db *DB) Leading() bool { return db.leading.Load() }

// lead works the queue until lead is cancelled, then drains.
func (db *DB) lead(lead context.Context, rt *runtime) error {
	cfg := rt.cfg
	workers := river.NewWorkers()
	rt.addWorkers(workers)
	// A fresh River client per leadership term.
	rc, err := river.NewClient(riverpgxv5.New(db.pool), &river.Config{
		Logger:       cfg.logger,
		Queues:       rt.queues(),
		Workers:      workers,
		PeriodicJobs: rt.periodicJobs(),
	})
	if err != nil {
		return fmt.Errorf("eddy: river client: %w", err)
	}
	// River's own context must outlive lead so that Stop can drain.
	if err := rc.Start(context.WithoutCancel(lead)); err != nil {
		return fmt.Errorf("eddy: start: %w", err)
	}
	db.setLeading(cfg, true)
	cfg.logger.Info("eddy: leading", "queue", cfg.queue, "kinds", len(rt.handlers))
	<-lead.Done()
	db.setLeading(cfg, false)
	return stop(rc, cfg)
}

func (db *DB) setLeading(cfg *config, v bool) {
	db.leading.Store(v)
	for _, f := range cfg.onLeadership {
		f(v)
	}
}

// stop drains rc, cancelling in-flight reconciles if that takes too long.
func stop(rc *river.Client[pgx.Tx], cfg *config) error {
	stopCtx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()
	if err := rc.Stop(stopCtx); err != nil {
		cfg.logger.Warn("eddy: drain timed out, cancelling in-flight reconciles", "error", err)
		hardCtx, hardCancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
		defer hardCancel()
		if err := rc.StopAndCancel(hardCtx); err != nil {
			return fmt.Errorf("eddy: stop: %w", err)
		}
	}
	return nil
}
