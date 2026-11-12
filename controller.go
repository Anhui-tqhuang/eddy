package eddy

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// Controller runs the registered reconcilers. Start one per replica: replicas
// elect a leader through the Elector, and only the leader works the queue.
// A Controller is also an Enqueuer.
type Controller struct {
	*Enqueuer
	pool    *pgxpool.Pool
	cfg     *config
	elector Elector
	rt      *runtime
	leading atomic.Bool
}

// New returns a Controller for pool. Register at least one reconciler with
// Handle or HandleFunc. It fails when River's tables are missing or out of
// date; see Migrate.
func New(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*Controller, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	if err := cfg.validateWorkers(); err != nil {
		return nil, err
	}
	el, err := cfg.electorFor(pool)
	if err != nil {
		return nil, err
	}
	enq, err := newEnqueuer(ctx, pool, cfg)
	if err != nil {
		return nil, err
	}
	return &Controller{Enqueuer: enq, pool: pool, cfg: cfg, elector: el, rt: newRuntime(cfg, pool)}, nil
}

// DB returns a DB on the controller's pool, for writing rows through a Table
// from the controller process.
func (c *Controller) DB() *DB { return Wrap(c.pool, c.Enqueuer) }

// Leading reports whether this replica currently leads. Wire it to a
// readiness probe if only the leader should count as ready.
func (c *Controller) Leading() bool { return c.leading.Load() }

// Run campaigns for leadership and, while leading, works the queue. When
// leadership is lost it drains and campaigns again. It returns nil once ctx
// is done and in-flight reconciles have finished or timed out.
func (c *Controller) Run(ctx context.Context) error {
	for {
		lead, release, err := c.elector.Campaign(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("eddy: campaign: %w", err)
		}
		err = c.lead(lead)
		release()
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		c.cfg.logger.Warn("eddy: lost leadership, campaigning again")
	}
}

// lead works the queue until lead is cancelled, then drains.
func (c *Controller) lead(lead context.Context) error {
	rc, err := c.workingClient()
	if err != nil {
		return err
	}
	// River's own context must outlive lead so that Stop can drain.
	if err := rc.Start(context.WithoutCancel(lead)); err != nil {
		return fmt.Errorf("eddy: start: %w", err)
	}
	c.setLeading(true)
	c.cfg.logger.Info("eddy: leading", "queue", c.cfg.queue, "kinds", len(c.cfg.handlers))
	<-lead.Done()
	c.setLeading(false)

	stopCtx, cancel := context.WithTimeout(context.Background(), c.cfg.shutdownTimeout)
	defer cancel()
	if err := rc.Stop(stopCtx); err != nil {
		c.cfg.logger.Warn("eddy: drain timed out, cancelling in-flight reconciles", "error", err)
		hardCtx, hardCancel := context.WithTimeout(context.Background(), c.cfg.shutdownTimeout)
		defer hardCancel()
		if err := rc.StopAndCancel(hardCtx); err != nil {
			return fmt.Errorf("eddy: stop: %w", err)
		}
	}
	return nil
}

func (c *Controller) setLeading(v bool) {
	c.leading.Store(v)
	for _, f := range c.cfg.onLeadership {
		f(v)
	}
}

// workingClient builds a fresh River client for one leadership term.
func (c *Controller) workingClient() (*river.Client[pgx.Tx], error) {
	workers := river.NewWorkers()
	c.rt.addWorkers(workers)
	rc, err := river.NewClient(riverpgxv5.New(c.pool), &river.Config{
		Logger:       c.cfg.logger,
		Queues:       map[string]river.QueueConfig{c.cfg.queue: {MaxWorkers: c.cfg.workers}},
		Workers:      workers,
		PeriodicJobs: c.rt.periodicJobs(),
	})
	if err != nil {
		return nil, fmt.Errorf("eddy: river client: %w", err)
	}
	return rc, nil
}
