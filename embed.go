package eddy

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Embedded plugs eddy into a River client you already run, instead of
// DB.Run starting its own. Register its workers, queue and periodic jobs on
// your client, then get the DB from it:
//
//	ed, _ := eddy.Embed(pool, eddy.Reconcile(Apps, reconcileApp))
//
//	workers := river.NewWorkers()
//	ed.AddWorkers(workers)
//	// ... your own workers ...
//	queues := ed.Queues()
//	queues[river.QueueDefault] = river.QueueConfig{MaxWorkers: 50}
//	client, _ := river.NewClient(riverpgxv5.New(pool), &river.Config{
//		Queues:       queues,
//		Workers:      workers,
//		PeriodicJobs: ed.PeriodicJobs(),
//	})
//	db := ed.DB(client)
//
// There is no eddy leader election here: every process that lists eddy's
// queues works them. Reconciles of one object are serialised within a process;
// add WithObjectLocks to serialise them across processes too. Resync jobs
// run on River's own elected leader.
type Embedded struct {
	rt *runtime
}

// Embed returns eddy's workers for use with your own River client.
// WithElector, OnLeadershipChange, WithShutdownTimeout and WithoutMigrate do
// not apply: your client owns its lifecycle and you run River's migrations.
func Embed(pool *pgxpool.Pool, opts ...Option) (*Embedded, error) {
	if pool == nil {
		return nil, errors.New("eddy: Embed: nil pool")
	}
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	rt, err := newRuntime(cfg, pool)
	if err != nil {
		return nil, err
	}
	return &Embedded{rt: rt}, nil
}

// AddWorkers registers eddy's workers on ws.
func (e *Embedded) AddWorkers(ws *river.Workers) { e.rt.addWorkers(ws) }

// Queues are eddy's queues and their sizes: the shared one, sized by
// WithWorkers, plus one per kind declared with MaxWorkers. Add them all to
// your client's Queues.
func (e *Embedded) Queues() map[string]river.QueueConfig { return e.rt.queues() }

// PeriodicJobs are the resync jobs; add them to your client's PeriodicJobs.
func (e *Embedded) PeriodicJobs() []*river.PeriodicJob { return e.rt.periodicJobs() }

// DB returns the DB that enqueues through client.
func (e *Embedded) DB(client *river.Client[pgx.Tx]) *DB {
	return &DB{pool: e.rt.pool, river: client, cfg: e.rt.cfg}
}
