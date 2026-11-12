package eddy

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// Embedded plugs eddy into a River client you already run, instead of
// eddy running its own (see New). Register its workers, queue and periodic
// jobs on your client:
//
//	ed, _ := eddy.Embed(pool, eddy.HandleFunc(Apps, reconcileApp))
//
//	workers := river.NewWorkers()
//	ed.AddWorkers(workers)
//	// ... your own workers ...
//	client, _ := river.NewClient(riverpgxv5.New(pool), &river.Config{
//		Queues:       map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 50}, ed.Queue(): ed.QueueConfig()},
//		Workers:      workers,
//		PeriodicJobs: ed.PeriodicJobs(),
//	})
//	api, _ := eddy.EnqueuerFor(client)
//
// There is no eddy leader election here: every process that lists the eddy
// queue works it. Reconciles of one object are serialised within a process;
// add WithObjectLocks to serialise them across processes too. Resync jobs
// run on River's own elected leader.
type Embedded struct {
	rt *runtime
}

// Embed returns eddy's workers for use with your own River client. pool is
// used by table reconcilers (Reconcile, ResyncAll) and WithObjectLocks, and
// may be nil without them. WithElector,
// OnLeadershipChange and WithShutdownTimeout do not apply: your client owns
// its lifecycle.
func Embed(pool *pgxpool.Pool, opts ...Option) (*Embedded, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	if err := cfg.validateWorkers(); err != nil {
		return nil, err
	}
	if cfg.objectLocks && pool == nil {
		return nil, errors.New("eddy: Embed: WithObjectLocks needs a pool")
	}
	if cfg.needsPool && pool == nil {
		return nil, errors.New("eddy: Embed: table reconcilers need a pool")
	}
	return &Embedded{rt: newRuntime(cfg, pool)}, nil
}

// AddWorkers registers eddy's workers on ws.
func (e *Embedded) AddWorkers(ws *river.Workers) { e.rt.addWorkers(ws) }

// Queue is the queue eddy's jobs go to; list it in your client's Queues.
func (e *Embedded) Queue() string { return e.rt.cfg.queue }

// QueueConfig sizes the eddy queue from WithWorkers.
func (e *Embedded) QueueConfig() river.QueueConfig {
	return river.QueueConfig{MaxWorkers: e.rt.cfg.workers}
}

// PeriodicJobs are the Resync jobs; add them to your client's PeriodicJobs.
func (e *Embedded) PeriodicJobs() []*river.PeriodicJob { return e.rt.periodicJobs() }

// EnqueuerFor returns an Enqueuer that inserts through your River client.
// Only WithQueue and WithLogger apply; WithQueue must match the controller.
func EnqueuerFor(client *river.Client[pgx.Tx], opts ...Option) (*Enqueuer, error) {
	if client == nil {
		return nil, errors.New("eddy: EnqueuerFor: nil client")
	}
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	return &Enqueuer{river: client, queue: cfg.queue}, nil
}
