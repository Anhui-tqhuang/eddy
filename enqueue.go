package eddy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// reconcileJob is the River job behind every enqueue. It carries only which
// object to look at; the row is the source of truth.
type reconcileJob struct {
	Object string          `json:"kind"`
	ID     json.RawMessage `json:"id"`
}

func (reconcileJob) Kind() string { return "eddy.reconcile" }

func (j reconcileJob) ref() Ref { return Ref{kind: j.Object, id: j.ID} }

// resyncJob asks the leader to enqueue every object of one kind.
type resyncJob struct {
	Object string `json:"kind"`
}

func (resyncJob) Kind() string { return "eddy.resync" }

// Enqueuer schedules reconciles. API processes use it to enqueue in the same
// transaction as the write that makes the reconcile necessary. A Controller
// is also an Enqueuer.
type Enqueuer struct {
	river *river.Client[pgx.Tx]
	queue string
}

// NewEnqueuer returns an insert-only Enqueuer for pool. It never works jobs.
// It fails when River's tables are missing or out of date; see Migrate.
func NewEnqueuer(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*Enqueuer, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	return newEnqueuer(ctx, pool, cfg)
}

func newEnqueuer(ctx context.Context, pool *pgxpool.Pool, cfg *config) (*Enqueuer, error) {
	if pool == nil {
		return nil, errors.New("eddy: nil pool")
	}
	if err := checkMigrated(ctx, pool); err != nil {
		return nil, err
	}
	rc, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Logger: cfg.logger})
	if err != nil {
		return nil, fmt.Errorf("eddy: river client: %w", err)
	}
	return &Enqueuer{river: rc, queue: cfg.queue}, nil
}

// EnqueueTx schedules a reconcile of each ref inside tx: the jobs exist only
// if tx commits. This is the call to make next to the write itself.
func (e *Enqueuer) EnqueueTx(ctx context.Context, tx pgx.Tx, refs ...Ref) error {
	params, err := e.params(refs)
	if err != nil || len(params) == 0 {
		return err
	}
	if _, err := e.river.InsertManyTx(ctx, tx, params); err != nil {
		return fmt.Errorf("eddy: enqueue: %w", err)
	}
	return nil
}

// Enqueue schedules a reconcile of each ref outside any transaction.
func (e *Enqueuer) Enqueue(ctx context.Context, refs ...Ref) error {
	params, err := e.params(refs)
	if err != nil || len(params) == 0 {
		return err
	}
	if _, err := e.river.InsertMany(ctx, params); err != nil {
		return fmt.Errorf("eddy: enqueue: %w", err)
	}
	return nil
}

func (e *Enqueuer) params(refs []Ref) ([]river.InsertManyParams, error) {
	params := make([]river.InsertManyParams, 0, len(refs))
	for _, r := range refs {
		if !r.valid() {
			return nil, errors.New("eddy: enqueue: invalid Ref (build one with Kind.Of)")
		}
		params = append(params, river.InsertManyParams{
			Args:       reconcileJob{Object: r.kind, ID: r.id},
			InsertOpts: &river.InsertOpts{Queue: e.queue},
		})
	}
	return params, nil
}

// Migrate creates or upgrades River's tables, which eddy stores its jobs in,
// in pool's current schema. Run it from your migration step; it is a no-op
// when they are current.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	m, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return fmt.Errorf("eddy: migrator: %w", err)
	}
	if _, err := m.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("eddy: migrate: %w", err)
	}
	return nil
}

func checkMigrated(ctx context.Context, pool *pgxpool.Pool) error {
	m, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return fmt.Errorf("eddy: migrator: %w", err)
	}
	res, err := m.Validate(ctx, nil)
	if err != nil {
		return fmt.Errorf("eddy: validate migrations: %w", err)
	}
	if !res.OK {
		return errors.New("eddy: river migrations pending (run eddy.Migrate): " + strings.Join(res.Messages, "; "))
	}
	return nil
}
