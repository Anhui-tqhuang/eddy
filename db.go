package eddy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// Querier runs SQL. *pgxpool.Pool, pgx.Tx, *DB and *Tx all satisfy it; Table
// reads accept any of them.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Session is where Table writes happen: a *DB (each write runs in its own
// transaction) or a *Tx (writes join that transaction). Either way the row
// and its reconcile job are written atomically.
type Session interface {
	Querier
	within(ctx context.Context, fn func(tx pgx.Tx, db *DB) error) error
}

// DB is eddy's handle on your database, used by the API and the controller
// alike. Use it like a pgx pool: Exec, Query and QueryRow pass straight
// through. Writes made through a Table enqueue the row's reconcile
// themselves, and Run works those reconciles.
type DB struct {
	pool    *pgxpool.Pool
	river   *river.Client[pgx.Tx]
	cfg     *config
	leading atomic.Bool
}

// Open returns a DB on pool. Unless WithoutMigrate is given, it first creates
// or upgrades River's tables, which hold eddy's jobs.
func Open(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*DB, error) {
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, errors.New("eddy: Open: nil pool")
	}
	if err := prepare(ctx, pool, cfg.migrate); err != nil {
		return nil, err
	}
	rc, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Logger: cfg.logger})
	if err != nil {
		return nil, fmt.Errorf("eddy: river client: %w", err)
	}
	return &DB{pool: pool, river: rc, cfg: cfg}, nil
}

// Attach returns a DB on pool that enqueues through a River client you
// already run, instead of opening its own. It does not migrate. See Embed
// for working eddy's jobs on that client too.
func Attach(pool *pgxpool.Pool, client *river.Client[pgx.Tx], opts ...Option) (*DB, error) {
	if pool == nil || client == nil {
		return nil, errors.New("eddy: Attach: nil pool or client")
	}
	cfg, err := newConfig(opts)
	if err != nil {
		return nil, err
	}
	return &DB{pool: pool, river: client, cfg: cfg}, nil
}

// Pool returns the underlying pool.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// Exec runs sql on the pool. It enqueues nothing; see Table.Exec for writes
// to a reconciled table.
func (db *DB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return db.pool.Exec(ctx, sql, args...)
}

// Query runs sql on the pool.
func (db *DB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return db.pool.Query(ctx, sql, args...)
}

// QueryRow runs sql on the pool.
func (db *DB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return db.pool.QueryRow(ctx, sql, args...)
}

// Begin starts a transaction. Table writes made through the returned Tx
// commit or roll back with it, jobs included.
func (db *DB) Begin(ctx context.Context) (*Tx, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("eddy: begin: %w", err)
	}
	return &Tx{Tx: tx, db: db}, nil
}

// InTx runs fn in a transaction: it commits when fn returns nil and rolls
// back otherwise.
func (db *DB) InTx(ctx context.Context, fn func(tx *Tx) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return errors.Join(err, rollback(ctx, tx.Tx))
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("eddy: commit: %w", err)
	}
	return nil
}

// WithTx adapts a pgx transaction you already hold, so Table writes can join
// it.
func (db *DB) WithTx(tx pgx.Tx) *Tx { return &Tx{Tx: tx, db: db} }

// Enqueue schedules a reconcile of each ref. Table writes do this for you;
// it is for kinds registered with Handle, or to nudge a row by hand.
func (db *DB) Enqueue(ctx context.Context, refs ...Ref) error {
	jobs, err := jobsFor(refs)
	if err != nil {
		return err
	}
	return db.insert(ctx, nil, jobs)
}

func (db *DB) within(ctx context.Context, fn func(tx pgx.Tx, db *DB) error) error {
	return db.InTx(ctx, func(tx *Tx) error { return fn(tx.Tx, db) })
}

// insert enqueues jobs, inside tx when it is not nil.
func (db *DB) insert(ctx context.Context, tx pgx.Tx, jobs []reconcileJob) error {
	jobs = withoutDuplicates(jobs)
	var q querier = db.pool
	if tx != nil {
		q = tx
	}
	for start := 0; start < len(jobs); start += insertBatch {
		batch := jobs[start:min(start+insertBatch, len(jobs))]
		// An object with a waiting job gets no second one (see merge.go): its job is
		// brought forward when it waits for later.
		waiting, err := waitingJobs(ctx, q, batch, 0)
		if err != nil {
			return err
		}
		params := make([]river.InsertManyParams, 0, len(batch))
		for i, j := range batch {
			w, ok := waiting[i]
			switch {
			case !ok:
				params = append(params, river.InsertManyParams{Args: j, InsertOpts: j.insertOpts(db.cfg.queue)})
			case w.later:
				if err := db.bringForward(ctx, tx, w.id); err != nil {
					return err
				}
			}
		}
		if len(params) == 0 {
			continue
		}
		if tx != nil {
			_, err = db.river.InsertManyTx(ctx, tx, params)
		} else {
			_, err = db.river.InsertMany(ctx, params)
		}
		if err != nil {
			return fmt.Errorf("eddy: enqueue: %w", err)
		}
	}
	return nil
}

// bringForward makes a waiting job runnable now.
func (db *DB) bringForward(ctx context.Context, tx pgx.Tx, id int64) error {
	var err error
	if tx != nil {
		_, err = db.river.JobRetryTx(ctx, tx, id)
	} else {
		_, err = db.river.JobRetry(ctx, id)
	}
	if err != nil {
		return fmt.Errorf("eddy: bring job %d forward: %w", id, err)
	}
	return nil
}

func jobsFor(refs []Ref) ([]reconcileJob, error) {
	jobs := make([]reconcileJob, len(refs))
	for i, r := range refs {
		if !r.valid() {
			return nil, errors.New("eddy: enqueue: invalid Ref (build one with Kind.Of or Table.Of)")
		}
		jobs[i] = reconcileJob{Object: r.kind, ID: r.id, set: r.set}
	}
	return jobs, nil
}

// Tx is a pgx transaction that knows how to enqueue reconciles. It has every
// pgx.Tx method (Exec, Query, Commit, Rollback and the rest).
type Tx struct {
	pgx.Tx
	db *DB
}

// Enqueue schedules a reconcile of each ref when the transaction commits.
func (t *Tx) Enqueue(ctx context.Context, refs ...Ref) error {
	jobs, err := jobsFor(refs)
	if err != nil {
		return err
	}
	return t.db.insert(ctx, t.Tx, jobs)
}

func (t *Tx) within(ctx context.Context, fn func(tx pgx.Tx, db *DB) error) error {
	return fn(t.Tx, t.db)
}

func rollback(ctx context.Context, tx pgx.Tx) error {
	// Roll back even when ctx is already cancelled.
	if err := tx.Rollback(context.WithoutCancel(ctx)); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("eddy: rollback: %w", err)
	}
	return nil
}

// migrateLock serialises migrations when several processes start at once.
const migrateLock int64 = 0x6564_6479_6d69_6772 // "eddymigr"

// Migrate creates or upgrades River's tables in pool's current schema. Open
// does this for you; call it yourself only together with WithoutMigrate, to
// run migrations as a separate step. It is a no-op when they are current.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return prepare(ctx, pool, true)
}

// prepare checks River's tables and, when migrate is set, brings them up to
// date.
func prepare(ctx context.Context, pool *pgxpool.Pool, migrate bool) error {
	m, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return fmt.Errorf("eddy: migrator: %w", err)
	}
	res, err := m.Validate(ctx, nil)
	if err != nil {
		return fmt.Errorf("eddy: validate migrations: %w", err)
	}
	if res.OK {
		return nil
	}
	if !migrate {
		return errors.New("eddy: river migrations pending (run eddy.Migrate): " + strings.Join(res.Messages, "; "))
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("eddy: migrate: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrateLock); err != nil {
		return fmt.Errorf("eddy: migrate: lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", migrateLock)
	}()
	if _, err := m.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("eddy: migrate: %w", err)
	}
	return nil
}
