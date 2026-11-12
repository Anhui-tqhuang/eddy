package eddy

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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
	within(ctx context.Context, fn func(tx pgx.Tx, enq *Enqueuer) error) error
}

// DB is a pgx pool that knows how to enqueue reconciles. Use it like the
// pool: Exec, Query and QueryRow pass straight through. Writes made through
// a Table (Insert, Update, Delete) enqueue the reconcile themselves; raw SQL
// through Exec does not.
type DB struct {
	*Enqueuer
	pool *pgxpool.Pool
}

// Open returns a DB on pool. It fails when River's tables are missing or out
// of date; see Migrate. Only WithQueue and WithLogger apply.
func Open(ctx context.Context, pool *pgxpool.Pool, opts ...Option) (*DB, error) {
	enq, err := NewEnqueuer(ctx, pool, opts...)
	if err != nil {
		return nil, err
	}
	return &DB{Enqueuer: enq, pool: pool}, nil
}

// Wrap returns a DB on pool that enqueues through enq, for apps that built
// the Enqueuer themselves (see EnqueuerFor).
func Wrap(pool *pgxpool.Pool, enq *Enqueuer) *DB {
	return &DB{Enqueuer: enq, pool: pool}
}

// Pool returns the underlying pool.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// Exec runs sql on the pool.
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
	return &Tx{Tx: tx, enq: db.Enqueuer}, nil
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
func (db *DB) WithTx(tx pgx.Tx) *Tx { return &Tx{Tx: tx, enq: db.Enqueuer} }

func (db *DB) within(ctx context.Context, fn func(tx pgx.Tx, enq *Enqueuer) error) error {
	return db.InTx(ctx, func(tx *Tx) error { return fn(tx.Tx, tx.enq) })
}

// Tx is a pgx transaction that knows how to enqueue reconciles. It has every
// pgx.Tx method (Exec, Query, Commit, Rollback and the rest).
type Tx struct {
	pgx.Tx
	enq *Enqueuer
}

// Enqueue schedules a reconcile of each ref when the transaction commits.
func (t *Tx) Enqueue(ctx context.Context, refs ...Ref) error {
	return t.enq.EnqueueTx(ctx, t.Tx, refs...)
}

func (t *Tx) within(ctx context.Context, fn func(tx pgx.Tx, enq *Enqueuer) error) error {
	return fn(t.Tx, t.enq)
}

func rollback(ctx context.Context, tx pgx.Tx) error {
	// Roll back even when ctx is already cancelled.
	if err := tx.Rollback(context.WithoutCancel(ctx)); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		return fmt.Errorf("eddy: rollback: %w", err)
	}
	return nil
}
