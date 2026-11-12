package eddy

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Elector decides which replica works the queue. Only the leader runs
// reconcilers, so one object is never reconciled by two replicas at once.
type Elector interface {
	// Campaign blocks until this replica leads or ctx is done. The returned
	// context is cancelled when leadership is lost. The controller calls
	// release once its reconcilers have stopped; only then may another
	// replica take over.
	Campaign(ctx context.Context) (lead context.Context, release func(), err error)
}

// Solo returns an Elector that always leads at once. Use it when exactly one
// replica runs, or in tests.
func Solo() Elector { return solo{} }

type solo struct{}

func (solo) Campaign(ctx context.Context) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	lead, cancel := context.WithCancel(ctx)
	return lead, cancel, nil
}

// AdvisoryLockOption configures AdvisoryLock.
type AdvisoryLockOption func(*advisoryLock)

// WithLockRetry sets how often a follower retries the lock and how often the
// leader checks its connection. The default is 2s.
func WithLockRetry(d time.Duration) AdvisoryLockOption {
	return func(a *advisoryLock) {
		if d > 0 {
			a.retry = d
		}
	}
}

// AdvisoryLock returns an Elector backed by a session-level Postgres advisory
// lock keyed on name. The leader holds one pool connection for as long as it
// leads; if that connection fails, leadership is lost. It needs nothing but
// the database eddy already uses.
func AdvisoryLock(pool *pgxpool.Pool, name string, opts ...AdvisoryLockOption) Elector {
	h := fnv.New64a()
	_, _ = h.Write([]byte("eddy:" + name)) // fnv never fails
	a := &advisoryLock{pool: pool, key: int64(h.Sum64()), retry: 2 * time.Second}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

type advisoryLock struct {
	pool  *pgxpool.Pool
	key   int64
	retry time.Duration
}

func (a *advisoryLock) Campaign(ctx context.Context) (context.Context, func(), error) {
	t := time.NewTicker(a.retry)
	defer t.Stop()
	for {
		lead, release, err := a.try(ctx)
		if err != nil || lead != nil {
			return lead, release, err
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-t.C:
		}
	}
}

// try attempts the lock once. It returns a nil context, and no error, when
// another replica holds it or the database is briefly unreachable.
func (a *advisoryLock) try(ctx context.Context) (context.Context, func(), error) {
	conn, err := a.pool.Acquire(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		return nil, nil, nil
	}
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", a.key).Scan(&got); err != nil || !got {
		conn.Release()
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		return nil, nil, nil
	}

	lead, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(a.retry)
		defer t.Stop()
		for {
			select {
			case <-lead.Done():
				return
			case <-t.C:
				if err := conn.Ping(lead); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	release := func() {
		cancel()
		<-done
		unlockCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", a.key); err != nil {
			// The session may still hold the lock: close it rather than
			// return it to the pool.
			_ = conn.Conn().Close(unlockCtx)
		}
		conn.Release()
	}
	return lead, release, nil
}

var errNoElector = errors.New("eddy: no elector")

func (c *config) electorFor(pool *pgxpool.Pool) (Elector, error) {
	if c.elector != nil {
		return c.elector, nil
	}
	if pool == nil {
		return nil, fmt.Errorf("%w: nil pool", errNoElector)
	}
	return AdvisoryLock(pool, c.queue), nil
}
