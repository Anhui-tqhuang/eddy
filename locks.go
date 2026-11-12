package eddy

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// busyRetry is how soon a reconcile is retried when another replica holds
// its object lock. Snoozing does not use up attempts.
const busyRetry = time.Second

// objectLocker keeps two reconciles of one object from overlapping.
type objectLocker interface {
	// acquire returns busy=true, without waiting, when another holder has
	// the lock and waiting would only tie up a worker.
	acquire(ctx context.Context, ref Ref) (release func(), busy bool, err error)
}

// WithObjectLocks makes eddy take a Postgres advisory lock per object for the
// length of each reconcile, so no two processes ever reconcile one object at
// once, even across a leadership handoff or when several processes work the
// queue (see Embed). Each running reconcile holds one pool connection while
// it runs, on top of whatever the reconciler uses: size the pool for at
// least twice the worker count.
//
// Without it, eddy serialises reconciles of an object within one process.
func WithObjectLocks() Option {
	return func(c *config) error {
		c.objectLocks = true
		return nil
	}
}

// processLocks serialises per object within this process. It waits rather
// than snoozing, since the holder is local and will finish soon.
type processLocks struct {
	mu   sync.Mutex
	held map[string]chan struct{}
}

func newProcessLocks() *processLocks { return &processLocks{held: map[string]chan struct{}{}} }

func (k *processLocks) acquire(ctx context.Context, ref Ref) (func(), bool, error) {
	key := ref.String()
	for {
		k.mu.Lock()
		ch, busy := k.held[key]
		if !busy {
			ch = make(chan struct{})
			k.held[key] = ch
			k.mu.Unlock()
			return func() {
				k.mu.Lock()
				delete(k.held, key)
				k.mu.Unlock()
				close(ch)
			}, false, nil
		}
		k.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
}

// advisoryLocks takes a session advisory lock per object on a dedicated pool
// connection, layered over processLocks so local duplicates wait instead of
// spinning on the database.
type advisoryLocks struct {
	pool  *pgxpool.Pool
	local *processLocks
}

func (a *advisoryLocks) acquire(ctx context.Context, ref Ref) (func(), bool, error) {
	releaseLocal, _, err := a.local.acquire(ctx, ref)
	if err != nil {
		return nil, false, err
	}
	conn, err := a.pool.Acquire(ctx)
	if err != nil {
		releaseLocal()
		return nil, false, fmt.Errorf("object lock: acquire connection: %w", err)
	}
	key := objectKey(ref)
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&got); err != nil {
		conn.Release()
		releaseLocal()
		return nil, false, fmt.Errorf("object lock: %w", err)
	}
	if !got {
		conn.Release()
		releaseLocal()
		return nil, true, nil
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", key); err != nil {
			// The session may still hold the lock: drop it rather than
			// return it to the pool.
			_ = conn.Conn().Close(ctx)
		}
		conn.Release()
		releaseLocal()
	}, false, nil
}

func objectKey(ref Ref) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("eddy/object:" + ref.String())) // fnv never fails
	return int64(h.Sum64())
}
