package eddy_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Anhui-tqhuang/eddy"
)

const waitFor = 10 * time.Second

var quiet = eddy.WithLogger(slog.New(slog.DiscardHandler))

// testPool returns a pool on a fresh, empty schema. It skips the test unless
// EDDY_TEST_DATABASE_URL is set.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("EDDY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("EDDY_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	schema := "eddy_" + uuid.NewString()[:8]
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(context.Background())
	})
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// testDB opens a DB on a fresh schema, running ddl first.
func testDB(t *testing.T, ddl ...string) *eddy.DB {
	t.Helper()
	pool := testPool(t)
	for _, stmt := range ddl {
		if _, err := pool.Exec(context.Background(), stmt); err != nil {
			t.Fatal(err)
		}
	}
	db, err := eddy.Open(context.Background(), pool, quiet)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// run starts db.Run as the only replica and stops it when the test ends.
func run(t *testing.T, db *eddy.DB, opts ...eddy.Option) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	opts = append([]eddy.Option{eddy.WithElector(eddy.Solo())}, opts...)
	go func() { done <- db.Run(ctx, opts...) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	})
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// seenLog is a concurrency-safe list of what a reconciler saw.
type seenLog[E any] struct {
	mu    sync.Mutex
	items []E
}

func (l *seenLog[E]) add(e E) {
	l.mu.Lock()
	l.items = append(l.items, e)
	l.mu.Unlock()
}

func (l *seenLog[E]) all() []E {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]E(nil), l.items...)
}

func (l *seenLog[E]) n() int { return len(l.all()) }

func count[E comparable](l *seenLog[E], want E) int {
	n := 0
	for _, e := range l.all() {
		if e == want {
			n++
		}
	}
	return n
}

func jobCount(t *testing.T, q eddy.Querier, where string) int {
	t.Helper()
	var n int
	if err := q.QueryRow(context.Background(), "SELECT count(*) FROM river_job WHERE "+where).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
