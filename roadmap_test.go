package eddy_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/Anhui-tqhuang/eddy"
)

func TestColumnStatus_noColumns_panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	eddy.ColumnStatus[int64](&pgxpool.Pool{}, eddy.StatusColumns{Table: "apps"})
}

func TestEmbed_invalidOptions_fail(t *testing.T) {
	apps := eddy.NewKind[int64]("app")
	noop := func(context.Context, int64) (eddy.Result, error) { return eddy.Done, nil }
	rec := func(context.Context, int64, eddy.Outcome) error { return nil }
	tests := []struct {
		name string
		opts []eddy.Option
	}{
		{"no_handlers", nil},
		{"record_without_handler", []eddy.Option{eddy.HandleFunc(eddy.NewKind[int64]("team"), noop), eddy.Record(apps, rec)}},
		{"object_locks_without_pool", []eddy.Option{eddy.HandleFunc(apps, noop), eddy.WithObjectLocks()}},
		{"nil_middleware", []eddy.Option{eddy.HandleFunc(apps, noop), eddy.WithMiddleware(nil)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := eddy.Embed(nil, tt.opts...); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func TestMiddleware_wrapsInOrderWithAttempt(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	apps := eddy.NewKind[int64]("app")
	var mu sync.Mutex
	var trace []string
	note := func(s string) {
		mu.Lock()
		trace = append(trace, s)
		mu.Unlock()
	}
	mw := func(name string) eddy.Middleware {
		return func(next eddy.Handler) eddy.Handler {
			return func(ctx context.Context, ref eddy.Ref) (eddy.Result, error) {
				note(name + ">" + ref.Kind() + ":" + ref.ID())
				res, err := next(ctx, ref)
				note("<" + name)
				return res, err
			}
		}
	}
	done := make(chan int, 1)
	var leading atomic.Bool
	ctl, err := eddy.New(ctx, pool,
		eddy.HandleFunc(apps, func(ctx context.Context, _ int64) (eddy.Result, error) {
			note("reconcile")
			done <- eddy.Attempt(ctx)
			return eddy.Done, nil
		}),
		eddy.WithMiddleware(mw("outer"), mw("inner")),
		eddy.OnLeadershipChange(func(v bool) { leading.Store(v) }),
		eddy.WithElector(eddy.Solo()),
	)
	if err != nil {
		t.Fatal(err)
	}
	run(t, ctl)
	if err := ctl.Enqueue(ctx, apps.Of(5)); err != nil {
		t.Fatal(err)
	}
	select {
	case attempt := <-done:
		if attempt != 1 {
			t.Fatalf("Attempt(ctx) = %d, want 1", attempt)
		}
	case <-time.After(waitFor):
		t.Fatal("timed out")
	}
	waitUntil(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(trace) == 5 })
	want := "outer>app:5 inner>app:5 reconcile <inner <outer"
	mu.Lock()
	got := strings.Join(trace, " ")
	mu.Unlock()
	if got != want {
		t.Fatalf("trace = %q, want %q", got, want)
	}
	if !leading.Load() {
		t.Fatal("OnLeadershipChange(true) not called")
	}
}

func TestColumnStatus_recordsFailureThenSuccess(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if _, err := pool.Exec(ctx, `CREATE TABLE apps (
		id bigint PRIMARY KEY, reconcile_error text,
		reconciled_at timestamptz, attempted_at timestamptz)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO apps (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	apps := eddy.NewKind[int64]("app")
	var fail atomic.Bool
	fail.Store(true)
	ctl, err := eddy.New(ctx, pool,
		eddy.HandleFunc(apps, func(context.Context, int64) (eddy.Result, error) {
			if fail.Load() {
				return eddy.Done, eddy.Permanent(errors.New("image not found"))
			}
			return eddy.Done, nil
		}),
		eddy.Record(apps, eddy.ColumnStatus[int64](pool, eddy.StatusColumns{
			Table: "apps", Error: "reconcile_error", ReconciledAt: "reconciled_at", AttemptedAt: "attempted_at",
		})),
		eddy.WithElector(eddy.Solo()),
	)
	if err != nil {
		t.Fatal(err)
	}
	run(t, ctl)

	type row struct {
		errMsg              *string
		reconciled, attempt *time.Time
	}
	read := func() row {
		var r row
		_ = pool.QueryRow(ctx, "SELECT reconcile_error, reconciled_at, attempted_at FROM apps WHERE id = 1").
			Scan(&r.errMsg, &r.reconciled, &r.attempt)
		return r
	}

	if err := ctl.Enqueue(ctx, apps.Of(1)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return read().errMsg != nil })
	if r := read(); !strings.Contains(*r.errMsg, "image not found") || r.reconciled != nil || r.attempt == nil {
		t.Fatalf("after failure: error=%v reconciled=%v attempted=%v", *r.errMsg, r.reconciled, r.attempt)
	}

	fail.Store(false)
	if err := ctl.Enqueue(ctx, apps.Of(1)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return read().reconciled != nil })
	if r := read(); r.errMsg != nil {
		t.Fatalf("after success: error = %q, want NULL", *r.errMsg)
	}
}

// startRiver runs a River client with the eddy queue, as an app that
// already uses River would.
func startRiver(t *testing.T, pool *pgxpool.Pool, ed *eddy.Embedded) *river.Client[pgx.Tx] {
	t.Helper()
	workers := river.NewWorkers()
	ed.AddWorkers(workers)
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:       map[string]river.QueueConfig{ed.Queue(): ed.QueueConfig()},
		Workers:      workers,
		PeriodicJobs: ed.PeriodicJobs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Stop(ctx)
	})
	return client
}

func TestEmbed_ownRiverClient_reconciles(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	orgs := eddy.NewKind[string]("org")
	rec := newRecorder[string]()
	ed, err := eddy.Embed(pool,
		eddy.HandleFunc(orgs, func(_ context.Context, id string) (eddy.Result, error) {
			rec.add(id)
			return eddy.Done, nil
		}),
		eddy.Resync(orgs, time.Hour, func(context.Context) ([]string, error) { return []string{"listed"}, nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := startRiver(t, pool, ed)
	api, err := eddy.EnqueuerFor(client)
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := pool.Begin(ctx)
	if err := api.EnqueueTx(ctx, tx, orgs.Of("acme")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	rec.wait(t, "acme")
	rec.wait(t, "listed")
}

func TestObjectLocks_twoProcesses_neverOverlap(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	apps := eddy.NewKind[int64]("app")
	rec := newRecorder[int64]()
	var inFlight, maxInFlight atomic.Int32
	reconcile := func(_ context.Context, id int64) (eddy.Result, error) {
		n := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
		inFlight.Add(-1)
		rec.add(id)
		return eddy.Done, nil
	}
	var client *river.Client[pgx.Tx]
	for range 2 {
		ed, err := eddy.Embed(pool, eddy.HandleFunc(apps, reconcile), eddy.WithObjectLocks())
		if err != nil {
			t.Fatal(err)
		}
		client = startRiver(t, pool, ed)
	}
	api, err := eddy.EnqueuerFor(client)
	if err != nil {
		t.Fatal(err)
	}
	refs := []eddy.Ref{apps.Of(9), apps.Of(9), apps.Of(9), apps.Of(9)}
	if err := api.Enqueue(ctx, refs...); err != nil {
		t.Fatal(err)
	}
	for range refs {
		rec.wait(t, 9)
	}
	if m := maxInFlight.Load(); m != 1 {
		t.Fatalf("max concurrent reconciles of one object across processes = %d, want 1", m)
	}
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

func TestRecord_retryableFailure_notFinalAndTruncated(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if _, err := pool.Exec(ctx, "CREATE TABLE teams (id bigint PRIMARY KEY, err text); INSERT INTO teams VALUES (2, NULL)"); err != nil {
		t.Fatal(err)
	}
	teams := eddy.NewKind[int64]("team")
	write := eddy.ColumnStatus[int64](pool, eddy.StatusColumns{Table: "teams", Error: "err", MaxErrorLen: 10})
	outcomes := make(chan eddy.Outcome, 10)
	ctl, err := eddy.New(ctx, pool,
		eddy.HandleFunc(teams, func(context.Context, int64) (eddy.Result, error) {
			return eddy.Done, errors.New("ééééééé quota exceeded")
		}),
		eddy.Record(teams, func(ctx context.Context, id int64, o eddy.Outcome) error {
			outcomes <- o
			return write(ctx, id, o)
		}),
		eddy.WithElector(eddy.Solo()),
		eddy.WithLogger(slog.New(slog.DiscardHandler)),
		eddy.WithReconcileTimeout(5*time.Second),
		eddy.WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	run(t, ctl)
	if err := ctl.Enqueue(ctx, teams.Of(2)); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-outcomes:
		if o.OK() || o.Final || o.Attempt != 1 {
			t.Fatalf("outcome = %+v, want failed, not final, attempt 1", o)
		}
	case <-time.After(waitFor):
		t.Fatal("timed out")
	}
	var msg string
	waitUntil(t, func() bool {
		return pool.QueryRow(ctx, "SELECT err FROM teams WHERE id = 2").Scan(&msg) == nil
	})
	if len(msg) > 10 || !utf8.ValidString(msg) {
		t.Fatalf("stored error %q: want at most 10 bytes of valid UTF-8", msg)
	}
}

func TestEnqueuer_misuse_fails(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if _, err := eddy.EnqueuerFor(nil); err == nil {
		t.Fatal("EnqueuerFor(nil): want error")
	}
	api, err := eddy.NewEnqueuer(ctx, pool, eddy.WithQueue("custom"))
	if err != nil {
		t.Fatal(err)
	}
	if err := api.Enqueue(ctx, eddy.Ref{}); err == nil {
		t.Fatal("Enqueue(zero Ref): want error")
	}
	if err := api.Enqueue(ctx); err != nil {
		t.Fatalf("Enqueue(): %v", err)
	}
}

func TestNewEnqueuer_unmigratedSchema_fails(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "TRUNCATE river_migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := eddy.NewEnqueuer(ctx, pool); err == nil {
		t.Fatal("want error on missing River tables")
	}
}
