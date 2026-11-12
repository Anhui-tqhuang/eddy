package eddy_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Anhui-tqhuang/eddy"
)

func TestKind_Of_formatsRef(t *testing.T) {
	tests := []struct {
		name string
		ref  eddy.Ref
		want string
	}{
		{"int_id", eddy.NewKind[int64]("team").Of(42), "team/42"},
		{"string_id", eddy.NewKind[string]("org").Of("acme"), `org/"acme"`},
		{"uuid_id", eddy.NewKind[uuid.UUID]("app").Of(uuid.MustParse("6f1c1e2a-0000-4000-8000-000000000001")), `app/"6f1c1e2a-0000-4000-8000-000000000001"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ref.String(); got != tt.want {
				t.Fatalf("String() = %s, want %s", got, tt.want)
			}
		})
	}
	if k := eddy.NewKind[int64]("team"); k.Name() != "team" || k.Of(1).Kind() != "team" || k.Of(1).ID() != "1" || k.Of(1).Deleted() {
		t.Fatal("Kind or Ref accessors wrong")
	}
}

func TestNewKind_emptyName_panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	eddy.NewKind[int64]("")
}

func TestErrors_permanentAndRetry(t *testing.T) {
	base := errors.New("bad row")
	if err := eddy.Permanent(base); !eddy.IsPermanent(err) || !errors.Is(err, base) || eddy.IsPermanent(base) {
		t.Fatalf("Permanent(%v) = %v", base, err)
	}
	if eddy.Permanent(nil) != nil {
		t.Fatal("Permanent(nil) != nil")
	}
	wrapped := errors.Join(errors.New("ctx"), eddy.Retry(3*time.Second))
	if d, ok := eddy.RetryDelay(wrapped); !ok || d != 3*time.Second {
		t.Fatalf("RetryDelay = %v, %v", d, ok)
	}
	if d, ok := eddy.RetryDelay(eddy.Retry(-time.Second)); !ok || d != 0 {
		t.Fatalf("negative Retry = %v, %v, want 0", d, ok)
	}
	if _, ok := eddy.RetryDelay(base); ok {
		t.Fatal("plain error reported as retry")
	}
}

func TestRun_invalidOptions_fail(t *testing.T) {
	db := testDB(t)
	apps := eddy.NewKind[int64]("app")
	type row struct{ ID int64 }
	table := eddy.NewTable[row]("app")
	noop := func(context.Context, int64) error { return nil }
	list := func(context.Context) ([]int64, error) { return nil, nil }
	rec := func(context.Context, int64, eddy.Outcome) error { return nil }
	tests := []struct {
		name string
		opts []eddy.Option
	}{
		{"nothing_registered", nil},
		{"duplicate_kind", []eddy.Option{eddy.Handle(apps, noop), eddy.Handle(apps, noop)}},
		{"nil_handler", []eddy.Option{eddy.Handle(apps, nil)}},
		{"resync_without_handler", []eddy.Option{eddy.Handle(eddy.NewKind[int64]("team"), noop), eddy.Resync(apps, time.Minute, list)}},
		{"record_without_handler", []eddy.Option{eddy.Handle(eddy.NewKind[int64]("team"), noop), eddy.Record(apps, rec)}},
		{"zero_resync_interval", []eddy.Option{eddy.Handle(apps, noop), eddy.Resync(apps, 0, list)}},
		{"negative_table_resync", []eddy.Option{eddy.Handle(apps, noop), eddy.WithResync(-1)}},
		{"zero_workers", []eddy.Option{eddy.Handle(apps, noop), eddy.WithWorkers(0)}},
		{"queue_on_run", []eddy.Option{eddy.Handle(apps, noop), eddy.WithQueue("other")}},
		{"nil_middleware", []eddy.Option{eddy.Handle(apps, noop), eddy.WithMiddleware(nil)}},
		{"nil_elector", []eddy.Option{eddy.Handle(apps, noop), eddy.WithElector(nil)}},
		{"table_and_kind_share_name", []eddy.Option{
			eddy.Handle(apps, noop),
			eddy.Reconcile(table, func(context.Context, *row) error { return nil }),
		}},
		{"table_reconcile_twice", []eddy.Option{
			eddy.Reconcile(table, func(context.Context, *row) error { return nil }),
			eddy.Reconcile(table, func(context.Context, *row) error { return nil }),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := db.Run(context.Background(), tt.opts...); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func TestOpen_migration(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if _, err := eddy.Open(ctx, pool, eddy.WithoutMigrate()); err == nil {
		t.Fatal("WithoutMigrate on an empty schema: want error")
	}
	if _, err := eddy.Open(ctx, nil); err == nil {
		t.Fatal("nil pool: want error")
	}
	// Several processes starting at once migrate safely.
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Go(func() { _, errs[i] = eddy.Open(ctx, pool, quiet) })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	if err := eddy.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate when current: %v", err)
	}
	if _, err := eddy.Open(ctx, pool, eddy.WithoutMigrate()); err != nil {
		t.Fatalf("WithoutMigrate once migrated: %v", err)
	}
}

func TestHandle_enqueueInTx_reconcilesOnlyWhenCommitted(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	apps := eddy.NewKind[uuid.UUID]("app")
	var got seenLog[uuid.UUID]
	run(t, db, eddy.Handle(apps, func(_ context.Context, id uuid.UUID) error {
		got.add(id)
		return nil
	}))
	committed, rolledBack, direct := uuid.New(), uuid.New(), uuid.New()

	tx, _ := db.Begin(ctx)
	if err := tx.Enqueue(ctx, apps.Of(rolledBack)); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)

	err := db.InTx(ctx, func(tx *eddy.Tx) error { return tx.Enqueue(ctx, apps.Of(committed)) })
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Enqueue(ctx, apps.Of(direct)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return count(&got, committed) == 1 && count(&got, direct) == 1 })
	if n := count(&got, rolledBack); n != 0 {
		t.Fatalf("rolled-back enqueue reconciled %d times", n)
	}
	if err := db.Enqueue(ctx, eddy.Ref{}); err == nil {
		t.Fatal("Enqueue(zero Ref): want error")
	}
}

func TestHandle_permanentError_doesNotRetry(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	teams := eddy.NewKind[int64]("team")
	var got seenLog[int64]
	run(t, db, eddy.Handle(teams, func(_ context.Context, id int64) error {
		got.add(id)
		return eddy.Permanent(errors.New("invalid row"))
	}))
	if err := db.Enqueue(ctx, teams.Of(7)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return jobCount(t, db, "kind = 'eddy.reconcile' AND state = 'cancelled'") == 1 })
	if n := got.n(); n != 1 {
		t.Fatalf("reconciled %d times, want 1", n)
	}
}

func TestHandle_retry_reconcilesAgainWithoutFailing(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	apps := eddy.NewKind[int64]("app")
	var got seenLog[int64]
	var outcomes seenLog[eddy.Outcome]
	run(t, db,
		eddy.Handle(apps, func(_ context.Context, id int64) error {
			got.add(id)
			if got.n() == 1 {
				return eddy.Retry(10 * time.Millisecond)
			}
			return nil
		}),
		eddy.Record(apps, func(_ context.Context, _ int64, o eddy.Outcome) error {
			outcomes.add(o)
			return nil
		}),
	)
	if err := db.Enqueue(ctx, apps.Of(3)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return outcomes.n() == 2 })
	first, second := outcomes.all()[0], outcomes.all()[1]
	if first.OK() || first.Err != nil || first.RetryAfter != 10*time.Millisecond || first.Final {
		t.Fatalf("first outcome = %+v, want a retry", first)
	}
	if !second.OK() || !second.Final {
		t.Fatalf("second outcome = %+v, want final success", second)
	}
	waitUntil(t, func() bool { return jobCount(t, db, "kind = 'eddy.reconcile' AND state = 'completed'") == 1 })
}

func TestResync_onLeading_enqueuesListedIDs(t *testing.T) {
	db := testDB(t)
	orgs := eddy.NewKind[string]("org")
	var got seenLog[string]
	run(t, db,
		eddy.Handle(orgs, func(_ context.Context, id string) error {
			got.add(id)
			return nil
		}),
		eddy.Resync(orgs, time.Hour, func(context.Context) ([]string, error) {
			return []string{"acme", "globex"}, nil
		}),
	)
	waitUntil(t, func() bool { return count(&got, "acme") == 1 && count(&got, "globex") == 1 })
}

func TestHandle_sameObjectEnqueuedTwice_neverOverlaps(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	apps := eddy.NewKind[int64]("app")
	var got seenLog[int64]
	var inFlight, maxInFlight atomic.Int32
	if err := db.Enqueue(ctx, apps.Of(1), apps.Of(1), apps.Of(1)); err != nil {
		t.Fatal(err)
	}
	run(t, db, eddy.Handle(apps, func(_ context.Context, id int64) error {
		maxInFlight.Store(max(maxInFlight.Load(), inFlight.Add(1)))
		time.Sleep(100 * time.Millisecond)
		inFlight.Add(-1)
		got.add(id)
		return nil
	}))
	waitUntil(t, func() bool { return got.n() == 3 })
	if m := maxInFlight.Load(); m != 1 {
		t.Fatalf("max concurrent reconciles of one object = %d, want 1", m)
	}
}

func TestAdvisoryLock_secondReplica_waitsForRelease(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	a := eddy.AdvisoryLock(pool, "lock", eddy.WithLockRetry(50*time.Millisecond))
	b := eddy.AdvisoryLock(pool, "lock", eddy.WithLockRetry(50*time.Millisecond))

	leadA, releaseA, err := a.Campaign(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan func(), 1)
	go func() {
		if _, releaseB, err := b.Campaign(ctx); err == nil {
			got <- releaseB
		}
	}()
	select {
	case <-got:
		t.Fatal("second replica led while first held the lock")
	case <-time.After(300 * time.Millisecond):
	}
	if leadA.Err() != nil {
		t.Fatal("first replica lost leadership unexpectedly")
	}
	releaseA()
	select {
	case releaseB := <-got:
		releaseB()
	case <-time.After(waitFor):
		t.Fatal("second replica never led after release")
	}
}

func TestRun_twoReplicas_onlyOneLeads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := testPool(t)
	apps := eddy.NewKind[int64]("app")
	var dbs []*eddy.DB
	var wg sync.WaitGroup
	for range 2 {
		db, err := eddy.Open(ctx, pool, quiet)
		if err != nil {
			t.Fatal(err)
		}
		dbs = append(dbs, db)
		wg.Go(func() {
			err := db.Run(ctx,
				eddy.Handle(apps, func(context.Context, int64) error { return nil }),
				eddy.WithElector(eddy.AdvisoryLock(pool, "replicas", eddy.WithLockRetry(50*time.Millisecond))),
				eddy.WithShutdownTimeout(5*time.Second), eddy.WithReconcileTimeout(5*time.Second),
			)
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		})
	}
	t.Cleanup(func() { cancel(); wg.Wait() })
	leaders := func() (n int) {
		for _, db := range dbs {
			if db.Leading() {
				n++
			}
		}
		return n
	}
	waitUntil(t, func() bool { return leaders() == 1 })
	time.Sleep(300 * time.Millisecond)
	if n := leaders(); n != 1 {
		t.Fatalf("leaders = %d, want 1", n)
	}
}

func TestMiddleware_wrapsInOrderWithAttempt(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	apps := eddy.NewKind[int64]("app")
	var trace seenLog[string]
	mw := func(name string) eddy.Middleware {
		return func(next eddy.Handler) eddy.Handler {
			return func(ctx context.Context, ref eddy.Ref) error {
				trace.add(name + ">" + ref.Kind() + ":" + ref.ID())
				err := next(ctx, ref)
				trace.add("<" + name)
				return err
			}
		}
	}
	var attempt atomic.Int32
	var leading atomic.Bool
	run(t, db,
		eddy.Handle(apps, func(ctx context.Context, _ int64) error {
			trace.add("reconcile")
			attempt.Store(int32(eddy.Attempt(ctx)))
			return nil
		}),
		eddy.WithMiddleware(mw("outer"), mw("inner")),
		eddy.OnLeadershipChange(func(v bool) { leading.Store(v) }),
	)
	if err := db.Enqueue(ctx, apps.Of(5)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return trace.n() == 5 })
	if got, want := strings.Join(trace.all(), " "), "outer>app:5 inner>app:5 reconcile <inner <outer"; got != want {
		t.Fatalf("trace = %q, want %q", got, want)
	}
	if attempt.Load() != 1 || eddy.Attempt(ctx) != 0 {
		t.Fatalf("Attempt inside = %d (want 1), outside = %d (want 0)", attempt.Load(), eddy.Attempt(ctx))
	}
	if !leading.Load() || !db.Leading() {
		t.Fatal("leadership not reported")
	}
}

func TestColumnStatus_recordsFailureThenSuccess(t *testing.T) {
	ctx := context.Background()
	db := testDB(t, `CREATE TABLE teams (id bigint PRIMARY KEY, err text, ok_at timestamptz, tried_at timestamptz)`,
		`INSERT INTO teams (id) VALUES (1)`)
	teams := eddy.NewKind[int64]("team")
	var fail atomic.Bool
	fail.Store(true)
	var outcomes seenLog[eddy.Outcome]
	write := eddy.ColumnStatus[int64](db, eddy.StatusColumns{
		Table: "teams", Error: "err", ReconciledAt: "ok_at", AttemptedAt: "tried_at", MaxErrorLen: 10,
	})
	run(t, db,
		eddy.Handle(teams, func(context.Context, int64) error {
			if fail.Load() {
				return errors.New("ééééééé quota exceeded")
			}
			return nil
		}),
		eddy.Record(teams, func(ctx context.Context, id int64, o eddy.Outcome) error {
			outcomes.add(o)
			return write(ctx, id, o)
		}),
	)
	type row struct {
		msg       *string
		ok, tried *time.Time
	}
	read := func() (r row) {
		_ = db.QueryRow(ctx, "SELECT err, ok_at, tried_at FROM teams WHERE id = 1").Scan(&r.msg, &r.ok, &r.tried)
		return r
	}

	if err := db.Enqueue(ctx, teams.Of(1)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return read().msg != nil })
	if r := read(); len(*r.msg) > 10 || !utf8.ValidString(*r.msg) || r.ok != nil || r.tried == nil {
		t.Fatalf("after failure: msg=%q ok=%v tried=%v", *r.msg, r.ok, r.tried)
	}
	if o := outcomes.all()[0]; o.OK() || o.Final || o.Attempt != 1 {
		t.Fatalf("outcome = %+v, want failed, not final, attempt 1", o)
	}

	fail.Store(false)
	if err := db.Enqueue(ctx, teams.Of(1)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return read().ok != nil })
	if r := read(); r.msg != nil {
		t.Fatalf("after success: err = %q, want NULL", *r.msg)
	}
}

func TestColumnStatus_noColumns_panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	eddy.ColumnStatus[int64](&pgxpool.Pool{}, eddy.StatusColumns{Table: "apps"})
}
