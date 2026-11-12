package eddy_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Anhui-tqhuang/eddy"
)

const waitFor = 10 * time.Second

// testPool returns a pool on a fresh schema with River's tables migrated. It
// skips the test unless EDDY_TEST_DATABASE_URL is set.
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
	if err := eddy.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

// recorder collects reconciled ids.
type recorder[ID comparable] struct {
	mu     sync.Mutex
	ids    []ID
	waited map[ID]int
}

func newRecorder[ID comparable]() *recorder[ID] { return &recorder[ID]{waited: map[ID]int{}} }

func (r *recorder[ID]) add(id ID) {
	r.mu.Lock()
	r.ids = append(r.ids, id)
	r.mu.Unlock()
}

// wait blocks until id has been reconciled once more than in earlier waits
// for it.
func (r *recorder[ID]) wait(t *testing.T, want ID) {
	t.Helper()
	r.mu.Lock()
	r.waited[want]++
	need := r.waited[want]
	r.mu.Unlock()
	deadline := time.Now().Add(waitFor)
	for r.count(want) < need {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for reconcile #%d of %v", need, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *recorder[ID]) count(id ID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, x := range r.ids {
		if x == id {
			n++
		}
	}
	return n
}

// run starts c and stops it when the test ends.
func run(t *testing.T, c *eddy.Controller) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	})
}

func TestEnqueueTx_committed_reconcilesTypedID(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	apps := eddy.NewKind[uuid.UUID]("app")
	rec := newRecorder[uuid.UUID]()
	ctl, err := eddy.New(ctx, pool,
		eddy.HandleFunc(apps, func(_ context.Context, id uuid.UUID) (eddy.Result, error) {
			rec.add(id)
			return eddy.Done, nil
		}),
		eddy.WithElector(eddy.Solo()),
	)
	if err != nil {
		t.Fatal(err)
	}
	run(t, ctl)

	api, err := eddy.NewEnqueuer(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	committed, rolledBack := uuid.New(), uuid.New()

	tx, _ := pool.Begin(ctx)
	if err := api.EnqueueTx(ctx, tx, apps.Of(rolledBack)); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)

	tx, _ = pool.Begin(ctx)
	if err := api.EnqueueTx(ctx, tx, apps.Of(committed)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	rec.wait(t, committed)
	if n := rec.count(rolledBack); n != 0 {
		t.Fatalf("rolled-back enqueue reconciled %d times", n)
	}
}

func TestReconcile_permanentError_doesNotRetry(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	teams := eddy.NewKind[int64]("team")
	rec := newRecorder[int64]()
	ctl, err := eddy.New(ctx, pool,
		eddy.HandleFunc(teams, func(_ context.Context, id int64) (eddy.Result, error) {
			rec.add(id)
			return eddy.Done, eddy.Permanent(errors.New("invalid row"))
		}),
		eddy.WithElector(eddy.Solo()),
	)
	if err != nil {
		t.Fatal(err)
	}
	run(t, ctl)
	if err := ctl.Enqueue(ctx, teams.Of(7)); err != nil {
		t.Fatal(err)
	}
	rec.wait(t, 7)

	var state string
	deadline := time.Now().Add(waitFor)
	for state != "cancelled" && time.Now().Before(deadline) {
		_ = pool.QueryRow(ctx, "SELECT state FROM river_job WHERE kind = 'eddy.reconcile'").Scan(&state)
		time.Sleep(50 * time.Millisecond)
	}
	if state != "cancelled" {
		t.Fatalf("job state = %q, want cancelled", state)
	}
	if n := rec.count(7); n != 1 {
		t.Fatalf("reconciled %d times, want 1", n)
	}
}

func TestResync_onLeading_enqueuesListedIDs(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	orgs := eddy.NewKind[string]("org")
	rec := newRecorder[string]()
	ctl, err := eddy.New(ctx, pool,
		eddy.HandleFunc(orgs, func(_ context.Context, id string) (eddy.Result, error) {
			rec.add(id)
			return eddy.Done, nil
		}),
		eddy.Resync(orgs, time.Hour, func(context.Context) ([]string, error) {
			return []string{"acme", "globex"}, nil
		}),
		eddy.WithElector(eddy.Solo()),
	)
	if err != nil {
		t.Fatal(err)
	}
	run(t, ctl)
	rec.wait(t, "acme")
	rec.wait(t, "globex")
}

func TestReconcile_sameObjectEnqueuedTwice_neverOverlaps(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	apps := eddy.NewKind[int64]("app")
	rec := newRecorder[int64]()
	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	ctl, err := eddy.New(ctx, pool,
		eddy.HandleFunc(apps, func(_ context.Context, id int64) (eddy.Result, error) {
			mu.Lock()
			inFlight++
			maxInFlight = max(maxInFlight, inFlight)
			mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			mu.Lock()
			inFlight--
			mu.Unlock()
			rec.add(id)
			return eddy.Done, nil
		}),
		eddy.WithElector(eddy.Solo()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := ctl.Enqueue(ctx, apps.Of(1), apps.Of(1), apps.Of(1)); err != nil {
		t.Fatal(err)
	}
	run(t, ctl)
	for range 3 {
		rec.wait(t, 1)
	}
	if maxInFlight != 1 {
		t.Fatalf("max concurrent reconciles of one object = %d, want 1", maxInFlight)
	}
}

func TestAdvisoryLock_secondReplica_waitsForRelease(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	name := fmt.Sprintf("test-%s", uuid.NewString())
	a := eddy.AdvisoryLock(pool, name, eddy.WithLockRetry(50*time.Millisecond))
	b := eddy.AdvisoryLock(pool, name, eddy.WithLockRetry(50*time.Millisecond))

	leadA, releaseA, err := a.Campaign(ctx)
	if err != nil {
		t.Fatal(err)
	}

	got := make(chan func(), 1)
	go func() {
		_, releaseB, err := b.Campaign(ctx)
		if err == nil {
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

func TestController_twoReplicas_onlyOneLeads(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	apps := eddy.NewKind[int64]("app")
	noop := func(context.Context, int64) (eddy.Result, error) { return eddy.Done, nil }
	var ctls []*eddy.Controller
	for range 2 {
		c, err := eddy.New(ctx, pool, eddy.HandleFunc(apps, noop),
			eddy.WithElector(eddy.AdvisoryLock(pool, "replicas", eddy.WithLockRetry(50*time.Millisecond))))
		if err != nil {
			t.Fatal(err)
		}
		run(t, c)
		ctls = append(ctls, c)
	}
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) && !slices.ContainsFunc(ctls, (*eddy.Controller).Leading) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	leaders := 0
	for _, c := range ctls {
		if c.Leading() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("leaders = %d, want 1", leaders)
	}
}

func TestReconcile_requeueAfter_reconcilesAgain(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	apps := eddy.NewKind[int64]("app")
	rec := newRecorder[int64]()
	ctl, err := eddy.New(ctx, pool,
		eddy.HandleFunc(apps, func(_ context.Context, id int64) (eddy.Result, error) {
			rec.add(id)
			if rec.count(id) == 1 {
				return eddy.RequeueAfter(10 * time.Millisecond), nil
			}
			return eddy.Done, nil
		}),
		eddy.WithElector(eddy.Solo()),
	)
	if err != nil {
		t.Fatal(err)
	}
	run(t, ctl)
	if err := ctl.Enqueue(ctx, apps.Of(3)); err != nil {
		t.Fatal(err)
	}
	rec.wait(t, 3)
	rec.wait(t, 3)
}
