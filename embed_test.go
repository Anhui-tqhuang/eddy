package eddy_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/Anhui-tqhuang/eddy"
)

// startRiver runs a River client with the eddy queue, as an app that
// already uses River would.
func startRiver(t *testing.T, pool *pgxpool.Pool, ed *eddy.Embedded) *river.Client[pgx.Tx] {
	t.Helper()
	workers := river.NewWorkers()
	ed.AddWorkers(workers)
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:       ed.Queues(),
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

func TestEmbed_ownRiverClient_reconcilesTableWrites(t *testing.T) {
	ctx := context.Background()
	pool := testDB(t, appsDDL, `INSERT INTO apps (name, image) VALUES ('listed', 'l:1')`).Pool()
	var seen seenLog[string]
	ed, err := eddy.Embed(pool, quiet, eddy.Reconcile(apps, func(_ context.Context, a *app) error {
		seen.add(a.Name)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	client := startRiver(t, pool, ed)
	db := ed.DB(client)
	if err := apps.Insert(ctx, db, &app{Name: "web", Image: "web:1"}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return count(&seen, "web") >= 1 && count(&seen, "listed") >= 1 })

	// An API-only process attaches to its own client the same way.
	api, err := eddy.Attach(pool, client, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if err := apps.Insert(ctx, api, &app{Name: "api", Image: "api:1"}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return count(&seen, "api") >= 1 })
}

func TestEmbed_misuse_fails(t *testing.T) {
	pool := testPool(t)
	if _, err := eddy.Embed(pool); err == nil {
		t.Fatal("Embed with nothing registered: want error")
	}
	if _, err := eddy.Embed(nil); err == nil {
		t.Fatal("Embed(nil): want error")
	}
	if _, err := eddy.Attach(pool, nil); err == nil {
		t.Fatal("Attach with nil client: want error")
	}
}

func TestObjectLocks_twoProcesses_neverOverlap(t *testing.T) {
	ctx := context.Background()
	pool := testDB(t).Pool()
	kind := eddy.NewKind[int64]("app")
	var done seenLog[int64]
	var inFlight, maxInFlight atomic.Int32
	reconcile := func(_ context.Context, id int64) error {
		n := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
		inFlight.Add(-1)
		done.add(id)
		return nil
	}
	var db *eddy.DB
	for range 2 {
		ed, err := eddy.Embed(pool, quiet, eddy.Handle(kind, reconcile), eddy.WithObjectLocks())
		if err != nil {
			t.Fatal(err)
		}
		db = ed.DB(startRiver(t, pool, ed))
	}
	if err := db.Enqueue(ctx, kind.Of(9), kind.Of(9), kind.Of(9), kind.Of(9)); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return done.n() == 4 })
	if m := maxInFlight.Load(); m != 1 {
		t.Fatalf("max concurrent reconciles of one object across processes = %d, want 1", m)
	}
}
