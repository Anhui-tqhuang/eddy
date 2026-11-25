package eddy_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Anhui-tqhuang/eddy"
)

func TestScheduling_badValues_panic(t *testing.T) {
	for name, f := range map[string]func(){
		"priority_zero":    func() { eddy.Priority(0) },
		"priority_five":    func() { eddy.Priority(5) },
		"max_workers_zero": func() { eddy.MaxWorkers(0) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("want panic")
				}
			}()
			f()
		})
	}
}

func TestMaxWorkers_kindGetsItsOwnCappedQueue(t *testing.T) {
	ctx := context.Background()
	type report struct {
		ID   int64
		Body string
	}
	// One pre-existing row, so the resync path is covered too.
	db := testDB(t, `CREATE TABLE reports (id bigint PRIMARY KEY, body text NOT NULL)`,
		`INSERT INTO reports VALUES (100, 'old')`)
	reports := eddy.NewTable[report]("reports", eddy.MaxWorkers(2), eddy.Priority(3))
	fast := eddy.NewKind[int64]("fast")
	bulk := eddy.NewKind[int64]("bulk", eddy.Priority(4))

	for i := range int64(6) {
		if err := reports.Insert(ctx, db, &report{ID: i, Body: "new"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Enqueue(ctx, fast.Of(1), bulk.Of(1), reports.Of(int64(0))); err != nil {
		t.Fatal(err)
	}
	for where, want := range map[string]int{
		"args->>'kind' = 'reports' AND queue = 'eddy-reports' AND priority = 3": 6, // report 0's second enqueue merged
		"args->>'kind' = 'fast' AND queue = 'eddy' AND priority = 1":            1,
		"args->>'kind' = 'bulk' AND queue = 'eddy' AND priority = 4":            1,
	} {
		if n := jobCount(t, db, where); n != want {
			t.Fatalf("jobs where %s = %d, want %d", where, n, want)
		}
	}

	var inFlight, maxInFlight, fastDuringReports atomic.Int32
	var done seenLog[int64]
	run(t, db,
		eddy.Reconcile(reports, func(_ context.Context, r *report) error {
			n := inFlight.Add(1)
			for {
				m := maxInFlight.Load()
				if n <= m || maxInFlight.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(150 * time.Millisecond)
			inFlight.Add(-1)
			done.add(r.ID)
			return nil
		}),
		eddy.Handle(fast, func(context.Context, int64) error {
			fastDuringReports.Store(inFlight.Load())
			return nil
		}),
		eddy.Handle(bulk, func(context.Context, int64) error { return nil }),
	)
	// Six inserts, one explicit enqueue, plus the resync of all seven rows.
	waitUntil(t, func() bool { return done.n() >= 8 && count(&done, 100) >= 1 })
	if m := maxInFlight.Load(); m != 2 {
		t.Fatalf("max concurrent report reconciles = %d, want 2", m)
	}
	waitUntil(t, func() bool { return jobCount(t, db, "args->>'kind' = 'fast' AND state = 'completed'") == 1 })
	if jobCount(t, db, "kind = 'eddy.reconcile' AND args->>'kind' = 'reports' AND queue <> 'eddy-reports'") != 0 {
		t.Fatal("a resynced report job left its own queue")
	}
}
