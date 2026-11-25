package eddy_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Anhui-tqhuang/eddy"
)

// waiting counts the reconcile jobs of one object that wait to run: not running, not done.
func waiting(t *testing.T, db *eddy.DB, kind string, id any) int {
	t.Helper()
	return jobCount(t, db, fmt.Sprintf(
		`kind = 'eddy.reconcile' AND state IN ('available', 'scheduled', 'retryable', 'pending')
		 AND args @> '{"kind": %q, "id": %v}' AND NOT args ? 'gone'`, kind, id))
}

// A resync must not add a job for an object that already has one waiting: an object that
// keeps asking for a Retry would otherwise gain one more loop at every resync.
func TestResync_objectWithAWaitingJob_getsNoSecond(t *testing.T) {
	db := testDB(t)
	apps := eddy.NewKind[int64]("app")
	var calls atomic.Int32
	var lists atomic.Int32
	run(t, db,
		eddy.Handle(apps, func(context.Context, int64) error {
			calls.Add(1)
			return eddy.Retry(time.Hour) // never converges
		}),
		eddy.Resync(apps, 200*time.Millisecond, func(context.Context) ([]int64, error) {
			lists.Add(1)
			return []int64{3}, nil
		}),
	)
	waitUntil(t, func() bool { return calls.Load() >= 1 && lists.Load() >= 4 })
	// The measure of a pile-up: how many jobs wait for the object, and how many retry loops
	// (snoozed jobs) it has. Without merging, each resync adds one of both. (A resync that
	// lands while a reconcile still counts as running may add a job; that job is merged at
	// the next Retry, so the loops stay at one.)
	// Two jobs in flight at once can each miss the other for a moment (each still counts as
	// running), but the one that wakes first finds the other waiting and finishes: the loops
	// come back to one within the busy snooze (a second), whatever the number of resyncs.
	waitUntil(t, func() bool {
		return waiting(t, db, "app", 3) == 1 &&
			jobCount(t, db, "kind = 'eddy.reconcile' AND state IN ('running', 'scheduled')") == 1
	})
}

// A write while a job waits for later (a Retry, or backoff after an error) brings that job
// forward instead of adding a second one: the change is seen now, by one job.
func TestEnqueue_whileAJobWaitsForLater_bringsItForward(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	apps := eddy.NewKind[int64]("app")
	if err := db.Enqueue(ctx, apps.Of(3)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE river_job SET state = 'scheduled', scheduled_at = now() + interval '1 hour'
		WHERE kind = 'eddy.reconcile'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Enqueue(ctx, apps.Of(3)); err != nil {
		t.Fatal(err)
	}
	if n := waiting(t, db, "app", 3); n != 1 {
		t.Fatalf("waiting jobs = %d, want 1", n)
	}
	if n := jobCount(t, db, "kind = 'eddy.reconcile' AND state = 'available' AND scheduled_at <= now()"); n != 1 {
		t.Errorf("the waiting job was not brought forward")
	}
	// Another object is not merged with it.
	if err := db.Enqueue(ctx, apps.Of(4)); err != nil {
		t.Fatal(err)
	}
	if n := waiting(t, db, "app", 4); n != 1 {
		t.Errorf("object 4: waiting jobs = %d, want 1", n)
	}
}

// A write while the object's reconcile runs must add a job: the running one may have read
// the row before the write.
func TestEnqueue_whileRunning_addsAJob(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	apps := eddy.NewKind[int64]("app")
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	run(t, db, eddy.WithResync(0), eddy.Handle(apps, func(context.Context, int64) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}))
	if err := db.Enqueue(ctx, apps.Of(3)); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := db.Enqueue(ctx, apps.Of(3)); err != nil {
		t.Fatal(err)
	}
	if n := waiting(t, db, "app", 3); n != 1 {
		t.Errorf("waiting jobs while running = %d, want 1", n)
	}
	close(release)
	waitUntil(t, func() bool { return calls.Load() == 2 })
}

// A reconcile that asks for a Retry while another job for the same object already waits
// finishes instead: the waiting job carries on, and no second loop forms.
func TestRetry_withAnotherJobWaiting_doesNotSnooze(t *testing.T) {
	ctx := context.Background()
	db := testDB(t)
	apps := eddy.NewKind[int64]("app")
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	run(t, db, eddy.WithResync(0), eddy.Handle(apps, func(context.Context, int64) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return eddy.Retry(time.Hour)
		}
		return nil
	}))
	if err := db.Enqueue(ctx, apps.Of(3)); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := db.Enqueue(ctx, apps.Of(3)); err != nil { // a write during the run
		t.Fatal(err)
	}
	close(release)
	waitUntil(t, func() bool { return calls.Load() == 2 })
	waitUntil(t, func() bool { return waiting(t, db, "app", 3) == 0 })
}

// A deleted row's job carries its last state: it is never merged into another job.
func TestDelete_isNeverMerged(t *testing.T) {
	ctx := context.Background()
	db := testDB(t, appsDDL)
	a := app{Name: "web", Image: "web:1"}
	if err := apps.Insert(ctx, db, &a); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE river_job SET state = 'scheduled', scheduled_at = now() + interval '1 hour'
		WHERE kind = 'eddy.reconcile'`); err != nil {
		t.Fatal(err)
	}
	if err := apps.Delete(ctx, db, a.ID); err != nil {
		t.Fatal(err)
	}
	if n := jobCount(t, db, "kind = 'eddy.reconcile' AND args ? 'gone'"); n != 1 {
		t.Errorf("deletion jobs = %d, want 1", n)
	}
}
