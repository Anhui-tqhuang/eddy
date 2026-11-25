package eddy_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Anhui-tqhuang/eddy"
)

type app struct {
	ID           uuid.UUID  `db:",default"`
	Name         string     // column "name"
	Image        string     `db:"image"`
	CreatedAt    time.Time  `db:",default"`
	Phase        string     `db:",status"`
	URL          *string    `db:"url,status"`
	Error        *string    `db:"reconcile_error,error"`
	ReconciledAt *time.Time `db:",reconciled"`
	scratch      int
	Ignored      string `db:"-"`
}

const appsDDL = `CREATE TABLE apps (
	id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	name text NOT NULL,
	image text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	phase text NOT NULL DEFAULT '',
	url text,
	reconcile_error text,
	reconciled_at timestamptz)`

var apps = eddy.NewTable[app]("apps")

func TestNewTable_badMapping_panics(t *testing.T) {
	tests := []struct {
		name string
		make func()
	}{
		{"not_a_struct", func() { eddy.NewTable[int]("t") }},
		{"empty_name", func() { eddy.NewTable[struct{ ID int64 }]("") }},
		{"no_key_field", func() { eddy.NewTable[struct{ Name string }]("t") }},
		{"unknown_tag_option", func() {
			eddy.NewTable[struct {
				ID int64 `db:"id,pk"`
			}]("t")
		}},
		{"duplicate_column", func() {
			eddy.NewTable[struct {
				ID  int64
				Id2 int64 `db:"id"`
			}]("t")
		}},
		{"status_key", func() {
			eddy.NewTable[struct {
				ID int64 `db:",status"`
			}]("t")
		}},
		{"error_field_not_string", func() {
			eddy.NewTable[struct {
				ID  int64
				Err int `db:",error"`
			}]("t")
		}},
		{"reconciled_field_not_time", func() {
			eddy.NewTable[struct {
				ID int64
				At string `db:",reconciled"`
			}]("t")
		}},
		{"of_wrong_key_type", func() { apps.Of("not-a-uuid") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("want panic")
				}
			}()
			tt.make()
		})
	}
}

func TestTable_fieldNames_mapToSnakeCase(t *testing.T) {
	type embedded struct {
		TeamID int64
	}
	type row struct {
		embedded
		UID      int64
		HTTPPort int
		V2Name   string
	}
	ctx := context.Background()
	db := testDB(t, "CREATE TABLE rows (uid bigint PRIMARY KEY, team_id bigint, http_port int, v2_name text)")
	rows := eddy.NewTable[row]("rows", eddy.Key("uid"), eddy.KindName("row"))
	if rows.Name() != "rows" || rows.Of(int64(1)).String() != "row/1" {
		t.Fatalf("name=%s ref=%s", rows.Name(), rows.Of(int64(1)))
	}
	in := row{embedded: embedded{TeamID: 7}, UID: 1, HTTPPort: 8080, V2Name: "x"}
	if err := rows.Insert(ctx, db, &in); err != nil {
		t.Fatal(err)
	}
	got, err := rows.Get(ctx, db, int64(1))
	if err != nil || *got != in {
		t.Fatalf("got %+v, %v; want %+v", got, err, in)
	}
	if _, err := rows.Get(ctx, db, 1); err == nil { // int, not int64
		t.Fatal("Get with the wrong key type: want error")
	}
	if err := rows.Delete(ctx, db, "1"); err == nil {
		t.Fatal("Delete with the wrong key type: want error")
	}
	// A table with only a key still updates (and enqueues).
	type bare struct{ UID int64 }
	if err := eddy.NewTable[bare]("rows", eddy.Key("uid")).Update(ctx, db, &bare{UID: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestTable_writes_reconcileLoadedRowAndSaveStatus(t *testing.T) {
	ctx := context.Background()
	db := testDB(t, appsDDL)
	var seen seenLog[app]
	var fail atomic.Bool
	run(t, db, eddy.Reconcile(apps, func(_ context.Context, a *app) error {
		seen.add(*a)
		if fail.Load() {
			a.Phase = "failed"
			return eddy.Permanent(errors.New("image not found"))
		}
		url := "https://" + a.Name + ".example.com"
		a.Phase, a.URL = "ready:"+a.Image, &url
		a.Image = "must-not-be-saved" // not a status column
		return nil
	}))

	// Insert: defaults come back, status input is ignored.
	a := app{Name: "web", Image: "web:1", Phase: "ignored-on-insert"}
	if err := apps.Insert(ctx, db, &a); err != nil {
		t.Fatal(err)
	}
	if a.ID == uuid.Nil || a.CreatedAt.IsZero() || a.Phase != "" {
		t.Fatalf("after insert: %+v, want generated id, created_at and empty phase", a)
	}
	var got *app
	load := func(cond func(*app) bool) {
		t.Helper()
		waitUntil(t, func() bool {
			var err error
			got, err = apps.Get(ctx, db, a.ID)
			return err == nil && cond(got)
		})
	}
	load(func(g *app) bool { return g.Phase == "ready:web:1" })
	if got.URL == nil || *got.URL != "https://web.example.com" || got.Image != "web:1" || got.Error != nil || got.ReconciledAt == nil {
		t.Fatalf("after reconcile: %+v", got)
	}
	if first := seen.all()[0]; first.ID != a.ID || first.Image != "web:1" {
		t.Fatalf("reconciler saw %+v", first)
	}

	// Update: writes spec, leaves the controller's status alone.
	a.Image, a.Phase = "web:2", "api-must-not-write-this"
	if err := apps.Update(ctx, db, &a); err != nil {
		t.Fatal(err)
	}
	if a.Phase != "ready:web:1" {
		t.Fatalf("Update clobbered or did not refresh status: phase=%q", a.Phase)
	}
	load(func(g *app) bool { return g.Phase == "ready:web:2" })

	// A failing reconcile records its error and whatever status it set.
	fail.Store(true)
	if err := apps.Update(ctx, db, &a); err != nil {
		t.Fatal(err)
	}
	load(func(g *app) bool { return g.Error != nil })
	if !strings.Contains(*got.Error, "image not found") || got.Phase != "failed" {
		t.Fatalf("after failure: error=%q phase=%q", *got.Error, got.Phase)
	}
	// And the next success clears it.
	fail.Store(false)
	if err := db.Enqueue(ctx, apps.Of(a.ID)); err != nil {
		t.Fatal(err)
	}
	load(func(g *app) bool { return g.Error == nil && g.Phase == "ready:web:2" })

	list, err := apps.Select(ctx, db, "WHERE name = $1", "web")
	if err != nil || len(list) != 1 || list[0].ID != a.ID {
		t.Fatalf("Select = %+v, %v", list, err)
	}
	got.Phase = "manual"
	if err := apps.SaveStatus(ctx, db, got); err != nil {
		t.Fatal(err)
	}
	load(func(g *app) bool { return g.Phase == "manual" })
}

func TestTable_delete_teardownGetsTheDeletedRow(t *testing.T) {
	ctx := context.Background()
	db := testDB(t, appsDDL)
	var reconciled, torn seenLog[app]
	var deletedRefs atomic.Int32
	run(t, db,
		eddy.Reconcile(apps, func(_ context.Context, a *app) error {
			a.Phase = "ready"
			reconciled.add(*a)
			return nil
		}),
		eddy.Teardown(apps, func(_ context.Context, a *app) error {
			torn.add(*a)
			if torn.n() == 1 {
				return eddy.Retry(10 * time.Millisecond) // teardowns retry like reconciles
			}
			return nil
		}),
		eddy.WithMiddleware(func(next eddy.Handler) eddy.Handler {
			return func(ctx context.Context, ref eddy.Ref) error {
				if ref.Deleted() {
					deletedRefs.Add(1)
				}
				return next(ctx, ref)
			}
		}),
		eddy.WithResync(0), // keep the reconcile count deterministic
	)
	a := app{Name: "web", Image: "web:1"}
	if err := apps.Insert(ctx, db, &a); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool {
		g, err := apps.Get(ctx, db, a.ID)
		return err == nil && g.Phase == "ready"
	})
	before := reconciled.n()
	if err := apps.Delete(ctx, db, a.ID); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return torn.n() == 2 })
	last := torn.all()[1]
	if last.ID != a.ID || last.Name != "web" || last.Image != "web:1" || last.Phase != "ready" || !last.CreatedAt.Equal(a.CreatedAt) {
		t.Fatalf("teardown got %+v, want the row as deleted", last)
	}
	if reconciled.n() != before || deletedRefs.Load() != 2 {
		t.Fatalf("reconciles after delete = %d, deleted refs = %d", reconciled.n()-before, deletedRefs.Load())
	}
	for name, err := range map[string]error{
		"Get":    func() error { _, err := apps.Get(ctx, db, a.ID); return err }(),
		"Update": apps.Update(ctx, db, &a),
		"Delete": apps.Delete(ctx, db, a.ID),
	} {
		if !errors.Is(err, eddy.ErrNotFound) {
			t.Errorf("%s on a deleted row: %v, want ErrNotFound", name, err)
		}
	}
}

func TestTable_exec_enqueuesEveryTouchedRow(t *testing.T) {
	ctx := context.Background()
	db := testDB(t, appsDDL)
	var reconciled, torn seenLog[string]
	for _, name := range []string{"a", "b", "c"} {
		if err := apps.Insert(ctx, db, &app{Name: name, Image: name + ":1"}); err != nil {
			t.Fatal(err)
		}
	}
	run(t, db,
		eddy.Reconcile(apps, func(_ context.Context, a *app) error {
			reconciled.add(a.Name + "=" + a.Image)
			return nil
		}),
		eddy.Teardown(apps, func(_ context.Context, a *app) error {
			torn.add(a.Name + "=" + a.Image)
			return nil
		}),
		eddy.WithResync(0),
	)
	waitUntil(t, func() bool { return reconciled.n() == 3 })

	n, err := apps.Exec(ctx, db, "UPDATE apps SET image = name || $1 WHERE name <> $2;", ":2", "c")
	if err != nil || n != 2 {
		t.Fatalf("Exec update = %d, %v; want 2", n, err)
	}
	waitUntil(t, func() bool { return count(&reconciled, "a=a:2") == 1 && count(&reconciled, "b=b:2") == 1 })

	n, err = apps.Exec(ctx, db, "INSERT INTO apps (name, image) VALUES ('d', 'd:1'), ('e', 'e:1')")
	if err != nil || n != 2 {
		t.Fatalf("Exec insert = %d, %v; want 2", n, err)
	}
	waitUntil(t, func() bool { return count(&reconciled, "d=d:1") == 1 && count(&reconciled, "e=e:1") == 1 })

	n, err = apps.Exec(ctx, db, "DELETE FROM apps WHERE name IN ('a', 'c')")
	if err != nil || n != 2 {
		t.Fatalf("Exec delete = %d, %v; want 2", n, err)
	}
	waitUntil(t, func() bool { return count(&torn, "a=a:2") == 1 && count(&torn, "c=c:1") == 1 })

	if n, err := apps.Exec(ctx, db, "UPDATE apps SET image = 'x' WHERE false"); err != nil || n != 0 {
		t.Fatalf("Exec touching nothing = %d, %v", n, err)
	}
	if _, err := apps.Exec(ctx, db, "UPDATE apps SET image = 'x' RETURNING id"); err == nil {
		t.Fatal("Exec with RETURNING: want error")
	}
	if _, err := apps.Exec(ctx, db, "UPDATE nope SET x = 1"); err == nil {
		t.Fatal("Exec with bad SQL: want error")
	}
}

func TestTable_tx_rollbackDropsRowAndJob(t *testing.T) {
	ctx := context.Background()
	db := testDB(t, appsDDL)
	jobs := func() int { return jobCount(t, db, "true") }

	boom := errors.New("boom")
	err := db.InTx(ctx, func(tx *eddy.Tx) error {
		if err := apps.Insert(ctx, tx, &app{Name: "a", Image: "a:1"}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) || jobs() != 0 {
		t.Fatalf("after rollback: err=%v jobs=%d, want boom and 0", err, jobs())
	}

	// Several writes join a pgx transaction the caller already holds.
	ptx, err := db.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.WithTx(ptx)
	a, b := app{Name: "a", Image: "a:1"}, app{Name: "b", Image: "b:1"}
	if err := errors.Join(apps.Insert(ctx, tx, &a), apps.Insert(ctx, tx, &b)); err != nil {
		t.Fatal(err)
	}
	if n, err := apps.Exec(ctx, tx, "UPDATE apps SET image = 'x'"); err != nil || n != 2 {
		t.Fatalf("Exec in tx = %d, %v", n, err)
	}
	if jobs() != 0 {
		t.Fatal("jobs visible before commit")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	// Two rows, written twice each: one job per row (merged, see merge.go).
	if n := jobs(); n != 2 {
		t.Fatalf("jobs after commit = %d, want 2", n)
	}
	// Plain SQL through the DB enqueues nothing.
	if _, err := db.Exec(ctx, "UPDATE apps SET image = 'raw'"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if n := jobs(); n != 2 {
		t.Fatalf("jobs after raw Exec = %d, want 2", n)
	}
}

func TestTable_resync_picksUpRowsWrittenOutsideEddy(t *testing.T) {
	db := testDB(t, appsDDL, `INSERT INTO apps (name, image) VALUES ('a', 'a:1'), ('b', 'b:1')`)
	var seen seenLog[string]
	run(t, db, eddy.Reconcile(apps, func(_ context.Context, a *app) error {
		seen.add(a.Name)
		return nil
	}))
	waitUntil(t, func() bool { return count(&seen, "a") == 1 && count(&seen, "b") == 1 })
}
