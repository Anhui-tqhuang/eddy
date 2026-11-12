package eddy_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Anhui-tqhuang/eddy"
)

type app struct {
	ID        uuid.UUID  `db:"id,default"`
	Name      string     // column "name"
	Image     string     `db:"image"`
	CreatedAt time.Time  `db:"created_at,default"`
	DeletedAt *time.Time `db:"deleted_at"`
	Phase     string     `db:"phase,status"`
	URL       *string    `db:"url,status"`
	scratch   int
	Ignored   string `db:"-"`
}

const appsDDL = `CREATE TABLE apps (
	id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	name text NOT NULL,
	image text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	deleted_at timestamptz,
	phase text NOT NULL DEFAULT '',
	url text)`

func TestNewTable_badMapping_panics(t *testing.T) {
	tests := []struct {
		name string
		make func()
	}{
		{"not_a_struct", func() { eddy.NewTable[int, int]("t") }},
		{"no_key_field", func() { eddy.NewTable[struct{ Name string }, int64]("t") }},
		{"key_type_mismatch", func() { eddy.NewTable[struct{ ID string }, int64]("t") }},
		{"unknown_tag_option", func() {
			eddy.NewTable[struct {
				ID int64 `db:"id,pk"`
			}, int64]("t")
		}},
		{"duplicate_column", func() {
			eddy.NewTable[struct {
				ID  int64
				Id2 int64 `db:"id"`
			}, int64]("t")
		}},
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

func TestNewTable_fieldNames_mapToSnakeCase(t *testing.T) {
	type embedded struct {
		TeamID int64
	}
	type row struct {
		embedded
		ID       int64
		HTTPPort int
		V2Name   string
	}
	pool := testPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, "CREATE TABLE rows (id bigint PRIMARY KEY, team_id bigint, http_port int, v2_name text)"); err != nil {
		t.Fatal(err)
	}
	rows := eddy.NewTable[row, int64]("rows", eddy.KindName("row"))
	if rows.Name() != "rows" || rows.Kind().Name() != "row" || rows.Of(1).String() != "row/1" {
		t.Fatalf("name=%s kind=%s ref=%s", rows.Name(), rows.Kind().Name(), rows.Of(1))
	}
	db, err := eddy.Open(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	in := row{embedded: embedded{TeamID: 7}, ID: 1, HTTPPort: 8080, V2Name: "x"}
	if err := rows.Insert(ctx, db, &in); err != nil {
		t.Fatal(err)
	}
	got, err := rows.Get(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if *got != in {
		t.Fatalf("got %+v, want %+v", *got, in)
	}
}

// seen records what a table reconciler was called with.
type seen struct {
	mu   sync.Mutex
	rows []*app
	ids  []uuid.UUID
}

func (s *seen) add(id uuid.UUID, a *app) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var cp *app
	if a != nil {
		c := *a
		cp = &c
	}
	s.rows, s.ids = append(s.rows, cp), append(s.ids, id)
}

func (s *seen) n() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.rows) }

func (s *seen) at(i int) (uuid.UUID, *app) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ids[i], s.rows[i]
}

func TestTable_insertUpdateDelete_reconcileLoadedRowAndSaveStatus(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if _, err := pool.Exec(ctx, appsDDL); err != nil {
		t.Fatal(err)
	}
	apps := eddy.NewTable[app, uuid.UUID]("apps")
	var calls seen
	ctl, err := eddy.New(ctx, pool,
		eddy.Reconcile(apps, func(_ context.Context, id uuid.UUID, a *app) (eddy.Result, error) {
			calls.add(id, a)
			if a == nil {
				return eddy.Done, nil
			}
			url := "https://" + a.Name + ".example.com"
			a.Phase, a.URL = "ready:"+a.Image, &url
			a.Image = "must-not-be-saved" // not a status column
			return eddy.Done, nil
		}),
		eddy.WithElector(eddy.Solo()),
	)
	if err != nil {
		t.Fatal(err)
	}
	run(t, ctl)
	db := ctl.DB()

	// Insert: defaults come back, the reconciler gets the loaded row.
	a := app{Name: "web", Image: "web:1", Phase: "ignored-on-insert"}
	if err := apps.Insert(ctx, db, &a); err != nil {
		t.Fatal(err)
	}
	if a.ID == uuid.Nil || a.CreatedAt.IsZero() || a.Phase != "" {
		t.Fatalf("after insert: %+v, want generated id, created_at and empty phase", a)
	}
	waitUntil(t, func() bool { return calls.n() == 1 })
	if id, row := calls.at(0); id != a.ID || row == nil || row.Image != "web:1" {
		t.Fatalf("reconcile 1 got id=%v row=%+v", id, row)
	}
	var got *app
	waitUntil(t, func() bool {
		got, err = apps.Get(ctx, pool, a.ID)
		return err == nil && got.Phase == "ready:web:1"
	})
	if got.URL == nil || *got.URL != "https://web.example.com" || got.Image != "web:1" {
		t.Fatalf("after reconcile 1: %+v", got)
	}

	// Update: writes spec, leaves the controller's status alone.
	a.Image, a.Phase = "web:2", "api-must-not-write-this"
	if err := apps.Update(ctx, db, &a); err != nil {
		t.Fatal(err)
	}
	if a.Phase != "ready:web:1" {
		t.Fatalf("Update clobbered or did not refresh status: phase=%q", a.Phase)
	}
	waitUntil(t, func() bool {
		got, err = apps.Get(ctx, pool, a.ID)
		return err == nil && got.Phase == "ready:web:2"
	})

	// Select.
	list, err := apps.Select(ctx, db, "WHERE name = $1", "web")
	if err != nil || len(list) != 1 || list[0].ID != a.ID {
		t.Fatalf("Select = %+v, %v", list, err)
	}

	// Delete: the row is gone, the reconciler gets nil and the id.
	before := calls.n()
	if err := apps.Delete(ctx, db, a.ID); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return calls.n() > before })
	if id, row := calls.at(calls.n() - 1); id != a.ID || row != nil {
		t.Fatalf("reconcile after delete got id=%v row=%+v, want nil row", id, row)
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

func TestTable_softDelete_reconcilerSeesRowThenPurges(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if _, err := pool.Exec(ctx, appsDDL); err != nil {
		t.Fatal(err)
	}
	apps := eddy.NewTable[app, uuid.UUID]("apps", eddy.SoftDelete("deleted_at"))
	var calls seen
	ctl, err := eddy.New(ctx, pool,
		eddy.Reconcile(apps, func(ctx context.Context, id uuid.UUID, a *app) (eddy.Result, error) {
			calls.add(id, a)
			if a != nil && a.DeletedAt != nil {
				return eddy.Done, apps.Purge(ctx, pool, id)
			}
			return eddy.Done, nil
		}),
		eddy.WithElector(eddy.Solo()),
	)
	if err != nil {
		t.Fatal(err)
	}
	run(t, ctl)
	db := ctl.DB()

	a := app{Name: "web", Image: "web:1"}
	if err := apps.Insert(ctx, db, &a); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return calls.n() == 1 })
	if err := apps.Delete(ctx, db, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := apps.Get(ctx, db, a.ID); !errors.Is(err, eddy.ErrNotFound) {
		t.Fatalf("Get soft-deleted row: %v, want ErrNotFound", err)
	}
	waitUntil(t, func() bool { return calls.n() == 2 })
	if _, row := calls.at(1); row == nil || row.DeletedAt == nil || row.Name != "web" {
		t.Fatalf("reconcile after soft delete got %+v, want the row with deleted_at set", row)
	}
	waitUntil(t, func() bool {
		var n int
		_ = pool.QueryRow(ctx, "SELECT count(*) FROM apps").Scan(&n)
		return n == 0
	})
}

func TestTable_tx_rollbackDropsRowAndJob(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if _, err := pool.Exec(ctx, appsDDL); err != nil {
		t.Fatal(err)
	}
	apps := eddy.NewTable[app, uuid.UUID]("apps")
	db, err := eddy.Open(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	jobs := func() (n int) {
		_ = pool.QueryRow(ctx, "SELECT count(*) FROM river_job").Scan(&n)
		return n
	}

	boom := errors.New("boom")
	err = db.InTx(ctx, func(tx *eddy.Tx) error {
		if err := apps.Insert(ctx, tx, &app{Name: "a", Image: "a:1"}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) || jobs() != 0 {
		t.Fatalf("after rollback: err=%v jobs=%d, want boom and 0", err, jobs())
	}

	// Two writes and a raw statement in one transaction, via plain pgx.
	ptx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx := db.WithTx(ptx)
	a, b := app{Name: "a", Image: "a:1"}, app{Name: "b", Image: "b:1"}
	if err := errors.Join(apps.Insert(ctx, tx, &a), apps.Insert(ctx, tx, &b)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "UPDATE apps SET image = 'raw' WHERE id = $1", a.ID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Enqueue(ctx, apps.Of(a.ID)); err != nil { // raw SQL needs an explicit enqueue
		t.Fatal(err)
	}
	if jobs() != 0 {
		t.Fatal("jobs visible before commit")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n := jobs(); n != 3 {
		t.Fatalf("jobs after commit = %d, want 3", n)
	}
}

func TestResyncAll_enqueuesEveryRow(t *testing.T) {
	ctx := context.Background()
	pool := testPool(t)
	if _, err := pool.Exec(ctx, appsDDL+`; INSERT INTO apps (name, image) VALUES ('a', 'a:1'), ('b', 'b:1')`); err != nil {
		t.Fatal(err)
	}
	apps := eddy.NewTable[app, uuid.UUID]("apps")
	var calls seen
	ctl, err := eddy.New(ctx, pool,
		eddy.Reconcile(apps, func(_ context.Context, id uuid.UUID, a *app) (eddy.Result, error) {
			calls.add(id, a)
			return eddy.Done, nil
		}),
		eddy.ResyncAll(apps, time.Hour),
		eddy.WithElector(eddy.Solo()),
	)
	if err != nil {
		t.Fatal(err)
	}
	run(t, ctl)
	waitUntil(t, func() bool { return calls.n() == 2 })
}

func TestEmbed_tableReconcilerWithoutPool_fails(t *testing.T) {
	apps := eddy.NewTable[app, uuid.UUID]("apps")
	_, err := eddy.Embed(nil, eddy.Reconcile(apps, func(context.Context, uuid.UUID, *app) (eddy.Result, error) {
		return eddy.Done, nil
	}))
	if err == nil {
		t.Fatal("want error")
	}
}
