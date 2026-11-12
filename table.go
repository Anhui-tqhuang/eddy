package eddy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when the row a Table call names does not exist, or
// is soft-deleted.
var ErrNotFound = errors.New("eddy: row not found")

// Table maps Go struct T to a database table whose key has type ID. Writes
// through it enqueue the row's reconcile in the same transaction, so there is
// no separate enqueue call to remember:
//
//	type App struct {
//		ID    uuid.UUID `db:"id,default"`    // filled by the database when zero
//		Image string    `db:"image"`
//		Phase string    `db:"phase,status"`  // written only by the controller
//	}
//
//	var Apps = eddy.NewTable[App, uuid.UUID]("apps")
//
// Columns come from the `db` tag, or the snake_case field name without one;
// `db:"-"` skips a field. Insert and Update never write "status" columns, so
// the API cannot clobber what the controller recorded.
type Table[T any, ID any] struct {
	name       string
	kind       Kind[ID]
	cols       []column
	key        column
	softDelete string // column set to now() by Delete; "" deletes the row
}

// TableOption configures NewTable.
type TableOption func(*tableOptions)

type tableOptions struct {
	key, softDelete, kind string
}

// Key names the key column; the default is "id".
func Key(column string) TableOption { return func(o *tableOptions) { o.key = column } }

// SoftDelete makes Delete set column (a nullable timestamptz) to now()
// instead of removing the row, so the reconciler can still read the row
// while it tears things down. Get and Update treat such rows as not found;
// Purge removes them for good.
func SoftDelete(column string) TableOption {
	return func(o *tableOptions) { o.softDelete = column }
}

// KindName sets the kind name used in jobs; the default is the table name.
func KindName(name string) TableOption { return func(o *tableOptions) { o.kind = name } }

// NewTable returns the Table for struct T stored in the table called name
// (optionally schema-qualified). It panics when T is not a struct, has no
// key column, or its key field is not of type ID: these are programming
// errors, caught at start-up.
func NewTable[T any, ID any](name string, opts ...TableOption) *Table[T, ID] {
	o := tableOptions{key: "id", kind: name}
	for _, opt := range opts {
		opt(&o)
	}
	typ := reflect.TypeFor[T]()
	cols, err := columnsOf(typ)
	if err != nil {
		panic(fmt.Sprintf("eddy: NewTable(%q): %v", name, err))
	}
	t := &Table[T, ID]{name: name, kind: NewKind[ID](o.kind), cols: cols, softDelete: o.softDelete}
	found := false
	for _, c := range cols {
		if c.name == o.key {
			t.key, found = c, true
		}
	}
	if !found {
		panic(fmt.Sprintf("eddy: NewTable(%q): %s has no field for key column %q", name, typ, o.key))
	}
	if kt := typ.FieldByIndex(t.key.index).Type; kt != reflect.TypeFor[ID]() {
		panic(fmt.Sprintf("eddy: NewTable(%q): key field is %s, not %s", name, kt, reflect.TypeFor[ID]()))
	}
	return t
}

// Name returns the table name.
func (t *Table[T, ID]) Name() string { return t.name }

// Kind returns the table's Kind, for Record, Resync and manual enqueues.
func (t *Table[T, ID]) Kind() Kind[ID] { return t.kind }

// Of returns a reference to the row with key id.
func (t *Table[T, ID]) Of(id ID) Ref { return t.kind.Of(id) }

// writable reports whether the API side writes column c.
func (t *Table[T, ID]) writable(c column) bool {
	return !c.status && c.name != t.softDelete
}

func (t *Table[T, ID]) live() string {
	if t.softDelete == "" {
		return ""
	}
	return " AND " + quote(t.softDelete) + " IS NULL"
}

// Insert adds row and enqueues its reconcile. Fields tagged "default" are
// left to the database when zero. row is refreshed from the database, so
// generated keys and defaults are filled in on return.
func (t *Table[T, ID]) Insert(ctx context.Context, s Session, row *T) error {
	v := reflect.ValueOf(row).Elem()
	var names, params []string
	var args []any
	for _, c := range t.cols {
		f := v.FieldByIndex(c.index)
		if !t.writable(c) || (c.hasDefault && f.IsZero()) {
			continue
		}
		args = append(args, f.Interface())
		names = append(names, quote(c.name))
		params = append(params, fmt.Sprintf("$%d", len(args)))
	}
	sql := "INSERT INTO " + quote(t.name)
	if len(names) == 0 {
		sql += " DEFAULT VALUES"
	} else {
		sql += " (" + strings.Join(names, ", ") + ") VALUES (" + strings.Join(params, ", ") + ")"
	}
	sql += " RETURNING " + quoteAll(t.cols)
	return t.write(ctx, s, "insert into", sql, args, v)
}

// Update writes row's non-status columns to the row with the same key and
// enqueues its reconcile. row is refreshed from the database. It returns
// ErrNotFound when no live row has that key.
func (t *Table[T, ID]) Update(ctx context.Context, s Session, row *T) error {
	v := reflect.ValueOf(row).Elem()
	args := []any{v.FieldByIndex(t.key.index).Interface()}
	var set []string
	for _, c := range t.cols {
		if !t.writable(c) || c.name == t.key.name {
			continue
		}
		args = append(args, v.FieldByIndex(c.index).Interface())
		set = append(set, fmt.Sprintf("%s = $%d", quote(c.name), len(args)))
	}
	if len(set) == 0 {
		// Nothing to write, but the caller still asked for a reconcile.
		set = []string{quote(t.key.name) + " = " + quote(t.key.name)}
	}
	sql := "UPDATE " + quote(t.name) + " SET " + strings.Join(set, ", ") +
		" WHERE " + quote(t.key.name) + " = $1" + t.live() + " RETURNING " + quoteAll(t.cols)
	return t.write(ctx, s, "update", sql, args, v)
}

// write runs a row-returning statement, refreshes dst from the result and
// enqueues the row's reconcile, all in one transaction.
func (t *Table[T, ID]) write(ctx context.Context, s Session, verb, sql string, args []any, dst reflect.Value) error {
	return s.within(ctx, func(tx pgx.Tx, enq *Enqueuer) error {
		rows, err := tx.Query(ctx, sql, args...)
		if err != nil {
			return fmt.Errorf("eddy: %s %s: %w", verb, t.name, err)
		}
		if err := t.scanOne(rows, dst); err != nil {
			if errors.Is(err, ErrNotFound) {
				return err
			}
			return fmt.Errorf("eddy: %s %s: %w", verb, t.name, err)
		}
		id, _ := dst.FieldByIndex(t.key.index).Interface().(ID)
		return enq.EnqueueTx(ctx, tx, t.kind.Of(id))
	})
}

// Delete removes the row with key id, or marks it deleted when the table
// uses SoftDelete, and enqueues its reconcile so the controller tears down
// what the row stood for. It returns ErrNotFound when no live row has that
// key.
func (t *Table[T, ID]) Delete(ctx context.Context, s Session, id ID) error {
	sql := "DELETE FROM " + quote(t.name) + " WHERE " + quote(t.key.name) + " = $1"
	if t.softDelete != "" {
		sql = "UPDATE " + quote(t.name) + " SET " + quote(t.softDelete) + " = now() WHERE " +
			quote(t.key.name) + " = $1" + t.live()
	}
	return s.within(ctx, func(tx pgx.Tx, enq *Enqueuer) error {
		tag, err := tx.Exec(ctx, sql, id)
		if err != nil {
			return fmt.Errorf("eddy: delete from %s: %w", t.name, err)
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return enq.EnqueueTx(ctx, tx, t.kind.Of(id))
	})
}

// Get returns the live row with key id, or ErrNotFound.
func (t *Table[T, ID]) Get(ctx context.Context, q Querier, id ID) (*T, error) {
	return t.get(ctx, q, id, t.live())
}

// Select returns the rows matching clause, which follows the FROM (for
// example "WHERE team_id = $1 ORDER BY name"). Soft-deleted rows are
// included unless clause filters them out.
func (t *Table[T, ID]) Select(ctx context.Context, q Querier, clause string, args ...any) ([]T, error) {
	rows, err := q.Query(ctx, "SELECT "+quoteAll(t.cols)+" FROM "+quote(t.name)+" "+clause, args...)
	if err != nil {
		return nil, fmt.Errorf("eddy: select from %s: %w", t.name, err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (T, error) {
		var row T
		err := r.Scan(t.targets(reflect.ValueOf(&row).Elem())...)
		return row, err
	})
	if err != nil {
		return nil, fmt.Errorf("eddy: select from %s: %w", t.name, err)
	}
	return out, nil
}

// SaveStatus writes row's "status" columns, and nothing else, to the row
// with the same key. It enqueues nothing: it is how a reconciler records
// progress. Reconcile already calls it when the reconciler returns, so call
// it yourself only to save progress midway.
func (t *Table[T, ID]) SaveStatus(ctx context.Context, q Querier, row *T) error {
	v := reflect.ValueOf(row).Elem()
	args := []any{v.FieldByIndex(t.key.index).Interface()}
	var set []string
	for _, c := range t.cols {
		if !c.status {
			continue
		}
		args = append(args, v.FieldByIndex(c.index).Interface())
		set = append(set, fmt.Sprintf("%s = $%d", quote(c.name), len(args)))
	}
	if len(set) == 0 {
		return nil
	}
	sql := "UPDATE " + quote(t.name) + " SET " + strings.Join(set, ", ") + " WHERE " + quote(t.key.name) + " = $1"
	if _, err := q.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("eddy: save status of %s: %w", t.name, err)
	}
	return nil
}

// Purge removes the row with key id for good and enqueues nothing. A
// reconciler calls it once a soft-deleted row is fully torn down. Purging a
// row that is already gone is not an error.
func (t *Table[T, ID]) Purge(ctx context.Context, q Querier, id ID) error {
	if _, err := q.Exec(ctx, "DELETE FROM "+quote(t.name)+" WHERE "+quote(t.key.name)+" = $1", id); err != nil {
		return fmt.Errorf("eddy: purge from %s: %w", t.name, err)
	}
	return nil
}

func (t *Table[T, ID]) get(ctx context.Context, q Querier, id ID, filter string) (*T, error) {
	sql := "SELECT " + quoteAll(t.cols) + " FROM " + quote(t.name) + " WHERE " + quote(t.key.name) + " = $1" + filter
	rows, err := q.Query(ctx, sql, id)
	if err != nil {
		return nil, fmt.Errorf("eddy: get from %s: %w", t.name, err)
	}
	var row T
	if err := t.scanOne(rows, reflect.ValueOf(&row).Elem()); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("eddy: get from %s: %w", t.name, err)
	}
	return &row, nil
}

// scanOne reads exactly one row into dst, or returns ErrNotFound.
func (t *Table[T, ID]) scanOne(rows pgx.Rows, dst reflect.Value) error {
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return ErrNotFound
	}
	if err := rows.Scan(t.targets(dst)...); err != nil {
		return err
	}
	rows.Close()
	return rows.Err()
}

func (t *Table[T, ID]) targets(v reflect.Value) []any {
	ptrs := make([]any, len(t.cols))
	for i, c := range t.cols {
		ptrs[i] = v.FieldByIndex(c.index).Addr().Interface()
	}
	return ptrs
}

// statusSnapshot encodes row's status fields, to tell later whether the
// reconciler changed them. ok is false when they cannot be compared, in
// which case the caller saves unconditionally.
func (t *Table[T, ID]) statusSnapshot(row *T) (snap string, ok bool) {
	v := reflect.ValueOf(row).Elem()
	var vals []any
	for _, c := range t.cols {
		if c.status {
			vals = append(vals, v.FieldByIndex(c.index).Interface())
		}
	}
	b, err := json.Marshal(vals)
	return string(b), err == nil
}

func (t *Table[T, ID]) hasStatus() bool {
	for _, c := range t.cols {
		if c.status {
			return true
		}
	}
	return false
}

// Reconcile registers f as the reconciler for t's rows. eddy loads the row
// before each call: row is nil when it no longer exists (tear down whatever
// id stood for), and soft-deleted rows are passed as they are. Whatever f
// assigns to row's "status" fields is saved when it returns, on success and
// on failure alike.
func Reconcile[T any, ID any](t *Table[T, ID], f func(ctx context.Context, id ID, row *T) (Result, error)) Option {
	return func(c *config) error {
		if t == nil || f == nil {
			return errors.New("eddy: Reconcile: nil table or func")
		}
		name := t.kind.name
		if _, dup := c.handlers[name]; dup {
			return fmt.Errorf("eddy: Reconcile(%s): kind registered twice", name)
		}
		c.needsPool = true
		c.handlers[name] = func(ctx context.Context, pool *pgxpool.Pool, raw json.RawMessage) (Result, error) {
			var id ID
			if err := json.Unmarshal(raw, &id); err != nil {
				return Done, Permanent(fmt.Errorf("decode %s id %s: %w", name, raw, err))
			}
			row, err := t.get(ctx, pool, id, "")
			if errors.Is(err, ErrNotFound) {
				return f(ctx, id, nil)
			}
			if err != nil {
				return Done, err
			}
			if !t.hasStatus() {
				return f(ctx, id, row)
			}
			before, known := t.statusSnapshot(row)
			res, rerr := f(ctx, id, row)
			if after, ok := t.statusSnapshot(row); !known || !ok || after != before {
				// Save even when the reconcile timed out or was cancelled.
				sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				if serr := t.SaveStatus(sctx, pool, row); serr != nil {
					return res, errors.Join(rerr, serr)
				}
			}
			return res, rerr
		}
		return nil
	}
}

// ResyncAll enqueues every row of t, soft-deleted ones included, when the
// controller becomes leader and then every interval. It repairs drift and
// catches rows changed outside the Table (raw SQL, other tools).
func ResyncAll[T any, ID any](t *Table[T, ID], every time.Duration) Option {
	return func(c *config) error {
		if t == nil {
			return errors.New("eddy: ResyncAll: nil table")
		}
		if every <= 0 {
			return fmt.Errorf("eddy: ResyncAll(%s): interval must be positive", t.kind.name)
		}
		c.needsPool = true
		c.resyncs[t.kind.name] = resync{every: every, list: func(ctx context.Context, pool *pgxpool.Pool) ([]json.RawMessage, error) {
			rows, err := pool.Query(ctx, "SELECT "+quote(t.key.name)+" FROM "+quote(t.name))
			if err != nil {
				return nil, err
			}
			ids, err := pgx.CollectRows(rows, pgx.RowTo[ID])
			if err != nil {
				return nil, err
			}
			raws := make([]json.RawMessage, len(ids))
			for i, id := range ids {
				raws[i] = t.kind.Of(id).id
			}
			return raws, nil
		}}
		return nil
	}
}
