package eddy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// maxErrorLen bounds the message stored in an "error" field, in bytes.
const maxErrorLen = 1024

// Table maps Go struct T to a database table. Writes through it enqueue the
// row's reconcile in the same transaction, so there is no enqueue call to
// remember:
//
//	type App struct {
//		ID    uuid.UUID `db:",default"` // filled by the database when zero
//		Image string
//		Phase string    `db:",status"`  // written only by the controller
//	}
//
//	var Apps = eddy.NewTable[App]("apps")
//
// A column is named by the `db` tag, or by the snake_case field name without
// one; `db:"-"` skips a field. Options follow the name:
//
//   - default: left out of Insert when zero, so the database default applies
//   - status: never written by Insert or Update; saved after each reconcile
//   - error: a status string that eddy sets to the last reconcile error
//   - reconciled: a status time that eddy sets on each successful reconcile
//
// Fields must round-trip through encoding/json, which is how a deleted row
// reaches Teardown.
type Table[T any] struct {
	name    string
	kind    string
	cols    []column
	key     column
	keyType reflect.Type
}

// TableOption configures NewTable.
type TableOption func(*tableOptions)

type tableOptions struct {
	key, kind string
}

// Key names the key column; the default is "id".
func Key(column string) TableOption { return func(o *tableOptions) { o.key = column } }

// KindName sets the kind name used in jobs; the default is the table name.
func KindName(name string) TableOption { return func(o *tableOptions) { o.kind = name } }

// NewTable returns the Table for struct T stored in the table called name
// (optionally schema-qualified). It panics when T cannot be mapped: these
// are programming errors, caught at start-up.
func NewTable[T any](name string, opts ...TableOption) *Table[T] {
	o := tableOptions{key: "id", kind: name}
	for _, opt := range opts {
		opt(&o)
	}
	fail := func(format string, args ...any) {
		panic(fmt.Sprintf("eddy: NewTable(%q): ", name) + fmt.Sprintf(format, args...))
	}
	if name == "" || o.kind == "" {
		fail("empty table or kind name")
	}
	typ := reflect.TypeFor[T]()
	cols, err := columnsOf(typ)
	if err != nil {
		fail("%v", err)
	}
	t := &Table[T]{name: name, kind: o.kind, cols: cols}
	for _, c := range cols {
		ft := typ.FieldByIndex(c.index).Type
		if c.name == o.key {
			t.key, t.keyType = c, ft
		}
		if c.errorMsg && !isOneOf(ft, reflect.TypeFor[string]()) {
			fail("error field for %q must be string or *string, not %s", c.name, ft)
		}
		if c.reconciled && !isOneOf(ft, reflect.TypeFor[time.Time]()) {
			fail("reconciled field for %q must be time.Time or *time.Time, not %s", c.name, ft)
		}
	}
	if t.keyType == nil {
		fail("%s has no field for key column %q", typ, o.key)
	}
	if t.key.status {
		fail("key column %q cannot be a status column", o.key)
	}
	return t
}

// isOneOf reports whether t is base or a pointer to it.
func isOneOf(t, base reflect.Type) bool {
	return t == base || (t.Kind() == reflect.Pointer && t.Elem() == base)
}

// Name returns the table name.
func (t *Table[T]) Name() string { return t.name }

// Of returns a reference to the row with key id, for DB.Enqueue. It panics
// when id is not of the key's type.
func (t *Table[T]) Of(id any) Ref {
	raw, err := t.encodeKey(id)
	if err != nil {
		panic(err.Error())
	}
	return Ref{kind: t.kind, id: raw}
}

func (t *Table[T]) encodeKey(id any) (json.RawMessage, error) {
	if reflect.TypeOf(id) != t.keyType {
		return nil, fmt.Errorf("eddy: %s: key is %s, got %T", t.name, t.keyType, id)
	}
	raw, err := json.Marshal(id)
	if err != nil {
		return nil, fmt.Errorf("eddy: %s: encode key: %w", t.name, err)
	}
	return raw, nil
}

func (t *Table[T]) returning() string {
	names := make([]string, len(t.cols))
	for i, c := range t.cols {
		names[i] = quote(t.name) + "." + quote(c.name)
	}
	return " RETURNING " + strings.Join(names, ", ")
}

// Insert adds row and enqueues its reconcile. Fields tagged "default" are
// left to the database when zero. row is refreshed from the database, so
// generated keys and defaults are filled in on return.
func (t *Table[T]) Insert(ctx context.Context, s Session, row *T) error {
	v := reflect.ValueOf(row).Elem()
	var names, params []string
	var args []any
	for _, c := range t.cols {
		f := v.FieldByIndex(c.index)
		if c.status || (c.hasDefault && f.IsZero()) {
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
	return t.write(ctx, s, "insert into", sql+t.returning(), args, v, false)
}

// Update writes row's non-status columns to the row with the same key and
// enqueues its reconcile. row is refreshed from the database. It returns
// ErrNotFound when no row has that key.
func (t *Table[T]) Update(ctx context.Context, s Session, row *T) error {
	v := reflect.ValueOf(row).Elem()
	args := []any{v.FieldByIndex(t.key.index).Interface()}
	var set []string
	for _, c := range t.cols {
		if c.status || c.name == t.key.name {
			continue
		}
		args = append(args, v.FieldByIndex(c.index).Interface())
		set = append(set, fmt.Sprintf("%s = $%d", quote(c.name), len(args)))
	}
	if len(set) == 0 {
		// Nothing to write, but the caller still asked for a reconcile.
		set = []string{quote(t.key.name) + " = " + quote(t.key.name)}
	}
	sql := "UPDATE " + quote(t.name) + " SET " + strings.Join(set, ", ") + " WHERE " + quote(t.key.name) + " = $1"
	return t.write(ctx, s, "update", sql+t.returning(), args, v, false)
}

// Delete removes the row with key id and enqueues its teardown: the row's
// last state travels with the job, and the function registered with Teardown
// receives it. It returns ErrNotFound when no row has that key.
func (t *Table[T]) Delete(ctx context.Context, s Session, id any) error {
	if _, err := t.encodeKey(id); err != nil {
		return err
	}
	var row T
	sql := "DELETE FROM " + quote(t.name) + " WHERE " + quote(t.key.name) + " = $1"
	return t.write(ctx, s, "delete from", sql+t.returning(), []any{id}, reflect.ValueOf(&row).Elem(), true)
}

// write runs a statement that returns one row, reads that row into dst and
// enqueues its reconcile (or teardown, when gone), all in one transaction.
func (t *Table[T]) write(ctx context.Context, s Session, verb, sql string, args []any, dst reflect.Value, gone bool) error {
	return s.within(ctx, func(tx pgx.Tx, db *DB) error {
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
		job, err := t.job(dst, gone)
		if err != nil {
			return err
		}
		return db.insert(ctx, tx, []reconcileJob{job})
	})
}

var returningRE = regexp.MustCompile(`(?i)\breturning\b`)

// Exec runs a free-form INSERT, UPDATE or DELETE on this table and enqueues
// every row it touched, in the same transaction. Use it for bulk changes and
// anything Insert, Update and Delete cannot express:
//
//	Apps.Exec(ctx, db, "UPDATE apps SET image = $1 WHERE team_id = $2", image, team)
//
// It returns the number of rows touched. eddy learns which rows those were
// by adding a RETURNING clause, so sql must not have one, must be a single
// statement, and must not alias the table.
func (t *Table[T]) Exec(ctx context.Context, s Session, sql string, args ...any) (int64, error) {
	if returningRE.MatchString(sql) {
		return 0, fmt.Errorf("eddy: %s.Exec: sql must not contain RETURNING; eddy adds its own", t.name)
	}
	sql = strings.TrimRight(strings.TrimSpace(sql), ";") + t.returning()
	var n int64
	err := s.within(ctx, func(tx pgx.Tx, db *DB) error {
		rows, err := tx.Query(ctx, sql, args...)
		if err != nil {
			return fmt.Errorf("eddy: exec on %s: %w", t.name, err)
		}
		touched, err := pgx.CollectRows(rows, t.scanRow)
		if err != nil {
			return fmt.Errorf("eddy: exec on %s: %w", t.name, err)
		}
		gone := rows.CommandTag().Delete()
		jobs := make([]reconcileJob, len(touched))
		for i := range touched {
			if jobs[i], err = t.job(reflect.ValueOf(&touched[i]).Elem(), gone); err != nil {
				return err
			}
		}
		n = int64(len(touched))
		return db.insert(ctx, tx, jobs)
	})
	return n, err
}

// job builds the reconcile job for the row in v; with gone, it carries the
// row itself.
func (t *Table[T]) job(v reflect.Value, gone bool) (reconcileJob, error) {
	id, err := json.Marshal(v.FieldByIndex(t.key.index).Interface())
	if err != nil {
		return reconcileJob{}, fmt.Errorf("eddy: %s: encode key: %w", t.name, err)
	}
	job := reconcileJob{Object: t.kind, ID: id}
	if gone {
		fields := make(map[string]json.RawMessage, len(t.cols))
		for _, c := range t.cols {
			if fields[c.name], err = json.Marshal(v.FieldByIndex(c.index).Interface()); err != nil {
				return reconcileJob{}, fmt.Errorf("eddy: %s: encode deleted row, column %s: %w", t.name, c.name, err)
			}
		}
		if job.Gone, err = json.Marshal(fields); err != nil {
			return reconcileJob{}, fmt.Errorf("eddy: %s: encode deleted row: %w", t.name, err)
		}
	}
	return job, nil
}

// decodeGone rebuilds a deleted row from its job.
func (t *Table[T]) decodeGone(raw json.RawMessage) (*T, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	var row T
	v := reflect.ValueOf(&row).Elem()
	for _, c := range t.cols {
		val, ok := fields[c.name]
		if !ok {
			continue // column added since the row was deleted
		}
		if err := json.Unmarshal(val, v.FieldByIndex(c.index).Addr().Interface()); err != nil {
			return nil, fmt.Errorf("column %s: %w", c.name, err)
		}
	}
	return &row, nil
}

// Get returns the row with key id, or ErrNotFound.
func (t *Table[T]) Get(ctx context.Context, q Querier, id any) (*T, error) {
	if _, err := t.encodeKey(id); err != nil {
		return nil, err
	}
	sql := "SELECT " + quoteAll(t.cols) + " FROM " + quote(t.name) + " WHERE " + quote(t.key.name) + " = $1"
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

// Select returns the rows matching clause, which follows the FROM (for
// example "WHERE team_id = $1 ORDER BY name"); an empty clause returns every
// row.
func (t *Table[T]) Select(ctx context.Context, q Querier, clause string, args ...any) ([]T, error) {
	rows, err := q.Query(ctx, "SELECT "+quoteAll(t.cols)+" FROM "+quote(t.name)+" "+clause, args...)
	if err != nil {
		return nil, fmt.Errorf("eddy: select from %s: %w", t.name, err)
	}
	out, err := pgx.CollectRows(rows, t.scanRow)
	if err != nil {
		return nil, fmt.Errorf("eddy: select from %s: %w", t.name, err)
	}
	return out, nil
}

// SaveStatus writes row's status columns, and nothing else, to the row with
// the same key. It enqueues nothing. Reconcile already does this when the
// reconciler returns; call it yourself only to save progress midway.
func (t *Table[T]) SaveStatus(ctx context.Context, q Querier, row *T) error {
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

func (t *Table[T]) scanRow(r pgx.CollectableRow) (T, error) {
	var row T
	err := r.Scan(t.targets(reflect.ValueOf(&row).Elem())...)
	return row, err
}

// scanOne reads exactly one row into dst, or returns ErrNotFound.
func (t *Table[T]) scanOne(rows pgx.Rows, dst reflect.Value) error {
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

func (t *Table[T]) targets(v reflect.Value) []any {
	ptrs := make([]any, len(t.cols))
	for i, c := range t.cols {
		ptrs[i] = v.FieldByIndex(c.index).Addr().Interface()
	}
	return ptrs
}

// statusSnapshot encodes row's status fields, to tell later whether they
// changed. ok is false when they cannot be compared, in which case the
// caller saves unconditionally.
func (t *Table[T]) statusSnapshot(row *T) (snap string, ok bool) {
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

// stamp sets the "error" and "reconciled" fields from a reconcile's result.
func (t *Table[T]) stamp(row *T, err error) {
	_, retry := RetryDelay(err)
	failed := err != nil && (!retry || IsPermanent(err))
	v := reflect.ValueOf(row).Elem()
	for _, c := range t.cols {
		f := v.FieldByIndex(c.index)
		switch {
		case c.errorMsg && failed:
			setField(f, reflect.ValueOf(truncate(err.Error(), maxErrorLen)))
		case c.errorMsg:
			f.SetZero()
		case c.reconciled && err == nil:
			setField(f, reflect.ValueOf(time.Now()))
		}
	}
}

// setField assigns val to f, which has val's type or is a pointer to it.
func setField(f, val reflect.Value) {
	if f.Kind() == reflect.Pointer {
		p := reflect.New(f.Type().Elem())
		p.Elem().Set(val)
		f.Set(p)
		return
	}
	f.Set(val)
}

// Reconcile registers f as the reconciler for t's rows. eddy loads the row
// before each call, so f always sees current state; a row that no longer
// exists is skipped (see Teardown). Whatever f assigns to row's status
// fields is saved when it returns, on success and on failure alike.
//
// f must be idempotent. Returning an error retries with backoff; see Retry
// and Permanent. Every row is also reconciled again periodically (see
// WithResync).
func Reconcile[T any](t *Table[T], f func(ctx context.Context, row *T) error) Option {
	return func(c *config) error {
		if t == nil || f == nil {
			return errors.New("eddy: Reconcile: nil table or func")
		}
		reg := c.table(t.kind)
		if reg.reconcile != nil {
			return fmt.Errorf("eddy: Reconcile(%s): registered twice", t.kind)
		}
		reg.reconcile = func(ctx context.Context, pool *pgxpool.Pool, raw json.RawMessage) error {
			id := reflect.New(t.keyType)
			if err := json.Unmarshal(raw, id.Interface()); err != nil {
				return Permanent(fmt.Errorf("decode %s key %s: %w", t.kind, raw, err))
			}
			row, err := t.Get(ctx, pool, id.Elem().Interface())
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			before, known := t.statusSnapshot(row)
			rerr := f(ctx, row)
			t.stamp(row, rerr)
			if after, ok := t.statusSnapshot(row); !known || !ok || after != before {
				// Save even when the reconcile timed out or was cancelled.
				sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer cancel()
				if serr := t.SaveStatus(sctx, pool, row); serr != nil {
					if _, retry := RetryDelay(rerr); rerr == nil || retry {
						return serr
					}
					return errors.Join(rerr, serr)
				}
			}
			return rerr
		}
		reg.list = func(ctx context.Context, pool *pgxpool.Pool) ([]json.RawMessage, error) {
			rows, err := pool.Query(ctx, "SELECT "+quote(t.key.name)+" FROM "+quote(t.name))
			if err != nil {
				return nil, err
			}
			// Encode keys in Go, exactly as writes do.
			return pgx.CollectRows(rows, func(r pgx.CollectableRow) (json.RawMessage, error) {
				id := reflect.New(t.keyType)
				if err := r.Scan(id.Interface()); err != nil {
					return nil, err
				}
				return json.Marshal(id.Elem().Interface())
			})
		}
		return nil
	}
}

// Teardown registers f to run when a row of t is deleted through the Table
// (Delete, or Exec with a DELETE). row is the row as it was when deleted, so
// f knows what to tear down without a soft-delete column. It is retried like
// a reconcile until it returns nil.
func Teardown[T any](t *Table[T], f func(ctx context.Context, row *T) error) Option {
	return func(c *config) error {
		if t == nil || f == nil {
			return errors.New("eddy: Teardown: nil table or func")
		}
		reg := c.table(t.kind)
		if reg.teardown != nil {
			return fmt.Errorf("eddy: Teardown(%s): registered twice", t.kind)
		}
		reg.teardown = func(ctx context.Context, gone json.RawMessage) error {
			row, err := t.decodeGone(gone)
			if err != nil {
				return Permanent(fmt.Errorf("decode deleted %s row: %w", t.kind, err))
			}
			return f(ctx, row)
		}
		return nil
	}
}
