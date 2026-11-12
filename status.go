package eddy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Outcome describes one finished reconcile attempt, for Record.
type Outcome struct {
	// Err is the reconciler's error; nil on success.
	Err error
	// Result is what the reconciler returned on success.
	Result Result
	// Attempt is 1 for the first try.
	Attempt int
	// Final is true when no retry follows: success, a Permanent error, or
	// the last allowed attempt.
	Final bool
	// Duration is how long Reconcile ran.
	Duration time.Duration
}

// OK reports whether the reconcile succeeded.
func (o Outcome) OK() bool { return o.Err == nil }

// Record calls f after every reconcile attempt of kind k, success or
// failure, so the outcome can be written back to the object's row, where the
// API can show it. A failing f is logged and does not fail the reconcile.
// ColumnStatus builds an f for the common case.
func Record[ID any](k Kind[ID], f func(ctx context.Context, id ID, o Outcome) error) Option {
	return func(c *config) error {
		if k.name == "" {
			return fmt.Errorf("eddy: Record: zero Kind")
		}
		if f == nil {
			return fmt.Errorf("eddy: Record(%s): nil func", k.name)
		}
		if _, dup := c.recorders[k.name]; dup {
			return fmt.Errorf("eddy: Record(%s): kind recorded twice", k.name)
		}
		c.recorders[k.name] = func(ctx context.Context, raw json.RawMessage, o Outcome) error {
			var id ID
			if err := json.Unmarshal(raw, &id); err != nil {
				return fmt.Errorf("decode %s id %s: %w", k.name, raw, err)
			}
			return f(ctx, id, o)
		}
		return nil
	}
}

// Execer runs a statement. *pgxpool.Pool, *pgx.Conn and pgx.Tx satisfy it.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// StatusColumns names where ColumnStatus writes. Table is required, along
// with at least one of Error, ReconciledAt and AttemptedAt; empty columns
// are not written.
type StatusColumns struct {
	// Table may be schema-qualified ("app.apps").
	Table string
	// ID is the key column matched against the object's ID; default "id".
	ID string
	// Error (text) receives the error message, or NULL on success.
	Error string
	// ReconciledAt (timestamptz) is set to now() on success.
	ReconciledAt string
	// AttemptedAt (timestamptz) is set to now() on every attempt.
	AttemptedAt string
	// MaxErrorLen truncates the message, in bytes; default 1024.
	MaxErrorLen int
}

// ColumnStatus returns a Record func that updates status columns on the
// object's own row:
//
//	eddy.Record(Apps, eddy.ColumnStatus[uuid.UUID](pool, eddy.StatusColumns{
//		Table: "apps", Error: "reconcile_error", ReconciledAt: "reconciled_at",
//	}))
//
// It panics if cols names no table or no column, which is a programming
// error.
func ColumnStatus[ID any](db Execer, cols StatusColumns) func(ctx context.Context, id ID, o Outcome) error {
	if db == nil || cols.Table == "" || (cols.Error == "" && cols.ReconciledAt == "" && cols.AttemptedAt == "") {
		panic("eddy: ColumnStatus needs a db, a Table and at least one status column")
	}
	if cols.ID == "" {
		cols.ID = "id"
	}
	if cols.MaxErrorLen <= 0 {
		cols.MaxErrorLen = 1024
	}
	okSQL, failSQL := cols.statements()
	return func(ctx context.Context, id ID, o Outcome) error {
		var err error
		switch {
		case o.OK() && okSQL != "":
			_, err = db.Exec(ctx, okSQL, id)
		case !o.OK() && failSQL != "":
			args := []any{id}
			if cols.Error != "" {
				args = append(args, truncate(o.Err.Error(), cols.MaxErrorLen))
			}
			_, err = db.Exec(ctx, failSQL, args...)
		}
		if err != nil {
			return fmt.Errorf("record status in %s: %w", cols.Table, err)
		}
		return nil
	}
}

// statements builds the success and failure UPDATEs; either is empty when it
// would set nothing.
func (c StatusColumns) statements() (ok, fail string) {
	q := func(name string) string { return pgx.Identifier(strings.Split(name, ".")).Sanitize() }
	var okSet, failSet []string
	if c.Error != "" {
		okSet = append(okSet, q(c.Error)+" = NULL")
		failSet = append(failSet, q(c.Error)+" = $2")
	}
	if c.ReconciledAt != "" {
		okSet = append(okSet, q(c.ReconciledAt)+" = now()")
	}
	if c.AttemptedAt != "" {
		okSet = append(okSet, q(c.AttemptedAt)+" = now()")
		failSet = append(failSet, q(c.AttemptedAt)+" = now()")
	}
	build := func(set []string) string {
		if len(set) == 0 {
			return ""
		}
		return "UPDATE " + q(c.Table) + " SET " + strings.Join(set, ", ") + " WHERE " + q(c.ID) + " = $1"
	}
	return build(okSet), build(failSet)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// Do not cut a UTF-8 sequence in half.
	for n > 0 && n < len(s) && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n]
}
