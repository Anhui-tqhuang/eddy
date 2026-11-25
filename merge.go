package eddy

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// One object, one waiting job. Every write enqueues, a resync enqueues every object, and a
// reconcile may ask for a Retry: left alone, an object that keeps converging slowly gains
// one more job (and one more retry loop) at every resync. River's unique jobs cannot help:
// they always count running jobs, and a write while the object's reconcile runs must enqueue,
// since that reconcile may have read the row before the write. So eddy merges by itself:
//
//   - a write finds the object's waiting job and uses it, bringing it forward when it
//     waits for later (a Retry, or backoff after an error); only without one it inserts;
//   - a resync skips an object that has a waiting job;
//   - a reconcile that asks for a Retry while another job for its object waits finishes
//     instead, and that job carries on;
//   - a resync cancels all but the earliest waiting job of each object of its kind. Two jobs
//     in flight at once can each miss the other (River records a job's outcome just after it
//     returns) and both snooze; the next resync takes the loops back to one.
//
// A deleted row's job carries the row's last state and is never merged. Two writes that
// race may still both insert; that is rare and costs one extra reconcile.

// waitingStates are the states of a reconcile job that has not started.
const waitingStates = `('available', 'scheduled', 'retryable', 'pending')`

// waitingJob is an object's reconcile job that has not started.
type waitingJob struct {
	id    int64
	later bool // scheduled for later than now
}

// querier runs a query: a pool or a transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// waitingJobs returns, by index into jobs, the waiting job of each job's object, other than
// the job with id except. Jobs of deleted rows are never looked up.
func waitingJobs(ctx context.Context, q querier, jobs []reconcileJob, except int64) (map[int]waitingJob, error) {
	keys := make([]string, 0, len(jobs))
	index := make([]int, 0, len(jobs))
	for i, j := range jobs {
		if j.Gone != nil {
			continue
		}
		key, err := json.Marshal(struct {
			Object string          `json:"kind"`
			ID     json.RawMessage `json:"id"`
		}{j.Object, j.ID})
		if err != nil {
			return nil, fmt.Errorf("eddy: job key: %w", err)
		}
		keys = append(keys, string(key))
		index = append(index, i)
	}
	out := map[int]waitingJob{}
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, `
		SELECT w.n, j.id, j.later
		FROM unnest($1::jsonb[]) WITH ORDINALITY AS w(key, n)
		CROSS JOIN LATERAL (
			SELECT id, scheduled_at > now() AS later FROM river_job
			WHERE kind = 'eddy.reconcile' AND state IN `+waitingStates+`
			  AND args @> w.key AND NOT args ? 'gone' AND id <> $2
			ORDER BY scheduled_at LIMIT 1
		) j`, keys, except)
	if err != nil {
		return nil, fmt.Errorf("eddy: find waiting jobs: %w", err)
	}
	var n, id int64
	var later bool
	_, err = pgx.ForEachRow(rows, []any{&n, &id, &later}, func() error {
		out[index[n-1]] = waitingJob{id: id, later: later}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("eddy: find waiting jobs: %w", err)
	}
	return out, nil
}

// withoutDuplicates drops later jobs for an object already in jobs (deletions are kept).
func withoutDuplicates(jobs []reconcileJob) []reconcileJob {
	seen := map[string]bool{}
	out := jobs[:0:0]
	for _, j := range jobs {
		if j.Gone == nil {
			key := j.Object + "\x00" + string(j.ID)
			if seen[key] {
				continue
			}
			seen[key] = true
		}
		out = append(out, j)
	}
	return out
}

// extraWaitingJobs returns, for every object of kind with more than one waiting job, the ids
// of all but its earliest one.
func extraWaitingJobs(ctx context.Context, q querier, kind string) ([]int64, error) {
	rows, err := q.Query(ctx, `
		SELECT id FROM (
			SELECT id, row_number() OVER (PARTITION BY args->'id' ORDER BY scheduled_at, id) AS n
			FROM river_job
			WHERE kind = 'eddy.reconcile' AND state IN `+waitingStates+`
			  AND args->>'kind' = $1 AND NOT args ? 'gone'
		) w WHERE n > 1`, kind)
	if err != nil {
		return nil, fmt.Errorf("eddy: find extra waiting jobs: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("eddy: find extra waiting jobs: %w", err)
	}
	return ids, nil
}
