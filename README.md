<p align="center">
  <img src="docs/assets/logo-mark.svg" alt="eddy" width="160">
</p>

<h1 align="center">eddy</h1>

<p align="center">Reconcile loops for Go, with Postgres as the only moving part.<br>Built on <a href="https://riverqueue.com">River</a>.</p>

---

> **Status:** pre-alpha. The API may still change.

An eddy is a current that curls back on itself inside a river. eddy is the reconcile loop that runs on River.

Your API owns plain Postgres rows: the desired state. eddy turns those rows into real resources somewhere else, such as cloud resources, Kubernetes objects, DNS records or third-party SaaS state, and keeps them converged. It's the controller pattern without a Kubernetes API server. Postgres stores the state, carries the queue and elects the leader.

## Install

```sh
go get github.com/Anhui-tqhuang/eddy
```

## Quick start

Describe the table as a Go struct, then use one `eddy.DB` on both sides.

```go
type App struct {
    ID    uuid.UUID `db:",default"`  // filled by the database when zero
    Name  string
    Image string
    Phase string    `db:",status"`   // written only by the controller
}

var Apps = eddy.NewTable[App]("apps")

db, _ := eddy.Open(ctx, pool)        // creates eddy's job tables if needed
```

**API:** write through the table. The reconcile is enqueued for you, in the same transaction as the row.

```go
app := App{Name: "web", Image: "web:1"}
Apps.Insert(ctx, db, &app)           // app.ID is filled in
app.Image = "web:2"
Apps.Update(ctx, db, &app)           // never touches phase
Apps.Delete(ctx, db, app.ID)
```

**Controller:** run it on any number of replicas. One is elected leader and does the work.

```go
db.Run(ctx,
    eddy.Reconcile(Apps, func(ctx context.Context, app *App) error {
        if err := deploy(ctx, app); err != nil {
            return err               // retried with backoff
        }
        app.Phase = "ready"          // saved when the function returns
        return nil
    }),
    eddy.Teardown(Apps, func(ctx context.Context, app *App) error {
        return undeploy(ctx, app)    // app is the row as it was when deleted
    }),
)
```

If a write commits, its reconcile is guaranteed to happen. If it rolls back, nothing happens. There is no enqueue call to remember, no dual write and no outbox table.

## Writing rows

`eddy.DB` is used like a pgx pool, and `*eddy.Tx` has every `pgx.Tx` method.

```go
got, err := Apps.Get(ctx, db, id)                          // eddy.ErrNotFound when missing
list, _ := Apps.Select(ctx, db, "WHERE name = $1", "web")
db.Query(ctx, "SELECT ...")                                // anything else: plain pgx
```

**Transactions.** Table writes take the `DB` (each write is its own transaction) or a `Tx` (writes join it).

```go
db.InTx(ctx, func(tx *eddy.Tx) error {
    if err := Teams.Insert(ctx, tx, &team); err != nil {
        return err
    }
    return Apps.Insert(ctx, tx, &app)    // both rows and both jobs, or none
})

tx := db.WithTx(pgxTx)                   // join a pgx transaction you already hold
```

**Free-form SQL.** `Table.Exec` runs your own INSERT, UPDATE or DELETE and enqueues every row it touched. Use it for bulk changes and anything the typed methods can't express.

```go
n, err := Apps.Exec(ctx, db, "UPDATE apps SET image = $1 WHERE team_id = $2", image, teamID)
```

eddy finds the touched rows by adding a `RETURNING` clause, so the statement must be a single statement without its own `RETURNING` and without a table alias.

**What is not captured.** `db.Exec`, psql and other services write rows without enqueueing anything. Those rows are picked up by the periodic resync, or at once if you call `db.Enqueue(ctx, Apps.Of(id))`.

## Reconciling

- **The row is always fresh.** eddy loads it before each call, so the function sees current state however stale the job is. It must be idempotent.
- **Status is saved for you.** Whatever the function assigns to `status` fields is written back when it returns, whether it succeeded or failed. `Apps.SaveStatus(ctx, db, app)` saves midway through a long reconcile.
- **Errors retry.** A returned error retries with backoff. `eddy.Permanent(err)` stops retrying, for a row only a new write can fix. `eddy.Retry(15*time.Second)` checks again later without counting as a failure, for polling something that is still converging.
- **Deletes carry the row.** `Delete` sends the row's last state with the job, and `Teardown` receives it. You don't need a soft-delete column to know what to tear down. Teardowns retry like reconciles.
- **Every row is re-checked.** Each table is resynced every 10 minutes by default, and when a replica becomes leader. `eddy.WithResync(d)` changes the interval; zero turns it off.
- **No overlap per object.** One row is never reconciled twice at once.

eddy can also record how each reconcile went, with two more field tags:

```go
type App struct {
    ...
    Error        *string    `db:"reconcile_error,error"`  // last error; cleared on success
    ReconciledAt *time.Time `db:",reconciled"`            // time of the last success
}
```

## Field tags

| Tag | Meaning |
|---|---|
| `db:"col"` | Column name. Without one, the snake_case field name (`CreatedAt` is `created_at`) |
| `db:",default"` | Left out of `Insert` when zero, so the database default applies |
| `db:",status"` | Never written by `Insert` or `Update`; saved after each reconcile |
| `db:",error"` | A status `string` or `*string` that eddy sets to the last reconcile error |
| `db:",reconciled"` | A status `time.Time` or `*time.Time` that eddy sets on each success |
| `db:"-"` | Not a column |

The key column is `id` unless you pass `eddy.Key("uid")`. `eddy.KindName("app")` names the job kind differently from the table. Embedded structs are flattened. Fields must survive `encoding/json`, which is how a deleted row reaches `Teardown`.

## How it works

```
 Table.Insert/Update/Delete/Exec ──tx──▶ your row + river_job ──▶ leader ──▶ Reconcile(row) ──▶ outside world
                                                                    ▲             │
                                                   resync every N ──┘   status fields on your row ◀┘
```

- **Jobs carry a pointer, not data.** A job holds only the kind and the key, and the row is reloaded before every reconcile. The one exception is a delete, which carries the deleted row.
- **One leader, elected in Postgres.** Replicas compete for a session-level advisory lock. The leader holds one pooled connection, and if that connection drops, the lease is gone. On a clean handoff the old leader drains its in-flight reconciles before it releases the lock. If its connection drops instead, Postgres frees the lock at once while the old leader is still draining. Add `WithObjectLocks()` to rule out overlap even then. You don't need Kubernetes, etcd or ZooKeeper.
- **Jobs live in River's tables.** `Open` creates and upgrades them. To run migrations as a separate step, call `eddy.Migrate(ctx, pool)` there and pass `eddy.WithoutMigrate()` to `Open`.

## Without a table

Under the table layer is a smaller core for objects that aren't one row of one table. Declare a kind, enqueue it yourself, and load what you need in the handler:

```go
var Clusters = eddy.NewKind[uuid.UUID]("cluster")   // any JSON-encodable ID

db.InTx(ctx, func(tx *eddy.Tx) error {
    // ... your own SQL on tx ...
    return tx.Enqueue(ctx, Clusters.Of(clusterID))
})

db.Run(ctx,
    eddy.Handle(Clusters, func(ctx context.Context, id uuid.UUID) error { ... }),
    eddy.Resync(Clusters, 10*time.Minute, listClusterIDs),
    eddy.Record(Clusters, eddy.ColumnStatus[uuid.UUID](db, eddy.StatusColumns{
        Table: "clusters", Error: "reconcile_error", ReconciledAt: "reconciled_at",
    })),
)
```

`Record` runs after every attempt with an `Outcome` (error, attempt number, duration, whether a retry follows). `ColumnStatus` writes the usual columns; pass your own function for anything else.

## Scheduling

Declare how a kind is scheduled where you declare the table or kind, so the API and the controller agree:

```go
var Reports = eddy.NewTable[Report]("reports",
    eddy.MaxWorkers(2),   // at most 2 report reconciles at once
    eddy.Priority(3),     // 1 (default) is most urgent, 4 least
)
```

- **`MaxWorkers(n)`** gives the kind a queue of its own with `n` workers, on top of the shared pool sized by `WithWorkers`. A slow kind can't crowd out the others, and the others can't starve it.
- **`Priority(n)`** orders kinds that share a queue. Lower-priority reconciles are picked up only when nothing more urgent is waiting.

Both also work on `eddy.NewKind`.

## Metrics and tracing

Ready-made instrumentation lives in two small modules, so the core depends on neither OpenTelemetry nor Prometheus.

**OpenTelemetry** (`go get github.com/Anhui-tqhuang/eddy/eddyotel`): a span and a duration measurement per reconcile.

```go
db.Run(ctx, ...,
    eddy.WithMiddleware(eddyotel.Middleware()),   // uses the global providers by default
)
```

Spans are named `eddy.reconcile <kind>` or `eddy.teardown <kind>` and carry `eddy.kind`, `eddy.id`, `eddy.attempt` and `eddy.outcome`. The histogram is `eddy.reconcile.duration`, in seconds. Spans the reconciler starts become children.

**Prometheus** (`go get github.com/Anhui-tqhuang/eddy/eddyprom`):

```go
m, _ := eddyprom.New(prometheus.DefaultRegisterer)

db.Run(ctx, ...,
    eddy.WithMiddleware(m.Middleware()),
    eddy.OnLeadershipChange(m.SetLeading),
)
```

| Metric | Labels | |
|---|---|---|
| `eddy_reconcile_duration_seconds` | `kind`, `operation`, `outcome` | Histogram of reconcile and teardown durations |
| `eddy_reconciles_in_flight` | `kind` | Reconciles running now |
| `eddy_leader` | — | 1 while this replica leads |

In both, the outcome is `ok`, `retry` (asked to be checked again later), `error` (will be retried) or `permanent`.

**Your own middleware** wraps every reconcile and teardown the same way:

```go
func logging(next eddy.Handler) eddy.Handler {
    return func(ctx context.Context, ref eddy.Ref) error {
        err := next(ctx, ref)
        slog.InfoContext(ctx, "reconciled", "kind", ref.Kind(), "id", ref.ID(),
            "attempt", eddy.Attempt(ctx), "teardown", ref.Deleted(), "error", err)
        return err
    }
}
```

The first middleware given is the outermost. `db.Leading()` reports leadership, for a readiness probe.

## Already running River?

`Embed` hands eddy's workers to your River client instead of `Run` starting its own:

```go
ed, _ := eddy.Embed(pool, eddy.Reconcile(Apps, reconcileApp))

workers := river.NewWorkers()
ed.AddWorkers(workers)
river.AddWorker(workers, &YourOtherWorker{})

queues := ed.Queues()              // eddy's queues and their sizes
queues[river.QueueDefault] = river.QueueConfig{MaxWorkers: 50}

client, _ := river.NewClient(riverpgxv5.New(pool), &river.Config{
    Queues:       queues,
    Workers:      workers,
    PeriodicJobs: ed.PeriodicJobs(),
})

db := ed.DB(client)                // the same eddy.DB, enqueueing through your client
```

An API-only process uses `eddy.Attach(pool, client)` to get the `DB`. eddy doesn't elect a leader in this mode: every process whose client lists `ed.Queues()` works them. List them on one process only, or add `WithObjectLocks()` so processes never overlap on a row. Resync jobs run on River's own elected leader, and you run River's migrations.

## Options

| Option | Default | |
|---|---|---|
| `Reconcile(table, f)` | — | Reconcile a table's rows |
| `Teardown(table, f)` | — | Tear down a table's deleted rows |
| `WithResync(every)` | 10m | How often every row is re-checked; 0 turns it off |
| `Handle(kind, f)` | — | Reconcile a bare kind |
| `Resync(kind, every, list)` | off | Periodically enqueue the listed objects of a bare kind |
| `Record(kind, f)` | off | Record each attempt of a bare kind (see `ColumnStatus`) |
| `WithWorkers(n)` | 10 | Concurrent reconciles on the shared queue; kinds with `MaxWorkers` run on top |
| `WithElector(e)` | advisory lock | Leader election; `Solo()` for a single replica |
| `WithObjectLocks()` | off | Per-object advisory lock across processes; holds one pool connection per running reconcile |
| `WithReconcileTimeout(d)` | 1m | Bound on one reconcile |
| `WithShutdownTimeout(d)` | 30s | Drain time on stop or lost leadership |
| `WithMiddleware(m...)` | — | Wrap every reconcile (metrics, tracing, logging) |
| `OnLeadershipChange(f)` | — | Called on gaining or losing leadership |
| `WithQueue(name)` | `eddy` | River queue; set it on `Open` |
| `WithoutMigrate()` | — | Don't create River's tables in `Open` |
| `WithLogger(l)` | `slog.Default()` | |

Options can go to `Open` or to `Run`; those given to `Open` apply to every `Run` on that `DB`.

## Development

```sh
make test      # unit tests
make test-db   # plus integration tests against Postgres
               # (TEST_DB_URL=postgres://localhost:5432/eddy_test?sslmode=disable)
```

## License

MIT
