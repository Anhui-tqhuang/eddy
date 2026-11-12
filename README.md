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

Describe the table as a Go struct. Fields tagged `status` belong to the controller; everything else belongs to the API.

```go
type App struct {
    ID        uuid.UUID `db:"id,default"`         // "default": left to the database when zero
    Name      string    `db:"name"`
    Image     string    `db:"image"`
    CreatedAt time.Time `db:"created_at,default"`
    Phase     string    `db:"phase,status"`       // "status": written only by the controller
    URL       *string   `db:"url,status"`
}

var Apps = eddy.NewTable[App, uuid.UUID]("apps")  // struct, key type, table name
```

**API process:** use `eddy.DB` like a pgx pool. Writes through the table enqueue the reconcile themselves, in the same transaction as the row.

```go
db, _ := eddy.Open(ctx, pool)

app := App{Name: "web", Image: "web:1"}
Apps.Insert(ctx, db, &app)        // app.ID and app.CreatedAt are filled in
app.Image = "web:2"
Apps.Update(ctx, db, &app)        // never touches phase or url
Apps.Delete(ctx, db, app.ID)

got, err := Apps.Get(ctx, db, id) // eddy.ErrNotFound when missing
list, _ := Apps.Select(ctx, db, "WHERE name = $1", "web")

db.Query(ctx, "SELECT ...")       // anything else: plain pgx passthrough
```

If the write commits, the reconcile is guaranteed to happen. If it rolls back, nothing happens. There is no separate enqueue call, no dual write and no outbox table.

**Controller process:** run one or more replicas. eddy loads the row for you and saves the status fields you assign.

```go
ctl, _ := eddy.New(ctx, pool,
    eddy.Reconcile(Apps, reconcileApp),
    eddy.ResyncAll(Apps, 10*time.Minute),
)
ctl.Run(ctx) // blocks; only the elected leader reconciles

func reconcileApp(ctx context.Context, id uuid.UUID, app *App) (eddy.Result, error) {
    if app == nil {                      // the row is gone
        return eddy.Done, teardown(ctx, id)
    }
    url, err := deploy(ctx, app)
    if err != nil {
        app.Phase = "failed"             // saved even though we return an error
        return eddy.Done, err            // retried with backoff
    }
    app.Phase, app.URL = "ready", &url   // saved when we return
    return eddy.Done, nil
}
```

**Schema:** eddy keeps its jobs in River's tables. Run this from your migration step:

```go
eddy.Migrate(ctx, pool)
```

## Working with tables

**Transactions.** Table writes take a `*eddy.DB` (each write is its own transaction) or a `*eddy.Tx` (writes join it). A `Tx` has every `pgx.Tx` method.

```go
db.InTx(ctx, func(tx *eddy.Tx) error {
    if err := Teams.Insert(ctx, tx, &team); err != nil {
        return err
    }
    return Apps.Insert(ctx, tx, &app)   // both rows and both jobs, or none
})

tx := db.WithTx(pgxTx)                  // join a pgx transaction you already hold
```

**Raw SQL is not captured.** eddy can't tell which rows an arbitrary statement touched. After a raw write to a reconciled table, enqueue the rows yourself with `tx.Enqueue(ctx, Apps.Of(id))`, or let `ResyncAll` pick them up later.

**Soft delete.** With `eddy.SoftDelete("deleted_at")`, `Delete` stamps the column instead of removing the row, so the reconciler can still read what it has to tear down. `Get` and `Update` treat the row as gone. The reconciler calls `Apps.Purge(ctx, pool, id)` when teardown is complete.

```go
var Apps = eddy.NewTable[App, uuid.UUID]("apps", eddy.SoftDelete("deleted_at"))

func reconcileApp(ctx context.Context, id uuid.UUID, app *App) (eddy.Result, error) {
    if app != nil && app.DeletedAt != nil {
        if err := teardown(ctx, app); err != nil {
            return eddy.Done, err
        }
        return eddy.Done, Apps.Purge(ctx, pool, id)
    }
    ...
}
```

**Mapping rules.**

| Tag | Meaning |
|---|---|
| `db:"col"` | Column name. Without a tag, the snake_case field name (`CreatedAt` is `created_at`) |
| `db:"col,default"` | Omitted from `Insert` when zero, so the database default applies |
| `db:"col,status"` | Never written by `Insert` or `Update`; saved by the reconciler |
| `db:"-"` | Not a column |

Other table options are `eddy.Key("uid")` for a key column not called `id`, and `eddy.KindName("app")` to name the job kind differently from the table. Embedded structs are flattened. `Apps.SaveStatus(ctx, pool, app)` saves status midway through a long reconcile.

## Without a table

The table layer sits on a smaller core that you can use directly, for objects that aren't one row of one table. Declare a kind, enqueue it yourself, and load whatever you need in the reconciler:

```go
var Clusters = eddy.NewKind[uuid.UUID]("cluster")   // any JSON-encodable ID

tx, _ := pool.Begin(ctx)
// ... your own SQL ...
db.EnqueueTx(ctx, tx, Clusters.Of(clusterID))
tx.Commit(ctx)

eddy.New(ctx, pool,
    eddy.HandleFunc(Clusters, func(ctx context.Context, id uuid.UUID) (eddy.Result, error) { ... }),
    eddy.Resync(Clusters, 10*time.Minute, listClusterIDs),
)
```

`eddy.RequeueAfter(d)` as the result reconciles the object again later, for polling something that is still converging.

## How it works

```
 Table.Insert/Update/Delete ──tx──▶ your row + river_job ──▶ leader replica ──▶ reconcile(id, row) ──▶ outside world
                                                              ▲                        │
                                             Resync every N ──┘      status fields on your row ◀┘
```

- **Jobs carry a pointer, not data.** A job holds only `kind` and `id`. The row is reloaded before every reconcile, so stale or duplicate jobs are harmless.
- **One leader, elected in Postgres.** Replicas compete for a session-level advisory lock (`pg_try_advisory_lock`). The leader holds one pooled connection, and if that connection drops, the lease is gone. On a clean handoff, the old leader drains its in-flight reconciles before it releases the lock. If its connection drops instead, Postgres frees the lock at once while the old leader is still draining. Add `WithObjectLocks()` to rule out overlap even then. You don't need Kubernetes, etcd or ZooKeeper.
- **No overlap per object.** One object is never reconciled twice at once within a process, even if it was enqueued many times. `WithObjectLocks()` extends that across processes with a per-object advisory lock.
- **Errors retry, `Permanent` stops.** A returned error retries with River's backoff. `eddy.Permanent(err)` cancels the job. Use it when only a new write can fix the row.
- **Resync repairs drift.** `ResyncAll` enqueues every row when a replica becomes leader, then on an interval. It also catches rows changed by raw SQL or other tools.
- **Typed end to end.** The table fixes the row and key types, so the reconciler receives `*App` and `Apps.Get(ctx, db, 42)` doesn't compile when the key is a UUID.

## Recording every attempt

Status fields cover what the reconciler decides to record. `Record` is for bookkeeping around it: it runs after every attempt, success or failure, with the error, attempt number and duration. `ColumnStatus` writes the common columns:

```go
eddy.New(ctx, pool,
    eddy.Reconcile(Apps, reconcileApp),
    eddy.Record(Apps.Kind(), eddy.ColumnStatus[uuid.UUID](pool, eddy.StatusColumns{
        Table:        "apps",
        Error:        "reconcile_error", // message on failure, NULL on success
        ReconciledAt: "reconciled_at",   // now() on success
        AttemptedAt:  "attempted_at",    // now() on every attempt
    })),
)
```

For anything else, such as a separate status table, a `phase` enum or events, pass your own `func(ctx, id, eddy.Outcome) error`. `Outcome` carries `Err`, `Result`, `Attempt`, `Duration` and `Final`. `Final` is true when no retry follows, which makes it the moment to mark a row `failed`. Record errors are logged and never fail the reconcile.

## Metrics and tracing

Middleware wraps every reconcile of every kind. eddy itself depends on neither OpenTelemetry nor Prometheus:

```go
func tracing(next eddy.Handler) eddy.Handler {
    return func(ctx context.Context, ref eddy.Ref) (eddy.Result, error) {
        ctx, span := tracer.Start(ctx, "reconcile "+ref.Kind(), trace.WithAttributes(
            attribute.String("eddy.id", ref.ID()),
            attribute.Int("eddy.attempt", eddy.Attempt(ctx)),
        ))
        defer span.End()
        res, err := next(ctx, ref)
        if err != nil {
            span.RecordError(err)
        }
        return res, err
    }
}

func metrics(next eddy.Handler) eddy.Handler {
    return func(ctx context.Context, ref eddy.Ref) (eddy.Result, error) {
        start := time.Now()
        res, err := next(ctx, ref)
        reconcileSeconds.WithLabelValues(ref.Kind(), strconv.FormatBool(err == nil)).
            Observe(time.Since(start).Seconds())
        return res, err
    }
}

eddy.New(ctx, pool, ...,
    eddy.WithMiddleware(tracing, metrics), // first is outermost
    eddy.OnLeadershipChange(func(leading bool) { isLeader.Store(leading) }),
)
```

## Already running River?

`Embed` hands eddy's workers to your River client instead of starting its own:

```go
ed, _ := eddy.Embed(pool,
    eddy.Reconcile(Apps, reconcileApp),
    eddy.ResyncAll(Apps, 10*time.Minute),
)

workers := river.NewWorkers()
ed.AddWorkers(workers)
river.AddWorker(workers, &YourOtherWorker{})

client, _ := river.NewClient(riverpgxv5.New(pool), &river.Config{
    Queues: map[string]river.QueueConfig{
        river.QueueDefault: {MaxWorkers: 50},
        ed.Queue():         ed.QueueConfig(),
    },
    Workers:      workers,
    PeriodicJobs: ed.PeriodicJobs(),
})

enq, _ := eddy.EnqueuerFor(client)
db := eddy.Wrap(pool, enq)         // same eddy.DB as before
```

eddy doesn't elect a leader in this mode. Every process whose client lists `ed.Queue()` works it. List the queue on one process only, or add `WithObjectLocks()` so processes never overlap on an object. Resync jobs run on River's own elected leader.

## Options

| Option | Default | |
|---|---|---|
| `Reconcile(table, f)` | — | Register a reconciler for a table's rows |
| `ResyncAll(table, every)` | off | Periodically enqueue every row of a table |
| `Handle(kind, r)` / `HandleFunc(kind, f)` | — | Register a reconciler for a bare kind |
| `Resync(kind, every, list)` | off | Periodically enqueue the listed objects of a kind |
| `WithWorkers(n)` | 10 | Concurrent reconciles on the leader |
| `WithQueue(name)` | `eddy` | River queue; API and controller must match |
| `WithElector(e)` | `AdvisoryLock(pool, queue)` | Leader election; `Solo()` for a single replica |
| `WithReconcileTimeout(d)` | 1m | Bound on one `Reconcile` call |
| `WithShutdownTimeout(d)` | 30s | Drain time on stop or lost leadership |
| `WithObjectLocks()` | off | Per-object advisory lock across processes; holds one pool connection per running reconcile |
| `Record(kind, f)` | off | Write each attempt's outcome back (see `ColumnStatus`) |
| `WithMiddleware(m...)` | — | Wrap every reconcile (metrics, tracing, logging) |
| `OnLeadershipChange(f)` | — | Called on gaining/losing leadership (`Controller` only) |
| `WithLogger(l)` | `slog.Default()` | |

`Controller.Leading()` reports leadership, for a readiness probe or metrics.

## Development

```sh
make test      # unit tests
make test-db   # plus integration tests against Postgres
               # (TEST_DB_URL=postgres://localhost:5432/eddy_test?sslmode=disable)
```

## Roadmap

- Ready-made OpenTelemetry and Prometheus middleware, as separate modules so the core stays dependency-light
- Per-kind worker limits and priorities

## License

MIT
