package eddy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DefaultQueue is the River queue eddy uses unless WithQueue says
	// otherwise.
	DefaultQueue = "eddy"
	// DefaultResync is how often every row of a reconciled table is enqueued
	// again unless WithResync says otherwise.
	DefaultResync = 10 * time.Minute

	defaultWorkers         = 10
	defaultShutdownTimeout = 30 * time.Second
	insertBatch            = 1000
)

// Option configures Open and DB.Run. Open needs only the connection options
// (WithQueue, WithLogger, WithoutMigrate); everything else matters to Run.
// Options given to Open also apply to Run.
type Option func(*config) error

// handler reconciles the object job points at.
type handler func(ctx context.Context, pool *pgxpool.Pool, job reconcileJob) error

// lister returns the encoded ids of every object of a kind.
type lister func(ctx context.Context, pool *pgxpool.Pool) ([]json.RawMessage, error)

type resync struct {
	every time.Duration
	list  lister
}

// tableReg is what Reconcile and Teardown registered for one table.
type tableReg struct {
	reconcile func(ctx context.Context, pool *pgxpool.Pool, id json.RawMessage) error
	teardown  func(ctx context.Context, gone json.RawMessage) error
	list      lister
}

// handle tears down a deleted row, then reconciles whatever row has the key
// now: usually none, but the key may have been reused since the delete.
func (r *tableReg) handle(ctx context.Context, pool *pgxpool.Pool, job reconcileJob) error {
	if job.Gone != nil && r.teardown != nil {
		if err := r.teardown(ctx, job.Gone); err != nil {
			return err
		}
	}
	if r.reconcile == nil {
		return nil
	}
	return r.reconcile(ctx, pool, job.ID)
}

type recorder func(ctx context.Context, id json.RawMessage, o Outcome) error

type config struct {
	queue   string
	logger  *slog.Logger
	migrate bool

	workers         int
	elector         Elector
	shutdownTimeout time.Duration
	jobTimeout      time.Duration
	resyncEvery     time.Duration // tables; 0 turns it off
	objectLocks     bool

	handlers     map[string]handler // bare kinds
	resyncs      map[string]resync  // bare kinds
	tables       map[string]*tableReg
	recorders    map[string]recorder
	middleware   []Middleware
	onLeadership []func(bool)
}

func newConfig(opts []Option) (*config, error) {
	c := &config{
		queue:           DefaultQueue,
		migrate:         true,
		workers:         defaultWorkers,
		shutdownTimeout: defaultShutdownTimeout,
		resyncEvery:     DefaultResync,
		handlers:        map[string]handler{},
		resyncs:         map[string]resync{},
		tables:          map[string]*tableReg{},
		recorders:       map[string]recorder{},
	}
	if err := c.apply(opts); err != nil {
		return nil, err
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	return c, nil
}

func (c *config) apply(opts []Option) error {
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return err
		}
	}
	return nil
}

// with returns a copy of c with opts applied, leaving c untouched.
func (c *config) with(opts []Option) (*config, error) {
	cp := *c
	cp.handlers, cp.resyncs, cp.recorders = maps.Clone(c.handlers), maps.Clone(c.resyncs), maps.Clone(c.recorders)
	cp.tables = make(map[string]*tableReg, len(c.tables))
	for name, reg := range c.tables {
		r := *reg
		cp.tables[name] = &r
	}
	cp.middleware = slices.Clone(c.middleware)
	cp.onLeadership = slices.Clone(c.onLeadership)
	if err := cp.apply(opts); err != nil {
		return nil, err
	}
	if cp.queue != c.queue {
		return nil, errors.New("eddy: WithQueue belongs on Open, not Run")
	}
	if cp.logger == nil {
		cp.logger = slog.Default()
	}
	return &cp, nil
}

func (c *config) table(name string) *tableReg {
	if c.tables[name] == nil {
		c.tables[name] = &tableReg{}
	}
	return c.tables[name]
}

// plan merges bare kinds and tables into what the workers run.
func (c *config) plan() (map[string]handler, map[string]resync, error) {
	handlers, resyncs := maps.Clone(c.handlers), maps.Clone(c.resyncs)
	for name, reg := range c.tables {
		if _, dup := handlers[name]; dup {
			return nil, nil, fmt.Errorf("eddy: kind %q is registered both as a table and with Handle", name)
		}
		handlers[name] = reg.handle
		if reg.list != nil && c.resyncEvery > 0 {
			resyncs[name] = resync{every: c.resyncEvery, list: reg.list}
		}
	}
	if len(handlers) == 0 {
		return nil, nil, errors.New("eddy: nothing to reconcile (use eddy.Reconcile or eddy.Handle)")
	}
	for name := range c.resyncs {
		if _, ok := c.handlers[name]; !ok {
			return nil, nil, fmt.Errorf("eddy: Resync(%s) without Handle(%s)", name, name)
		}
	}
	for name := range c.recorders {
		if _, ok := c.handlers[name]; !ok {
			return nil, nil, fmt.Errorf("eddy: Record(%s) without Handle(%s)", name, name)
		}
	}
	return handlers, resyncs, nil
}

// Handle registers f as the reconciler for kind k. f gets only the id: it
// loads what it needs, must be idempotent, and treats a missing object as
// "tear everything down". Returning an error retries with backoff; see Retry
// and Permanent. For a table, use Reconcile.
func Handle[ID any](k Kind[ID], f func(ctx context.Context, id ID) error) Option {
	return func(c *config) error {
		if k.name == "" {
			return errors.New("eddy: Handle: zero Kind")
		}
		if f == nil {
			return fmt.Errorf("eddy: Handle(%s): nil func", k.name)
		}
		if _, dup := c.handlers[k.name]; dup {
			return fmt.Errorf("eddy: Handle(%s): kind registered twice", k.name)
		}
		c.handlers[k.name] = func(ctx context.Context, _ *pgxpool.Pool, job reconcileJob) error {
			var id ID
			if err := json.Unmarshal(job.ID, &id); err != nil {
				return Permanent(fmt.Errorf("decode %s id %s: %w", k.name, job.ID, err))
			}
			return f(ctx, id)
		}
		return nil
	}
}

// Resync enqueues every object list returns, once when Run starts leading
// and then every interval. Tables resync by themselves; this is for kinds
// registered with Handle.
func Resync[ID any](k Kind[ID], every time.Duration, list func(ctx context.Context) ([]ID, error)) Option {
	return func(c *config) error {
		if every <= 0 {
			return fmt.Errorf("eddy: Resync(%s): interval must be positive", k.name)
		}
		if list == nil {
			return fmt.Errorf("eddy: Resync(%s): nil list", k.name)
		}
		c.resyncs[k.name] = resync{every: every, list: func(ctx context.Context, _ *pgxpool.Pool) ([]json.RawMessage, error) {
			ids, err := list(ctx)
			if err != nil {
				return nil, err
			}
			raws := make([]json.RawMessage, len(ids))
			for i, id := range ids {
				raws[i] = k.Of(id).id
			}
			return raws, nil
		}}
		return nil
	}
}

// WithResync sets how often every row of each reconciled table is enqueued
// again, to repair drift and catch rows changed outside eddy. The default is
// DefaultResync; zero turns it off.
func WithResync(every time.Duration) Option {
	return func(c *config) error {
		if every < 0 {
			return errors.New("eddy: WithResync: negative interval")
		}
		c.resyncEvery = every
		return nil
	}
}

// WithQueue sets the River queue eddy inserts into and works. The default is
// DefaultQueue. Every process must agree on it.
func WithQueue(name string) Option {
	return func(c *config) error {
		if name == "" {
			return errors.New("eddy: WithQueue: empty name")
		}
		c.queue = name
		return nil
	}
}

// WithoutMigrate stops Open from creating or upgrading River's tables; Open
// then fails when they are missing or out of date. Use it when migrations
// run as a separate step (see Migrate).
func WithoutMigrate() Option {
	return func(c *config) error {
		c.migrate = false
		return nil
	}
}

// WithWorkers sets how many reconciles run at once. The default is 10.
func WithWorkers(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return fmt.Errorf("eddy: WithWorkers: need at least 1, got %d", n)
		}
		c.workers = n
		return nil
	}
}

// WithElector sets how replicas decide which one works the queue. The
// default is a Postgres advisory lock named after the queue.
func WithElector(e Elector) Option {
	return func(c *config) error {
		if e == nil {
			return errors.New("eddy: WithElector: nil elector")
		}
		c.elector = e
		return nil
	}
}

// WithLogger sets the logger; the default is slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(c *config) error {
		c.logger = l
		return nil
	}
}

// WithShutdownTimeout bounds how long Run waits for in-flight reconciles when
// it stops or loses leadership, before cancelling them. The default is 30s.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return errors.New("eddy: WithShutdownTimeout: must be positive")
		}
		c.shutdownTimeout = d
		return nil
	}
}

// WithReconcileTimeout bounds a single reconcile. The default is River's job
// timeout (one minute).
func WithReconcileTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return errors.New("eddy: WithReconcileTimeout: must be positive")
		}
		c.jobTimeout = d
		return nil
	}
}
