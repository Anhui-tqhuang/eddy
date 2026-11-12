package eddy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DefaultQueue is the River queue eddy uses unless WithQueue says
	// otherwise.
	DefaultQueue = "eddy"

	defaultWorkers         = 10
	defaultShutdownTimeout = 30 * time.Second
	defaultResyncBatch     = 1000
)

// Option configures New and NewEnqueuer. NewEnqueuer uses only WithQueue and
// WithLogger and ignores the rest.
type Option func(*config) error

// handler reconciles one object; pool is the controller's pool, for handlers
// that load the row themselves.
type handler func(ctx context.Context, pool *pgxpool.Pool, id json.RawMessage) (Result, error)

type resync struct {
	every time.Duration
	list  func(ctx context.Context, pool *pgxpool.Pool) ([]json.RawMessage, error)
}

type config struct {
	queue           string
	workers         int
	elector         Elector
	logger          *slog.Logger
	shutdownTimeout time.Duration
	jobTimeout      time.Duration
	handlers        map[string]handler
	resyncs         map[string]resync
	recorders       map[string]recorder
	middleware      []Middleware
	onLeadership    []func(bool)
	objectLocks     bool
	needsPool       bool // a table reconciler or resync loads rows itself
}

type recorder func(ctx context.Context, id json.RawMessage, o Outcome) error

func newConfig(opts []Option) (*config, error) {
	c := &config{
		queue:           DefaultQueue,
		workers:         defaultWorkers,
		shutdownTimeout: defaultShutdownTimeout,
		handlers:        map[string]handler{},
		resyncs:         map[string]resync{},
		recorders:       map[string]recorder{},
	}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	return c, nil
}

// validateWorkers checks the options that only matter to the side that
// works jobs.
func (c *config) validateWorkers() error {
	if len(c.handlers) == 0 {
		return errors.New("eddy: no reconcilers registered (use eddy.Handle)")
	}
	for kind := range c.resyncs {
		if _, ok := c.handlers[kind]; !ok {
			return fmt.Errorf("eddy: Resync(%s) without Handle(%s)", kind, kind)
		}
	}
	for kind := range c.recorders {
		if _, ok := c.handlers[kind]; !ok {
			return fmt.Errorf("eddy: Record(%s) without Handle(%s)", kind, kind)
		}
	}
	return nil
}

// Handle registers r as the reconciler for kind k.
func Handle[ID any](k Kind[ID], r Reconciler[ID]) Option {
	return func(c *config) error {
		if k.name == "" {
			return fmt.Errorf("eddy: Handle: zero Kind")
		}
		if r == nil {
			return fmt.Errorf("eddy: Handle(%s): nil reconciler", k.name)
		}
		if _, dup := c.handlers[k.name]; dup {
			return fmt.Errorf("eddy: Handle(%s): kind registered twice", k.name)
		}
		c.handlers[k.name] = func(ctx context.Context, _ *pgxpool.Pool, raw json.RawMessage) (Result, error) {
			var id ID
			if err := json.Unmarshal(raw, &id); err != nil {
				return Done, Permanent(fmt.Errorf("decode %s id %s: %w", k.name, raw, err))
			}
			return r.Reconcile(ctx, id)
		}
		return nil
	}
}

// HandleFunc registers f as the reconciler for kind k.
func HandleFunc[ID any](k Kind[ID], f func(ctx context.Context, id ID) (Result, error)) Option {
	return Handle[ID](k, ReconcilerFunc[ID](f))
}

// Resync enqueues every object list returns, once when the controller becomes
// leader and then every interval. It repairs drift and anything a lost
// enqueue missed. list should return all live objects of the kind, plus
// deleted ones that may still need tearing down.
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

// WithQueue sets the River queue eddy inserts into and works. The default is
// DefaultQueue. API and controller must agree on it.
func WithQueue(name string) Option {
	return func(c *config) error {
		if name == "" {
			return fmt.Errorf("eddy: WithQueue: empty name")
		}
		c.queue = name
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
			return fmt.Errorf("eddy: WithElector: nil elector")
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
			return fmt.Errorf("eddy: WithShutdownTimeout: must be positive")
		}
		c.shutdownTimeout = d
		return nil
	}
}

// WithReconcileTimeout bounds a single Reconcile call. The default is River's
// job timeout (one minute).
func WithReconcileTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return fmt.Errorf("eddy: WithReconcileTimeout: must be positive")
		}
		c.jobTimeout = d
		return nil
	}
}
