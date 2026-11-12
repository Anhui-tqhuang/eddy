package eddy

import (
	"context"
	"fmt"
)

// Handler reconciles the object ref points at. Middleware wraps it.
type Handler func(ctx context.Context, ref Ref) (Result, error)

// Middleware wraps every reconcile, for every kind: the place for metrics,
// tracing, logging or panic recovery. The first middleware given is the
// outermost. Attempt(ctx) reports the attempt number inside it.
//
//	func timing(next eddy.Handler) eddy.Handler {
//		return func(ctx context.Context, ref eddy.Ref) (eddy.Result, error) {
//			start := time.Now()
//			res, err := next(ctx, ref)
//			observe(ref.Kind(), time.Since(start), err)
//			return res, err
//		}
//	}
type Middleware func(next Handler) Handler

// WithMiddleware appends middleware around every reconcile.
func WithMiddleware(m ...Middleware) Option {
	return func(c *config) error {
		for i, mw := range m {
			if mw == nil {
				return fmt.Errorf("eddy: WithMiddleware: middleware %d is nil", i)
			}
		}
		c.middleware = append(c.middleware, m...)
		return nil
	}
}

// OnLeadershipChange calls f with true when this replica starts leading and
// with false when it stops. f must not block. It applies to Controller only;
// with Embed, leadership is River's.
func OnLeadershipChange(f func(leading bool)) Option {
	return func(c *config) error {
		if f == nil {
			return fmt.Errorf("eddy: OnLeadershipChange: nil func")
		}
		c.onLeadership = append(c.onLeadership, f)
		return nil
	}
}

type attemptKey struct{}

// Attempt returns the attempt number (1 for the first try) of the reconcile
// running in ctx, or 0 outside one.
func Attempt(ctx context.Context) int {
	n, _ := ctx.Value(attemptKey{}).(int)
	return n
}

func withAttempt(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, attemptKey{}, n)
}
