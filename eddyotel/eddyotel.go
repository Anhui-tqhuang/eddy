// Package eddyotel instruments eddy reconciles with OpenTelemetry: a span
// and a duration measurement for every reconcile and teardown.
//
//	db.Run(ctx,
//		eddy.Reconcile(Apps, reconcileApp),
//		eddy.WithMiddleware(eddyotel.Middleware()),
//	)
//
// It lives in its own module so that eddy itself does not depend on
// OpenTelemetry.
package eddyotel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Anhui-tqhuang/eddy"
)

const scope = "github.com/Anhui-tqhuang/eddy/eddyotel"

// Outcome values of the eddy.outcome attribute.
const (
	OutcomeOK        = "ok"        // reconciled
	OutcomeRetry     = "retry"     // asked to be checked again later (eddy.Retry)
	OutcomeError     = "error"     // failed; will be retried
	OutcomePermanent = "permanent" // failed for good (eddy.Permanent)
)

// Option configures Middleware.
type Option func(*config)

type config struct {
	tracers trace.TracerProvider
	meters  metric.MeterProvider
}

// WithTracerProvider sets where spans go; the default is the global
// provider.
func WithTracerProvider(p trace.TracerProvider) Option { return func(c *config) { c.tracers = p } }

// WithMeterProvider sets where measurements go; the default is the global
// provider.
func WithMeterProvider(p metric.MeterProvider) Option { return func(c *config) { c.meters = p } }

// Middleware returns an eddy middleware that, for every reconcile and
// teardown:
//
//   - starts a span named "eddy.reconcile <kind>" or "eddy.teardown <kind>",
//     with the attributes eddy.kind, eddy.id, eddy.attempt and eddy.outcome,
//     marked as an error unless the outcome is ok or retry;
//   - records its duration in the histogram eddy.reconcile.duration
//     (seconds), with the attributes eddy.kind, eddy.operation and
//     eddy.outcome.
//
// The span's context is passed on, so spans the reconciler starts become its
// children.
func Middleware(opts ...Option) eddy.Middleware {
	cfg := config{tracers: otel.GetTracerProvider(), meters: otel.GetMeterProvider()}
	for _, opt := range opts {
		opt(&cfg)
	}
	tracer := cfg.tracers.Tracer(scope)
	duration, err := cfg.meters.Meter(scope).Float64Histogram("eddy.reconcile.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of eddy reconciles and teardowns."),
	)
	if err != nil {
		// The SDK still returns a usable no-op instrument alongside err.
		otel.Handle(err)
	}
	return func(next eddy.Handler) eddy.Handler {
		return func(ctx context.Context, ref eddy.Ref) error {
			op := "reconcile"
			if ref.Deleted() {
				op = "teardown"
			}
			ctx, span := tracer.Start(ctx, "eddy."+op+" "+ref.Kind(),
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("eddy.kind", ref.Kind()),
					attribute.String("eddy.id", ref.ID()),
					attribute.Int("eddy.attempt", eddy.Attempt(ctx)),
				),
			)
			start := time.Now()
			err := next(ctx, ref)
			outcome := Outcome(err)

			span.SetAttributes(attribute.String("eddy.outcome", outcome))
			if outcome == OutcomeError || outcome == OutcomePermanent {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
			duration.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(
				attribute.String("eddy.kind", ref.Kind()),
				attribute.String("eddy.operation", op),
				attribute.String("eddy.outcome", outcome),
			))
			return err
		}
	}
}

// Outcome classifies what a reconcile returned as one of the Outcome
// constants.
func Outcome(err error) string {
	switch {
	case err == nil:
		return OutcomeOK
	case eddy.IsPermanent(err):
		return OutcomePermanent
	}
	if _, retry := eddy.RetryDelay(err); retry {
		return OutcomeRetry
	}
	return OutcomeError
}
