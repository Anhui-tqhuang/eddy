// Package eddyprom instruments eddy reconciles with Prometheus metrics.
//
//	m, err := eddyprom.New(prometheus.DefaultRegisterer)
//	...
//	db.Run(ctx,
//		eddy.Reconcile(Apps, reconcileApp),
//		eddy.WithMiddleware(m.Middleware()),
//		eddy.OnLeadershipChange(m.SetLeading),
//	)
//
// It lives in its own module so that eddy itself does not depend on the
// Prometheus client.
package eddyprom

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Anhui-tqhuang/eddy"
)

// Outcome values of the "outcome" label.
const (
	OutcomeOK        = "ok"        // reconciled
	OutcomeRetry     = "retry"     // asked to be checked again later (eddy.Retry)
	OutcomeError     = "error"     // failed; will be retried
	OutcomePermanent = "permanent" // failed for good (eddy.Permanent)
)

// Metrics are eddy's Prometheus collectors:
//
//   - eddy_reconcile_duration_seconds{kind,operation,outcome}: histogram of
//     how long reconciles and teardowns took
//   - eddy_reconciles_in_flight{kind}: gauge of reconciles running now
//   - eddy_leader: gauge, 1 while this replica leads
//
// operation is "reconcile" or "teardown"; outcome is one of the Outcome
// constants.
type Metrics struct {
	duration *prometheus.HistogramVec
	inFlight *prometheus.GaugeVec
	leader   prometheus.Gauge
}

// Option configures New.
type Option func(*options)

type options struct {
	buckets []float64
}

// WithBuckets sets the duration histogram's buckets, in seconds; the default
// is prometheus.DefBuckets.
func WithBuckets(buckets ...float64) Option { return func(o *options) { o.buckets = buckets } }

// New creates the metrics and registers them with reg. It fails when reg
// already has metrics of the same names.
func New(reg prometheus.Registerer, opts ...Option) (*Metrics, error) {
	if reg == nil {
		return nil, errors.New("eddyprom: nil Registerer")
	}
	o := options{buckets: prometheus.DefBuckets}
	for _, opt := range opts {
		opt(&o)
	}
	m := &Metrics{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "eddy_reconcile_duration_seconds",
			Help:    "Duration of eddy reconciles and teardowns.",
			Buckets: o.buckets,
		}, []string{"kind", "operation", "outcome"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "eddy_reconciles_in_flight",
			Help: "Reconciles and teardowns running now.",
		}, []string{"kind"}),
		leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "eddy_leader",
			Help: "1 while this replica is the eddy leader, else 0.",
		}),
	}
	for _, c := range []prometheus.Collector{m.duration, m.inFlight, m.leader} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Middleware returns the eddy middleware that feeds the duration histogram
// and the in-flight gauge.
func (m *Metrics) Middleware() eddy.Middleware {
	return func(next eddy.Handler) eddy.Handler {
		return func(ctx context.Context, ref eddy.Ref) error {
			op := "reconcile"
			if ref.Deleted() {
				op = "teardown"
			}
			inFlight := m.inFlight.WithLabelValues(ref.Kind())
			inFlight.Inc()
			start := time.Now()
			err := next(ctx, ref)
			inFlight.Dec()
			m.duration.WithLabelValues(ref.Kind(), op, Outcome(err)).Observe(time.Since(start).Seconds())
			return err
		}
	}
}

// SetLeading feeds the leader gauge; pass it to eddy.OnLeadershipChange.
func (m *Metrics) SetLeading(leading bool) {
	if leading {
		m.leader.Set(1)
		return
	}
	m.leader.Set(0)
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
