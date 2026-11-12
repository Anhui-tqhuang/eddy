package eddyotel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Anhui-tqhuang/eddy"
	"github.com/Anhui-tqhuang/eddy/eddyotel"
)

func TestOutcome_classifiesErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, eddyotel.OutcomeOK},
		{"retry", eddy.Retry(time.Second), eddyotel.OutcomeRetry},
		{"plain_error", errors.New("boom"), eddyotel.OutcomeError},
		{"permanent", eddy.Permanent(errors.New("bad row")), eddyotel.OutcomePermanent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := eddyotel.Outcome(tt.err); got != tt.want {
				t.Fatalf("Outcome = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMiddleware_recordsSpanAndDuration(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name       string
		err        error
		outcome    string
		wantStatus codes.Code
	}{
		{"success", nil, eddyotel.OutcomeOK, codes.Unset},
		{"retry_is_not_an_error", eddy.Retry(time.Second), eddyotel.OutcomeRetry, codes.Unset},
		{"failure", boom, eddyotel.OutcomeError, codes.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spans := tracetest.NewSpanRecorder()
			reader := sdkmetric.NewManualReader()
			mw := eddyotel.Middleware(
				eddyotel.WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))),
				eddyotel.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
			)
			var inner trace.SpanContext
			handler := mw(func(ctx context.Context, _ eddy.Ref) error {
				inner = trace.SpanContextFromContext(ctx)
				return tt.err
			})
			if err := handler(context.Background(), eddy.NewKind[int64]("app").Of(7)); !errors.Is(err, tt.err) {
				t.Fatalf("middleware changed the error: %v", err)
			}

			ended := spans.Ended()
			if len(ended) != 1 {
				t.Fatalf("spans = %d, want 1", len(ended))
			}
			span := ended[0]
			if span.Name() != "eddy.reconcile app" || span.Status().Code != tt.wantStatus {
				t.Fatalf("span %q status %v, want eddy.reconcile app / %v", span.Name(), span.Status().Code, tt.wantStatus)
			}
			if !inner.Equal(span.SpanContext()) {
				t.Fatal("reconciler did not run inside the span")
			}
			attrs := attribute.NewSet(span.Attributes()...)
			for key, want := range map[attribute.Key]string{"eddy.kind": "app", "eddy.id": "7", "eddy.outcome": tt.outcome} {
				if v, _ := attrs.Value(key); v.AsString() != want {
					t.Errorf("span attribute %s = %q, want %q", key, v.AsString(), want)
				}
			}

			var rm metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &rm); err != nil {
				t.Fatal(err)
			}
			hist, ok := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[float64])
			if !ok || rm.ScopeMetrics[0].Metrics[0].Name != "eddy.reconcile.duration" || len(hist.DataPoints) != 1 {
				t.Fatalf("metrics = %+v", rm.ScopeMetrics[0].Metrics)
			}
			point := hist.DataPoints[0]
			for key, want := range map[attribute.Key]string{"eddy.kind": "app", "eddy.operation": "reconcile", "eddy.outcome": tt.outcome} {
				if v, _ := point.Attributes.Value(key); v.AsString() != want {
					t.Errorf("metric attribute %s = %q, want %q", key, v.AsString(), want)
				}
			}
			if point.Count != 1 {
				t.Errorf("histogram count = %d, want 1", point.Count)
			}
		})
	}
}

func TestMiddleware_defaults_useGlobalProviders(t *testing.T) {
	handler := eddyotel.Middleware()(func(context.Context, eddy.Ref) error { return nil })
	if err := handler(context.Background(), eddy.NewKind[string]("org").Of("acme")); err != nil {
		t.Fatal(err)
	}
}
