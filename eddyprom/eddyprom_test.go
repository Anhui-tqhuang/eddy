package eddyprom_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Anhui-tqhuang/eddy"
	"github.com/Anhui-tqhuang/eddy/eddyprom"
)

func TestOutcome_classifiesErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, eddyprom.OutcomeOK},
		{"retry", eddy.Retry(time.Second), eddyprom.OutcomeRetry},
		{"plain_error", errors.New("boom"), eddyprom.OutcomeError},
		{"permanent", eddy.Permanent(errors.New("bad row")), eddyprom.OutcomePermanent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := eddyprom.Outcome(tt.err); got != tt.want {
				t.Fatalf("Outcome = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMetrics_middlewareAndLeader(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := eddyprom.New(reg, eddyprom.WithBuckets(0.1, 1, 10))
	if err != nil {
		t.Fatal(err)
	}
	gauge := func(name string) float64 {
		t.Helper()
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range families {
			if f.GetName() == name {
				return f.GetMetric()[0].GetGauge().GetValue()
			}
		}
		t.Fatalf("metric %s not found", name)
		return 0
	}

	boom := errors.New("boom")
	var during float64
	results := []error{nil, boom, eddy.Retry(time.Second)}
	apps := eddy.NewKind[int64]("app")
	for i, want := range results {
		handler := m.Middleware()(func(context.Context, eddy.Ref) error {
			during = gauge("eddy_reconciles_in_flight")
			return want
		})
		if err := handler(context.Background(), apps.Of(int64(i))); !errors.Is(err, want) {
			t.Fatalf("middleware changed the error: %v", err)
		}
	}
	if during != 1 || gauge("eddy_reconciles_in_flight") != 0 {
		t.Fatalf("in flight during = %v, after = %v; want 1 and 0", during, gauge("eddy_reconciles_in_flight"))
	}
	// One series per outcome seen.
	if n := testutil.CollectAndCount(reg, "eddy_reconcile_duration_seconds"); n != 3 {
		t.Fatalf("duration series = %d, want 3", n)
	}

	m.SetLeading(true)
	if gauge("eddy_leader") != 1 {
		t.Fatal("eddy_leader != 1 while leading")
	}
	m.SetLeading(false)
	if gauge("eddy_leader") != 0 {
		t.Fatal("eddy_leader != 0 after losing leadership")
	}
}

func TestNew_misuse_fails(t *testing.T) {
	if _, err := eddyprom.New(nil); err == nil {
		t.Fatal("nil Registerer: want error")
	}
	reg := prometheus.NewRegistry()
	if _, err := eddyprom.New(reg); err != nil {
		t.Fatal(err)
	}
	if _, err := eddyprom.New(reg); err == nil {
		t.Fatal("registering twice: want error")
	}
}
