package eddy_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Anhui-tqhuang/eddy"
)

func TestKind_Of_formatsRef(t *testing.T) {
	tests := []struct {
		name string
		ref  eddy.Ref
		want string
	}{
		{"int_id", eddy.NewKind[int64]("team").Of(42), "team/42"},
		{"string_id", eddy.NewKind[string]("org").Of("acme"), `org/"acme"`},
		{"uuid_id", eddy.NewKind[uuid.UUID]("app").Of(uuid.MustParse("6f1c1e2a-0000-4000-8000-000000000001")), `app/"6f1c1e2a-0000-4000-8000-000000000001"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ref.String(); got != tt.want {
				t.Fatalf("String() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestNewKind_emptyName_panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	eddy.NewKind[int64]("")
}

func TestPermanent_wrapsAndDetects(t *testing.T) {
	base := errors.New("bad row")
	err := eddy.Permanent(base)
	if !eddy.IsPermanent(err) || !errors.Is(err, base) {
		t.Fatalf("Permanent(%v) = %v: want permanent wrapping base", base, err)
	}
	if eddy.IsPermanent(base) {
		t.Fatal("plain error reported permanent")
	}
	if eddy.Permanent(nil) != nil {
		t.Fatal("Permanent(nil) != nil")
	}
}

func TestNew_invalidOptions_fail(t *testing.T) {
	apps := eddy.NewKind[int64]("app")
	noop := func(context.Context, int64) (eddy.Result, error) { return eddy.Done, nil }
	list := func(context.Context) ([]int64, error) { return nil, nil }
	tests := []struct {
		name string
		opts []eddy.Option
	}{
		{"no_handlers", nil},
		{"duplicate_kind", []eddy.Option{eddy.HandleFunc(apps, noop), eddy.HandleFunc(apps, noop)}},
		{"resync_without_handler", []eddy.Option{eddy.HandleFunc(eddy.NewKind[int64]("team"), noop), eddy.Resync(apps, time.Minute, list)}},
		{"zero_resync_interval", []eddy.Option{eddy.HandleFunc(apps, noop), eddy.Resync(apps, 0, list)}},
		{"zero_workers", []eddy.Option{eddy.HandleFunc(apps, noop), eddy.WithWorkers(0)}},
		{"empty_queue", []eddy.Option{eddy.HandleFunc(apps, noop), eddy.WithQueue("")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := eddy.New(context.Background(), nil, tt.opts...); err == nil {
				t.Fatal("want error")
			}
		})
	}
}
