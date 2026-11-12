package eddy

import (
	"context"
	"errors"
	"time"
)

// Reconciler brings one object's real-world state in line with its database
// row. The row is the source of truth: Reconcile reloads it on every call,
// so it must be idempotent and must treat a missing or soft-deleted row as
// "tear everything down".
//
// Returning an error retries the object with backoff. Wrap the error with
// Permanent to stop retrying. eddy never runs two Reconcile calls for the
// same object at the same time.
type Reconciler[ID any] interface {
	Reconcile(ctx context.Context, id ID) (Result, error)
}

// ReconcilerFunc adapts a plain function to a Reconciler.
type ReconcilerFunc[ID any] func(ctx context.Context, id ID) (Result, error)

// Reconcile calls f.
func (f ReconcilerFunc[ID]) Reconcile(ctx context.Context, id ID) (Result, error) {
	return f(ctx, id)
}

// Result tells eddy what to do after a successful reconcile. The zero value
// means "done until something enqueues this object again".
type Result struct {
	// RequeueAfter, when positive, reconciles the object again after this
	// long, for example to poll an external system that is still converging.
	RequeueAfter time.Duration
}

// Done is the zero Result.
var Done = Result{}

// RequeueAfter returns a Result that reconciles the object again after d.
func RequeueAfter(d time.Duration) Result { return Result{RequeueAfter: d} }

// Permanent marks err as not worth retrying: the job is cancelled instead of
// retried. Use it when the row itself is invalid and only a new write can fix
// it. Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }
