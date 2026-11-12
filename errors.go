package eddy

import (
	"errors"
	"time"
)

// ErrNotFound is returned when the row a Table call names does not exist.
var ErrNotFound = errors.New("eddy: row not found")

// ErrUnknownKind is returned (and retried) for a job whose kind has no
// reconciler, so jobs enqueued ahead of a rollout are not lost.
var ErrUnknownKind = errors.New("eddy: no reconciler registered for kind")

// Retry returns an error that reconciles the object again after d without
// counting as a failure: use it to poll something that is still converging.
//
//	if !ready {
//		return eddy.Retry(15 * time.Second)
//	}
func Retry(after time.Duration) error {
	return &retryError{after: max(after, 0)}
}

// RetryDelay reports whether err asks for a Retry, and after how long.
// Middleware uses it to tell a retry from a failure.
func RetryDelay(err error) (time.Duration, bool) {
	var r *retryError
	if errors.As(err, &r) {
		return r.after, true
	}
	return 0, false
}

type retryError struct{ after time.Duration }

func (e *retryError) Error() string { return "retry after " + e.after.String() }

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
