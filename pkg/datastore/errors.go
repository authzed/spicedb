package datastore

import (
	"fmt"

	"github.com/rs/zerolog"
)

// ErrNamespaceNotFound occurs when a namespace was not found.
type ErrNamespaceNotFound struct {
	error
	namespaceName string
}

// NotFoundNamespaceName is the name of the namespace not found.
func (enf ErrNamespaceNotFound) NotFoundNamespaceName() string {
	return enf.namespaceName
}

// MarshalZerologObject implements zerolog object marshalling.
func (enf ErrNamespaceNotFound) MarshalZerologObject(e *zerolog.Event) {
	e.Str("error", enf.Error()).Str("namespace", enf.namespaceName)
}

// ErrWatchDisconnected occurs when a watch has fallen too far behind and was forcibly disconnected
// as a result.
type ErrWatchDisconnected struct{ error }

// ErrWatchCanceled occurs when a watch was canceled by the caller
type ErrWatchCanceled struct{ error }

// ErrReadOnly is returned when the operation cannot be completed because the datastore is in
// read-only mode.
type ErrReadOnly struct{ error }

// InvalidRevisionReason is the reason the revision could not be used.
type InvalidRevisionReason int

const (
	// RevisionStale is the reason returned when a revision is outside the window of
	// validity by being too old.
	RevisionStale InvalidRevisionReason = iota

	// RevisionInFuture is the reason returned when a revision is outside the window of
	// validity by being too new.
	RevisionInFuture

	// CouldNotDetermineRevision is the reason returned when a revision for a
	// request could not be determined.
	CouldNotDetermineRevision
)

// ErrInvalidRevision occurs when a revision specified to a call was invalid.
type ErrInvalidRevision struct {
	error
	revision Revision
	reason   InvalidRevisionReason
}

// InvalidRevision is the revision that failed.
func (eri ErrInvalidRevision) InvalidRevision() Revision {
	return eri.revision
}

// Reason is the reason the revision failed.
func (eri ErrInvalidRevision) Reason() InvalidRevisionReason {
	return eri.reason
}

// MarshalZerologObject implements zerolog object marshalling.
func (eri ErrInvalidRevision) MarshalZerologObject(e *zerolog.Event) {
	switch eri.reason {
	case RevisionStale:
		e.Str("error", eri.Error()).Str("reason", "stale")
	case RevisionInFuture:
		e.Str("error", eri.Error()).Str("reason", "future")
	case CouldNotDetermineRevision:
		e.Str("error", eri.Error()).Str("reason", "indeterminate")
	default:
		e.Str("error", eri.Error()).Str("reason", "unknown")
	}
}

// NewNamespaceNotFoundErr constructs a new namespace not found error.
func NewNamespaceNotFoundErr(nsName string) error {
	return ErrNamespaceNotFound{
		error:         fmt.Errorf("object definition `%s` not found", nsName),
		namespaceName: nsName,
	}
}

// NewWatchDisconnectedErr constructs a new watch was disconnected error.
func NewWatchDisconnectedErr() error {
	return ErrWatchDisconnected{
		error: fmt.Errorf("watch fell too far behind and was disconnected"),
	}
}

// NewWatchCanceledErr constructs a new watch was canceled error.
func NewWatchCanceledErr() error {
	return ErrWatchCanceled{
		error: fmt.Errorf("watch was canceled by the caller"),
	}
}

// NewReadonlyErr constructs an error for when a request has failed because
// the datastore has been configured to be read-only.
func NewReadonlyErr() error {
	return ErrReadOnly{
		error: fmt.Errorf("datastore is in read-only mode"),
	}
}

// NewInvalidRevisionErr constructs a new invalid revision error.
func NewInvalidRevisionErr(revision Revision, reason InvalidRevisionReason) error {
	switch reason {
	case RevisionStale:
		return ErrInvalidRevision{
			error:    fmt.Errorf("revision has expired"),
			revision: revision,
			reason:   reason,
		}

	case RevisionInFuture:
		return ErrInvalidRevision{
			error:    fmt.Errorf("revision is for a future time"),
			revision: revision,
			reason:   reason,
		}

	default:
		return ErrInvalidRevision{
			error:    fmt.Errorf("revision was invalid"),
			revision: revision,
			reason:   reason,
		}
	}
}
