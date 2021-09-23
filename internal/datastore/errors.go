package datastore

import (
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
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

func (enf ErrNamespaceNotFound) MarshalZerologObject(e *zerolog.Event) {
	e.Str("error", enf.Error()).Str("namespace", enf.namespaceName)
}

// ErrPreconditionFailed occurs when the precondition to a write tuple call does not match.
type ErrPreconditionFailed struct {
	error
	precondition *v1.Precondition
}

func (epf ErrPreconditionFailed) MarshalZerologObject(e *zerolog.Event) {
	e.Str("error", epf.Error()).Interface("precondition", epf.precondition)
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
	RevisionStale InvalidRevisionReason = iota
	RevisionInFuture
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
		error:         fmt.Errorf("namespace `%s` not found", nsName),
		namespaceName: nsName,
	}
}

// NewPreconditionFailedErr constructs a new precondition failed error.
func NewPreconditionFailedErr(precondition *v1.Precondition) error {
	return ErrPreconditionFailed{
		error:        fmt.Errorf("unable to satisfy write precondition `%s`", precondition),
		precondition: precondition,
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
