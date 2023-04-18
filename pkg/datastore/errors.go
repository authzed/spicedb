package datastore

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog"
)

// ErrNotFound is a shared interface for not found errors.
type ErrNotFound interface {
	IsNotFoundError() bool
}

// ErrNamespaceNotFound occurs when a namespace was not found.
type ErrNamespaceNotFound struct {
	error
	namespaceName string
}

var _ ErrNotFound = ErrNamespaceNotFound{}

func (err ErrNamespaceNotFound) IsNotFoundError() bool {
	return true
}

// NotFoundNamespaceName is the name of the namespace not found.
func (err ErrNamespaceNotFound) NotFoundNamespaceName() string {
	return err.namespaceName
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrNamespaceNotFound) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("namespace", err.namespaceName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrNamespaceNotFound) DetailsMetadata() map[string]string {
	return map[string]string{
		"definition_name": err.namespaceName,
	}
}

// ErrWatchDisconnected occurs when a watch has fallen too far behind and was forcibly disconnected
// as a result.
type ErrWatchDisconnected struct{ error }

// ErrWatchCanceled occurs when a watch was canceled by the caller.
type ErrWatchCanceled struct{ error }

// ErrWatchDisabled occurs when watch is disabled by being unsupported by the datastore.
type ErrWatchDisabled struct{ error }

// ErrReadOnly is returned when the operation cannot be completed because the datastore is in
// read-only mode.
type ErrReadOnly struct{ error }

// InvalidRevisionReason is the reason the revision could not be used.
type InvalidRevisionReason int

const (
	// RevisionStale is the reason returned when a revision is outside the window of
	// validity by being too old.
	RevisionStale InvalidRevisionReason = iota

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
func (err ErrInvalidRevision) InvalidRevision() Revision {
	return err.revision
}

// Reason is the reason the revision failed.
func (err ErrInvalidRevision) Reason() InvalidRevisionReason {
	return err.reason
}

// MarshalZerologObject implements zerolog object marshalling.
func (err ErrInvalidRevision) MarshalZerologObject(e *zerolog.Event) {
	switch err.reason {
	case RevisionStale:
		e.Err(err.error).Str("reason", "stale")
	case CouldNotDetermineRevision:
		e.Err(err.error).Str("reason", "indeterminate")
	default:
		e.Err(err.error).Str("reason", "unknown")
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

// NewWatchDisabledErr constructs a new watch is disabled error.
func NewWatchDisabledErr(reason string) error {
	return ErrWatchDisabled{
		error: fmt.Errorf("watch is currently disabled: %s", reason),
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

	default:
		return ErrInvalidRevision{
			error:    fmt.Errorf("revision was invalid"),
			revision: revision,
			reason:   reason,
		}
	}
}

// ErrCaveatNameNotFound is the error returned when a caveat is not found by its name
type ErrCaveatNameNotFound struct {
	error
	name string
}

var _ ErrNotFound = ErrCaveatNameNotFound{}

func (err ErrCaveatNameNotFound) IsNotFoundError() bool {
	return true
}

// CaveatName returns the name of the caveat that couldn't be found
func (err ErrCaveatNameNotFound) CaveatName() string {
	return err.name
}

// NewCaveatNameNotFoundErr constructs a new caveat name not found error.
func NewCaveatNameNotFoundErr(name string) error {
	return ErrCaveatNameNotFound{
		error: fmt.Errorf("caveat with name `%s` not found", name),
		name:  name,
	}
}

// DetailsMetadata returns the metadata for details for this error.
func (err ErrCaveatNameNotFound) DetailsMetadata() map[string]string {
	return map[string]string{
		"caveat_name": err.name,
	}
}

var (
	ErrClosedIterator        = errors.New("unable to iterate: iterator closed")
	ErrCursorsWithoutSorting = errors.New("cursors are disabled on unsorted results")
	ErrCursorEmpty           = errors.New("cursors are only available after the first result")
)
