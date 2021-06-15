package datastore

import (
	"fmt"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ErrNamespaceNotFound occurs when a namespace was not found.
type ErrNamespaceNotFound struct {
	error
	namespaceName string
}

func (enf ErrNamespaceNotFound) NotFoundNamespaceName() string {
	return enf.namespaceName
}

// ErrPreconditionFailed occurs when the precondition to a write tuple call does not match.
type ErrPreconditionFailed struct {
	error
	precondition *pb.RelationTuple
}

// FailedPrecondition is the tuple that was not found but was required as a precondition of a write.
func (epf *ErrPreconditionFailed) FailedPrecondition() *pb.RelationTuple {
	return epf.precondition
}

// ErrWatchDisconnected occurs when a watch has fallen too far behind and was forcibly disconnected
// as a result.
type ErrWatchDisconnected struct {
	error
}

// ErrWatchCanceled occurs when a watch was canceled by the caller
type ErrWatchCanceled struct {
	error
}

type InvalidRevisionReason int

const (
	RevisionStale InvalidRevisionReason = iota
	RevisionInFuture
	CouldNotDetermineRevision
)

// ErrInvalidRevision occurs when a revision specified to a call was invalid
type ErrInvalidRevision struct {
	error
	revision Revision
	reason   InvalidRevisionReason
}

func (eri ErrInvalidRevision) InvalidRevision() Revision {
	return eri.revision
}

func (eri ErrInvalidRevision) Reason() InvalidRevisionReason {
	return eri.reason
}

func NewNamespaceNotFoundErr(nsName string) error {
	return ErrNamespaceNotFound{
		error:         fmt.Errorf("namespace `%s` not found", nsName),
		namespaceName: nsName,
	}
}

func NewPreconditionFailedErr(precondition *pb.RelationTuple) error {
	return ErrPreconditionFailed{
		error:        fmt.Errorf("unable to satisfy write precondition `%s`", tuple.String(precondition)),
		precondition: precondition,
	}
}

func NewWatchDisconnectedErr() error {
	return ErrWatchDisconnected{
		error: fmt.Errorf("watch fell too far behind and was disconnected"),
	}
}

func NewWatchCanceledErr() error {
	return ErrWatchCanceled{
		error: fmt.Errorf("watch was canceled by the caller"),
	}
}

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
