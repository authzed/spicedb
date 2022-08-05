package shared

import (
	"context"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
)

var limitOne uint64 = 1

// CheckPreconditions checks whether the preconditions are met in the context of a datastore
// read-write transaction, and returns an error if they are not met.
func CheckPreconditions(
	ctx context.Context,
	rwt datastore.ReadWriteTransaction,
	preconditions []*v1.Precondition,
) error {
	for _, precond := range preconditions {
		iter, err := rwt.QueryRelationships(ctx, datastore.RelationshipsFilterFromPublicFilter(precond.Filter), options.WithLimit(&limitOne))
		if err != nil {
			return fmt.Errorf("error reading relationships: %w", err)
		}
		defer iter.Close()

		first := iter.Next()

		if first == nil && iter.Err() != nil {
			return fmt.Errorf("error reading relationships from iterator: %w", err)
		}

		switch precond.Operation {
		case v1.Precondition_OPERATION_MUST_NOT_MATCH:
			if first != nil {
				return NewPreconditionFailedErr(precond)
			}
		case v1.Precondition_OPERATION_MUST_MATCH:
			if first == nil {
				return NewPreconditionFailedErr(precond)
			}
		default:
			return fmt.Errorf("unspecified precondition operation: %s", precond.Operation)
		}
	}

	return nil
}

// ErrPreconditionFailed occurs when the precondition to a write tuple call does not match.
type ErrPreconditionFailed struct {
	error
	precondition *v1.Precondition
}

// MarshalZerologObject implements zerolog object marshalling.
func (epf ErrPreconditionFailed) MarshalZerologObject(e *zerolog.Event) {
	e.Str("error", epf.Error()).Interface("precondition", epf.precondition)
}

// NewPreconditionFailedErr constructs a new precondition failed error.
func NewPreconditionFailedErr(precondition *v1.Precondition) error {
	return ErrPreconditionFailed{
		error:        fmt.Errorf("unable to satisfy write precondition `%s`", precondition),
		precondition: precondition,
	}
}
