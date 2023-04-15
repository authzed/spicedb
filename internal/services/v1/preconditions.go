package v1

import (
	"context"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

var limitOne uint64 = 1

// checkPreconditions checks whether the preconditions are met in the context of a datastore
// read-write transaction, and returns an error if they are not met.
func checkPreconditions(
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
		iter.Close()

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
