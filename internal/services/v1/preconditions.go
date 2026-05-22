package v1

import (
	"context"
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

var limitOne uint64 = 1

// checkPreconditions checks whether the preconditions are met in the context of a datastore
// read-write transaction, and returns an error if they are not met.
func checkPreconditions(
	ctx context.Context,
	rwt datalayer.ReadWriteTransaction,
	preconditions []*v1.Precondition,
) error {
	for _, precond := range preconditions {
		dsFilter, err := datastore.RelationshipsFilterFromPublicFilter(precond.Filter)
		if err != nil {
			return fmt.Errorf("error converting filter: %w", err)
		}

		iter, err := rwt.QueryRelationships(ctx, dsFilter, options.WithLimit(&limitOne), options.WithQueryShape(queryshape.Varying))
		if err != nil {
			return fmt.Errorf("error reading relationships: %w", err)
		}

		_, ok, err := datastore.FirstRelationshipIn(iter)
		if err != nil {
			return fmt.Errorf("error reading relationships from iterator: %w", err)
		}

		switch precond.Operation {
		case v1.Precondition_OPERATION_MUST_NOT_MATCH:
			if ok {
				return NewPreconditionFailedErr(precond)
			}
		case v1.Precondition_OPERATION_MUST_MATCH:
			if !ok {
				return NewPreconditionFailedErr(precond)
			}
		default:
			return fmt.Errorf("unspecified precondition operation: %s", precond.Operation)
		}
	}

	return nil
}
