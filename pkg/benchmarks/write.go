package benchmarks

import (
	"context"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	"github.com/authzed/spicedb/pkg/tuple"
)

const writeBatchSize = 500

// writeRelationships writes relationships to the datastore in batches of 500,
// matching the batch size used by the validation file loader.
func writeRelationships(ctx context.Context, ds datastore.Datastore, rels []tuple.Relationship) (datastore.Revision, error) {
	var revision datastore.Revision
	var err error
	slicez.ForEachChunk(rels, writeBatchSize, func(chunk []tuple.Relationship) {
		if err != nil {
			return
		}
		revision, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, chunk...)
	})
	return revision, err
}
