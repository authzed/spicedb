package v1

import (
	"context"
	"errors"
	"fmt"

	log "github.com/authzed/spicedb/internal/logging"

	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

// ErrPartitioningNotSupported is returned by PlanPartitionedExport when the
// underlying datastore does not implement BulkExportPartitioner.
var ErrPartitioningNotSupported = errors.New("datastore does not implement BulkExportPartitioner")

// ExportPartition represents a non-overlapping portion of the relationship table
// that can be streamed independently.
type ExportPartition struct {
	Index      uint32
	LowerBound dsoptions.Cursor // nil = start of table (exclusive)
	UpperBound dsoptions.Cursor // nil = end of table (inclusive)
}

// ExportPlan contains the partitions for a partitioned export.
type ExportPlan struct {
	Partitions []ExportPartition
}

// StreamRequest contains the parameters for streaming a single partition.
type StreamRequest struct {
	Partition ExportPartition
	Revision  datastore.Revision
}

// PlanPartitionedExport plans a partitioned export by splitting the relationship
// table into non-overlapping key ranges for parallel streaming.
func PlanPartitionedExport(ctx context.Context, ds datastore.Datastore, desiredPartitions uint32) (*ExportPlan, error) {
	if desiredPartitions == 0 {
		desiredPartitions = 1
	}

	partitioner := datastore.UnwrapAs[datastore.BulkExportPartitioner](ds)
	if partitioner == nil {
		return nil, ErrPartitioningNotSupported
	}

	ranges, err := partitioner.PlanPartitions(ctx, desiredPartitions)
	if err != nil {
		return nil, fmt.Errorf("failed to plan partitions: %w", err)
	}

	log.Ctx(ctx).Info().
		Uint32("desired_partitions", desiredPartitions).
		Int("actual_partitions", len(ranges)).
		Msg("planned partitioned export")

	partitions := make([]ExportPartition, len(ranges))
	for i, r := range ranges {
		partitions[i] = ExportPartition{
			Index:      uint32(i),
			LowerBound: r.LowerBound,
			UpperBound: r.UpperBound,
		}
	}

	return &ExportPlan{
		Partitions: partitions,
	}, nil
}

// StreamPartitionedExport returns an iterator over relationships in a single
// partition. Use PlanPartitionedExport first to obtain partitions.
//
// The returned iterator streams rows lazily from the datastore. The caller
// is responsible for iterating and handling errors:
//
//	iter, err := v1.StreamPartitionedExport(ctx, ds, req)
//	if err != nil { ... }
//	for rel, err := range iter {
//	    if err != nil { ... }
//	    // process rel
//	}
func StreamPartitionedExport(ctx context.Context, ds datastore.ReadOnlyDatastore, req StreamRequest) (datastore.RelationshipIterator, error) {
	if req.Revision == nil {
		return nil, errors.New("revision is required")
	}

	if err := ds.CheckRevision(ctx, req.Revision); err != nil {
		return nil, fmt.Errorf("revision expired: %w", err)
	}

	reader := ds.SnapshotReader(req.Revision)

	opts := []dsoptions.QueryOptionsOption{
		dsoptions.WithSort(dsoptions.ByResource),
		dsoptions.WithQueryShape(queryshape.Varying),
		// Force tuple comparison syntax (e.g., (a,b,c) > (1,2,3)) instead of
		// the default ExpandedLogicComparison for CRDB. The expanded form with
		// both After and BeforeOrEqual bounds generates OR clauses that CRDB
		// wraps in a union-all + distinct plan, which buffers all rows for
		// deduplication and exhausts temp disk storage on large exports.
		// Tuple comparison produces a clean index scan with a bounded span
		// and no buffering. See TestExplainPartitionedQuery for verification.
		dsoptions.WithUseTupleComparison(true),
	}

	// Lower bound from partition.
	if req.Partition.LowerBound != nil {
		opts = append(opts, dsoptions.WithAfter(req.Partition.LowerBound))
	}

	// Upper bound from partition.
	if req.Partition.UpperBound != nil {
		opts = append(opts, dsoptions.WithBeforeOrEqual(req.Partition.UpperBound))
	}

	return reader.QueryRelationships(ctx, datastore.RelationshipsFilter{}, opts...)
}
