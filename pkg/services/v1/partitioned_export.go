package v1

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/rs/zerolog/log"
)

const defaultBatchSize = 1000

// ExportPartition represents a non-overlapping portion of the relationship table
// that can be streamed independently.
type ExportPartition struct {
	Index      uint32
	LowerBound dsoptions.Cursor // nil = start of table (exclusive)
	UpperBound dsoptions.Cursor // nil = end of table (inclusive)
}

// ExportPlan contains the partitions and revision for a partitioned export.
type ExportPlan struct {
	Revision   datastore.Revision
	Partitions []ExportPartition
}

// StreamRequest contains the parameters for streaming a single partition.
type StreamRequest struct {
	Partition ExportPartition
	Revision  datastore.Revision
	Cursor    string // opaque cursor for resumability; empty to start from partition lower bound
	BatchSize uint32
}

// ExportBatch is a batch of relationships returned by StreamPartitionedExport.
type ExportBatch struct {
	Cursor        string // opaque cursor for resumability
	Relationships []tuple.Relationship
}

// PlanPartitionedExport plans a partitioned export by splitting the relationship
// table into non-overlapping key ranges for parallel streaming. The returned
// ExportPlan contains partitions and a frozen revision that should be passed to
// StreamPartitionedExport.
func PlanPartitionedExport(ctx context.Context, ds datastore.Datastore, desiredPartitions uint32) (*ExportPlan, error) {
	if desiredPartitions == 0 {
		desiredPartitions = 1
	}

	revision, err := ds.OptimizedRevision(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get revision: %w", err)
	}

	partitioner := datastore.UnwrapAs[datastore.BulkExportPartitioner](ds)
	if partitioner == nil {
		log.Warn().Msg("datastore does not implement BulkExportPartitioner, returning single partition")
		return &ExportPlan{
			Revision:   revision,
			Partitions: []ExportPartition{{Index: 0}},
		}, nil
	}

	ranges, err := partitioner.PlanPartitions(ctx, revision, desiredPartitions)
	if err != nil {
		return nil, fmt.Errorf("failed to plan partitions: %w", err)
	}

	log.Info().
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
		Revision:   revision,
		Partitions: partitions,
	}, nil
}

// StreamPartitionedExport streams relationships for a single partition. The sender
// callback is invoked for each batch. Use PlanPartitionedExport first to obtain
// partitions and a revision.
func StreamPartitionedExport(ctx context.Context, ds datastore.ReadOnlyDatastore, req StreamRequest, sender func(batch ExportBatch) error) error {
	if req.Revision == nil {
		return fmt.Errorf("revision is required")
	}

	if err := ds.CheckRevision(ctx, req.Revision); err != nil {
		return fmt.Errorf("revision expired: %w", err)
	}

	reader := ds.SnapshotReader(req.Revision)

	batchSize := uint64(req.BatchSize)
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}

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

	// Determine lower bound: resume cursor takes priority over partition lower bound.
	if req.Cursor != "" {
		cur, err := decodeCursor(req.Cursor)
		if err != nil {
			return fmt.Errorf("invalid cursor: %w", err)
		}
		opts = append(opts, dsoptions.WithAfter(cur))
	} else if req.Partition.LowerBound != nil {
		opts = append(opts, dsoptions.WithAfter(req.Partition.LowerBound))
	}

	// Upper bound from partition.
	if req.Partition.UpperBound != nil {
		opts = append(opts, dsoptions.WithBeforeOrEqual(req.Partition.UpperBound))
	}

	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{}, opts...)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}

	batch := make([]tuple.Relationship, 0, batchSize)
	var last tuple.Relationship

	for rel, err := range iter {
		if err != nil {
			return fmt.Errorf("iteration error: %w", err)
		}

		batch = append(batch, rel)
		last = rel

		if uint64(len(batch)) == batchSize {
			if err := sender(ExportBatch{
				Cursor:        encodeCursor(last),
				Relationships: batch,
			}); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Send remaining partial batch.
	if len(batch) > 0 {
		if err := sender(ExportBatch{
			Cursor:        encodeCursor(last),
			Relationships: batch,
		}); err != nil {
			return err
		}
	}

	return nil
}

func encodeCursor(rel tuple.Relationship) string {
	return base64.StdEncoding.EncodeToString([]byte(tuple.MustString(rel)))
}

func decodeCursor(encoded string) (dsoptions.Cursor, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}

	rel, err := tuple.Parse(string(data))
	if err != nil {
		return nil, fmt.Errorf("invalid cursor content: %w", err)
	}

	return dsoptions.ToCursor(rel), nil
}
