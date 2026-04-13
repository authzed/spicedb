package v1

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPlanPartitionedExport(t *testing.T) {
	t.Run("returns single partition for non-partitioner datastore", func(t *testing.T) {
		ds := newTestDatastore(t)
		plan, err := PlanPartitionedExport(context.Background(), ds, 4)
		require.NoError(t, err)
		require.Len(t, plan.Partitions, 1)
		require.Nil(t, plan.Partitions[0].LowerBound)
		require.Nil(t, plan.Partitions[0].UpperBound)
		require.NotNil(t, plan.Revision)
	})

	t.Run("desiredCount=0 defaults to 1", func(t *testing.T) {
		ds := newTestDatastore(t)
		plan, err := PlanPartitionedExport(context.Background(), ds, 0)
		require.NoError(t, err)
		require.Len(t, plan.Partitions, 1)
	})

	t.Run("revision is valid and usable", func(t *testing.T) {
		ds := newTestDatastore(t)
		writeTestRelationships(t, ds, 10)
		plan, err := PlanPartitionedExport(context.Background(), ds, 1)
		require.NoError(t, err)
		require.NoError(t, ds.CheckRevision(context.Background(), plan.Revision))
	})
}

func TestStreamPartitionedExport(t *testing.T) {
	t.Run("streams all relationships", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 50)

		var batches []ExportBatch
		err := StreamPartitionedExport(context.Background(), ds, StreamRequest{
			Revision:  rev,
			BatchSize: 100,
		}, func(batch ExportBatch) error {
			batches = append(batches, batch)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Len(t, batches[0].Relationships, 50)
	})

	t.Run("batches correctly with small batch size", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 50)

		var batches []ExportBatch
		err := StreamPartitionedExport(context.Background(), ds, StreamRequest{
			Revision:  rev,
			BatchSize: 10,
		}, func(batch ExportBatch) error {
			batches = append(batches, batch)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, batches, 5)
		for _, b := range batches {
			require.Len(t, b.Relationships, 10)
			require.NotEmpty(t, b.Cursor)
		}
	})

	t.Run("batch_size=0 uses default", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 10)

		var batches []ExportBatch
		err := StreamPartitionedExport(context.Background(), ds, StreamRequest{
			Revision:  rev,
			BatchSize: 0,
		}, func(batch ExportBatch) error {
			batches = append(batches, batch)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Len(t, batches[0].Relationships, 10)
	})

	t.Run("batch_size=1 sends one per batch", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 5)

		var batches []ExportBatch
		err := StreamPartitionedExport(context.Background(), ds, StreamRequest{
			Revision:  rev,
			BatchSize: 1,
		}, func(batch ExportBatch) error {
			batches = append(batches, batch)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, batches, 5)
	})

	t.Run("batch larger than total rows", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 5)

		var batches []ExportBatch
		err := StreamPartitionedExport(context.Background(), ds, StreamRequest{
			Revision:  rev,
			BatchSize: 1000,
		}, func(batch ExportBatch) error {
			batches = append(batches, batch)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, batches, 1)
		require.Len(t, batches[0].Relationships, 5)
	})

	t.Run("empty table produces no batches", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev, err := ds.HeadRevision(context.Background())
		require.NoError(t, err)

		var batches []ExportBatch
		err = StreamPartitionedExport(context.Background(), ds, StreamRequest{
			Revision:  rev,
			BatchSize: 100,
		}, func(batch ExportBatch) error {
			batches = append(batches, batch)
			return nil
		})
		require.NoError(t, err)
		require.Empty(t, batches)
	})

	t.Run("cursor resumability", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 30)

		// Stream and capture the first batch cursor.
		var firstCursor string
		var firstBatchCount int
		err := StreamPartitionedExport(context.Background(), ds, StreamRequest{
			Revision:  rev,
			BatchSize: 10,
		}, func(batch ExportBatch) error {
			if firstCursor == "" {
				firstCursor = batch.Cursor
				firstBatchCount = len(batch.Relationships)
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 10, firstBatchCount)

		// Resume from cursor.
		var resumedCount int
		err = StreamPartitionedExport(context.Background(), ds, StreamRequest{
			Revision:  rev,
			BatchSize: 100,
			Cursor:    firstCursor,
		}, func(batch ExportBatch) error {
			resumedCount += len(batch.Relationships)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 20, resumedCount)
	})

	t.Run("nil revision returns error", func(t *testing.T) {
		ds := newTestDatastore(t)
		err := StreamPartitionedExport(context.Background(), ds, StreamRequest{
			BatchSize: 100,
		}, func(batch ExportBatch) error {
			return nil
		})
		require.Error(t, err)
	})

	t.Run("invalid cursor returns error", func(t *testing.T) {
		ds := newTestDatastore(t)
		rev := writeTestRelationships(t, ds, 10)

		err := StreamPartitionedExport(context.Background(), ds, StreamRequest{
			Revision:  rev,
			BatchSize: 100,
			Cursor:    "not-valid-base64!!!",
		}, func(batch ExportBatch) error {
			return nil
		})
		require.Error(t, err)
	})
}

func newTestDatastore(t *testing.T) datastore.Datastore {
	t.Helper()
	ds, err := memdb.NewMemdbDatastore(0, 0, 1*time.Hour)
	require.NoError(t, err)
	return ds
}

func writeTestRelationships(t *testing.T, ds datastore.Datastore, count int) datastore.Revision {
	t.Helper()
	ctx := context.Background()
	rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		var updates []tuple.RelationshipUpdate
		for i := 0; i < count; i++ {
			updates = append(updates, tuple.Create(
				tuple.MustParse(fmt.Sprintf("resource:%d#viewer@user:%d", i, i)),
			))
		}
		return rwt.WriteRelationships(ctx, updates)
	})
	require.NoError(t, err)
	return rev
}
