//go:build ci && docker

package crdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	v1 "github.com/authzed/spicedb/pkg/services/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPartitionedExportEndToEnd(t *testing.T) {
	b := testdatastore.RunCRDBClusterForTesting(t, 3, crdbTestVersion())

	t.Run("Plan + Stream covers all relationships", func(t *testing.T) {
		ctx := t.Context()

		var connectStr string
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			connectStr = uri
			ds, err := NewCRDBDatastore(ctx, uri,
				GCWindow(veryLargeGCWindow),
				RevisionQuantization(0),
				WithAcquireTimeout(30*time.Second),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ds.Close() })
			return ds
		})

		// Write relationships across multiple namespaces.
		totalWritten := 0
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			var updates []tuple.RelationshipUpdate
			for ns := 0; ns < 10; ns++ {
				for i := 0; i < 100; i++ {
					updates = append(updates, tuple.Create(tuple.MustParse(
						fmt.Sprintf("resource%d:%d#viewer@user:%d", ns, i, i),
					)))
					totalWritten++
				}
			}
			return rwt.WriteRelationships(ctx, updates)
		})
		require.NoError(t, err)

		// Force range splits so we get real multi-partition plans.
		conn, err := pgx.Connect(ctx, connectStr)
		require.NoError(t, err)
		defer conn.Close(ctx)

		for _, ns := range []string{"resource3", "resource5", "resource7"} {
			_, err := conn.Exec(ctx, fmt.Sprintf(
				`ALTER TABLE relation_tuple SPLIT AT VALUES ('%s', '0', 'viewer', 'user', '0', '...')`, ns,
			))
			require.NoError(t, err)
		}

		// Phase 1: Plan.
		plan, err := v1.PlanPartitionedExport(ctx, ds, 4)
		require.NoError(t, err)
		require.Greater(t, len(plan.Partitions), 1, "expected multiple partitions from forced splits")
		require.NotNil(t, plan.Revision)

		t.Logf("Plan returned %d partitions at revision %s", len(plan.Partitions), plan.Revision)

		// Phase 2: Stream each partition and collect all relationships.
		seen := make(map[string]int) // relationship key → partition index
		for pi, partition := range plan.Partitions {
			partitionCount := 0
			err := v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
				Partition: partition,
				Revision:  plan.Revision,
				BatchSize: 1000,
			}, func(batch v1.ExportBatch) error {
				require.NotEmpty(t, batch.Cursor, "batch should have a cursor")
				for _, rel := range batch.Relationships {
					key := tuple.MustString(rel)
					prevPartition, duplicate := seen[key]
					require.False(t, duplicate,
						"relationship %s appeared in both partition %d and %d", key, prevPartition, pi)
					seen[key] = pi
					partitionCount++
				}
				return nil
			})
			require.NoError(t, err)
			t.Logf("Partition %d: %d relationships", pi, partitionCount)
		}

		require.Len(t, seen, totalWritten,
			"all relationships should be covered across partitions (no gaps)")
	})

	t.Run("cursor resumability", func(t *testing.T) {
		ctx := t.Context()

		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewCRDBDatastore(ctx, uri,
				GCWindow(veryLargeGCWindow),
				RevisionQuantization(0),
				WithAcquireTimeout(30*time.Second),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ds.Close() })
			return ds
		})

		// Write relationships.
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			var updates []tuple.RelationshipUpdate
			for i := 0; i < 50; i++ {
				updates = append(updates, tuple.Create(tuple.MustParse(
					fmt.Sprintf("resource:%d#viewer@user:%d", i, i),
				)))
			}
			return rwt.WriteRelationships(ctx, updates)
		})
		require.NoError(t, err)

		plan, err := v1.PlanPartitionedExport(ctx, ds, 1)
		require.NoError(t, err)
		require.Len(t, plan.Partitions, 1)

		// Stream first batch of 10 rows and capture cursor.
		var firstCursor string
		var firstBatchCount int
		err = v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
			Partition: plan.Partitions[0],
			Revision:  plan.Revision,
			BatchSize: 10,
		}, func(batch v1.ExportBatch) error {
			if firstCursor == "" {
				firstCursor = batch.Cursor
				firstBatchCount = len(batch.Relationships)
			}
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 10, firstBatchCount)

		// Resume from cursor — should get the remaining 40.
		var resumedCount int
		err = v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
			Partition: plan.Partitions[0],
			Revision:  plan.Revision,
			BatchSize: 1000,
			Cursor:    firstCursor,
		}, func(batch v1.ExportBatch) error {
			resumedCount += len(batch.Relationships)
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 40, resumedCount)
		require.Equal(t, 50, firstBatchCount+resumedCount, "resume should cover all remaining rows")
	})
}
