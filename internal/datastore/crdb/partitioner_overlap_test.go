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
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestPlanPartitionsOnlyUsesPrimaryIndex verifies that PlanPartitions returns
// non-overlapping partitions that cover all data exactly once, even when the
// table has secondary indexes with different column orders.
//
// Background: SHOW RANGES FROM TABLE returns ranges for ALL indexes on the table.
// Secondary index range keys have columns in a different order than the PK, so
// parseRangeStartKey would map them into the wrong fields if they were included,
// producing partition bounds that overlap in PK tuple order. The fix is to use
// SHOW RANGES FROM INDEX @primary to only query PK ranges.
func TestPlanPartitionsOnlyUsesPrimaryIndex(t *testing.T) {
	b := testdatastore.RunCRDBClusterForTesting(t, 3, crdbTestVersion())

	t.Run("partitions are non-overlapping with secondary indexes present", func(t *testing.T) {
		ctx := context.Background()

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

		// Write relationships with varied namespaces and subject IDs so that
		// both PK and secondary index ranges have meaningful split points.
		totalWritten := 0
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			var updates []tuple.RelationshipUpdate
			for ns := 0; ns < 10; ns++ {
				for i := 0; i < 100; i++ {
					updates = append(updates, tuple.Create(tuple.MustParse(
						fmt.Sprintf("resource%d:%d#viewer@user%d:%d", ns, i, ns, i),
					)))
					totalWritten++
				}
			}
			return rwt.WriteRelationships(ctx, updates)
		})
		require.NoError(t, err)

		conn, err := pgx.Connect(ctx, connectStr)
		require.NoError(t, err)
		defer conn.Close(ctx)

		// Force splits on the PRIMARY index at known PK boundaries.
		for _, ns := range []string{"resource3", "resource5", "resource7"} {
			_, err := conn.Exec(ctx, fmt.Sprintf(
				`ALTER TABLE relation_tuple SPLIT AT VALUES ('%s', '0', 'viewer', 'user0', '0', '...')`, ns,
			))
			require.NoError(t, err)
		}

		// Force splits on the SECONDARY index (ix_relation_tuple_by_subject)
		// at subject-ordered boundaries. These have columns in a different order:
		// (userset_object_id, userset_namespace, userset_relation, namespace, relation)
		for _, uoid := range []string{"30", "60", "90"} {
			_, err := conn.Exec(ctx, fmt.Sprintf(
				`ALTER INDEX relation_tuple@ix_relation_tuple_by_subject SPLIT AT VALUES ('%s', 'user0', '...', 'resource0', 'viewer')`, uoid,
			))
			require.NoError(t, err)
		}

		// Verify that SHOW RANGES FROM TABLE includes secondary index ranges
		// (confirming the table has ranges from multiple indexes).
		var tableRangeCount int
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM [SHOW RANGES FROM TABLE relation_tuple]").Scan(&tableRangeCount))

		var primaryRangeCount int
		require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM [SHOW RANGES FROM INDEX relation_tuple@primary]").Scan(&primaryRangeCount))

		require.Greater(t, tableRangeCount, primaryRangeCount,
			"SHOW RANGES FROM TABLE should return more ranges than FROM INDEX @primary (secondary indexes present)")
		t.Logf("Table ranges: %d, Primary index ranges: %d", tableRangeCount, primaryRangeCount)

		// Plan partitions — should only use primary index ranges.
		partitioner := datastore.UnwrapAs[datastore.BulkExportPartitioner](ds)
		require.NotNil(t, partitioner)

		rev, err := ds.HeadRevision(ctx)
		require.NoError(t, err)

		partitions, err := partitioner.PlanPartitions(ctx, rev, 8)
		require.NoError(t, err)
		require.Greater(t, len(partitions), 1, "expected multiple partitions")
		t.Logf("Got %d partitions", len(partitions))

		// Verify partitions are non-overlapping and cover all data exactly once.
		reader := ds.SnapshotReader(rev)
		seen := make(map[string]int) // relationship key → partition index
		for pi, p := range partitions {
			opts := []dsoptions.QueryOptionsOption{
				dsoptions.WithSort(dsoptions.ByResource),
				dsoptions.WithUseTupleComparison(true),
			}
			if p.LowerBound != nil {
				opts = append(opts, dsoptions.WithAfter(p.LowerBound))
			}
			if p.UpperBound != nil {
				opts = append(opts, dsoptions.WithBeforeOrEqual(p.UpperBound))
			}

			iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{}, opts...)
			require.NoError(t, err)

			partitionCount := 0
			for rel, err := range iter {
				require.NoError(t, err)
				key := tuple.MustString(rel)
				prevPartition, duplicate := seen[key]
				require.False(t, duplicate,
					"OVERLAP: relationship %s appeared in both partition %d and %d", key, prevPartition, pi)
				seen[key] = pi
				partitionCount++
			}
			t.Logf("Partition %d: %d relationships", pi, partitionCount)
		}

		require.Equal(t, totalWritten, len(seen),
			"partitions should cover all %d relationships exactly once (got %d)", totalWritten, len(seen))
	})
}
