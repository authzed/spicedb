//go:build ci && docker

package crdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/proxy"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPlanPartitions(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

	tests := []struct {
		name              string
		numNamespaces     int
		relsPerNamespace  int
		desiredPartitions uint32
		minPartitions     int
	}{
		{
			name:              "empty datastore with desiredCount=1",
			numNamespaces:     0,
			relsPerNamespace:  0,
			desiredPartitions: 1,
			minPartitions:     1,
		},
		{
			name:              "populated datastore returns valid partitions",
			numNamespaces:     5,
			relsPerNamespace:  100,
			desiredPartitions: 4,
			minPartitions:     1,
		},
		{
			name:              "small dataset with multiple partitions requested",
			numNamespaces:     3,
			relsPerNamespace:  50,
			desiredPartitions: 4,
			minPartitions:     1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, createDatastoreTest(b, func(t *testing.T, ds datastore.Datastore) {
			ctx := context.Background()

			totalWritten := tt.numNamespaces * tt.relsPerNamespace
			if totalWritten > 0 {
				_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					var updates []tuple.RelationshipUpdate
					for ns := 0; ns < tt.numNamespaces; ns++ {
						for i := 0; i < tt.relsPerNamespace; i++ {
							updates = append(updates, tuple.Create(tuple.MustParse(
								fmt.Sprintf("resource%d:%d#viewer@user:%d", ns, i, i),
							)))
						}
					}
					return rwt.WriteRelationships(ctx, updates)
				})
				require.NoError(t, err)
			}

			partitioner := datastore.UnwrapAs[datastore.BulkExportPartitioner](ds)
			require.NotNil(t, partitioner)

			rev, err := ds.HeadRevision(ctx)
			require.NoError(t, err)

			partitions, err := partitioner.PlanPartitions(ctx, rev, tt.desiredPartitions)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(partitions), tt.minPartitions)

			// First partition has nil lower bound, last has nil upper bound.
			require.Nil(t, partitions[0].LowerBound)
			require.Nil(t, partitions[len(partitions)-1].UpperBound)

			// Adjacent partitions share boundaries.
			for i := 1; i < len(partitions); i++ {
				require.NotNil(t, partitions[i].LowerBound)
				require.NotNil(t, partitions[i-1].UpperBound)
				lower := dsoptions.ToRelationship(partitions[i].LowerBound)
				upper := dsoptions.ToRelationship(partitions[i-1].UpperBound)
				require.Equal(t, *upper, *lower)
			}

			// Verify all relationships are covered by the partitions.
			reader := ds.SnapshotReader(rev)
			var totalRead int
			for _, p := range partitions {
				opts := []dsoptions.QueryOptionsOption{
					dsoptions.WithSort(dsoptions.ByResource),
				}
				if p.LowerBound != nil {
					opts = append(opts, dsoptions.WithAfter(p.LowerBound))
				}
				if p.UpperBound != nil {
					opts = append(opts, dsoptions.WithBeforeOrEqual(p.UpperBound))
				}

				iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{}, opts...)
				require.NoError(t, err)

				for _, err := range iter {
					require.NoError(t, err)
					totalRead++
				}
			}
			require.Equal(t, totalWritten, totalRead, "partitions should cover all relationships")
		},
			GCWindow(veryLargeGCWindow),
			RevisionQuantization(0),
			WithAcquireTimeout(30*time.Second),
		))
	}
}

func TestUnwrapAsBulkExportPartitioner(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

	t.Run("bare datastore can be unwrapped", createDatastoreTest(b, func(t *testing.T, ds datastore.Datastore) {
		partitioner := datastore.UnwrapAs[datastore.BulkExportPartitioner](ds)
		require.NotNil(t, partitioner, "expected datastore to be unwrappable as BulkExportPartitioner")
	},
		GCWindow(veryLargeGCWindow),
		RevisionQuantization(0),
		WithAcquireTimeout(30*time.Second),
	))

	t.Run("readonly-wrapped datastore can be unwrapped", createDatastoreTest(b, func(t *testing.T, ds datastore.Datastore) {
		roDS := proxy.NewReadonlyDatastore(ds)
		partitioner := datastore.UnwrapAs[datastore.BulkExportPartitioner](roDS)
		require.NotNil(t, partitioner, "expected readonly-wrapped datastore to be unwrappable as BulkExportPartitioner")
	},
		GCWindow(veryLargeGCWindow),
		RevisionQuantization(0),
		WithAcquireTimeout(30*time.Second),
	))
}

func TestPlanPartitionsMultiNode(t *testing.T) {
	b := testdatastore.RunCRDBClusterForTesting(t, 3, crdbTestVersion())

	t.Run("forced splits produce real partitions", func(t *testing.T) {
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

		// Force range splits at specific namespace boundaries using raw SQL.
		conn, err := pgx.Connect(ctx, connectStr)
		require.NoError(t, err)
		defer conn.Close(ctx)

		splitPoints := []struct{ ns, oid, rel, uns, uoid, urel string }{
			{"resource3", "0", "viewer", "user", "0", "..."},
			{"resource5", "0", "viewer", "user", "0", "..."},
			{"resource7", "0", "viewer", "user", "0", "..."},
		}
		for _, sp := range splitPoints {
			_, err := conn.Exec(ctx, fmt.Sprintf(
				`ALTER TABLE relation_tuple SPLIT AT VALUES ('%s', '%s', '%s', '%s', '%s', '%s')`,
				sp.ns, sp.oid, sp.rel, sp.uns, sp.uoid, sp.urel,
			))
			require.NoError(t, err)
		}

		// Now plan partitions — should get multiple real partitions.
		partitioner := datastore.UnwrapAs[datastore.BulkExportPartitioner](ds)
		require.NotNil(t, partitioner)

		rev, err := ds.HeadRevision(ctx)
		require.NoError(t, err)

		partitions, err := partitioner.PlanPartitions(ctx, rev, 4)
		require.NoError(t, err)
		require.Greater(t, len(partitions), 1, "forced splits should produce multiple partitions")

		t.Logf("Got %d partitions from %d forced split points", len(partitions), len(splitPoints))

		// First partition has nil lower bound, last has nil upper bound.
		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[len(partitions)-1].UpperBound)

		// Adjacent partitions share boundaries (no gaps).
		for i := 1; i < len(partitions); i++ {
			require.NotNil(t, partitions[i].LowerBound, "partition %d should have lower bound", i)
			require.NotNil(t, partitions[i-1].UpperBound, "partition %d should have upper bound", i-1)
			lower := dsoptions.ToRelationship(partitions[i].LowerBound)
			upper := dsoptions.ToRelationship(partitions[i-1].UpperBound)
			require.Equal(t, *upper, *lower, "boundary between partition %d and %d should match", i-1, i)
		}

		// Read all relationships via partitioned queries — verify no gaps or overlaps.
		reader := ds.SnapshotReader(rev)
		seen := make(map[string]int)
		for pi, p := range partitions {
			opts := []dsoptions.QueryOptionsOption{
				dsoptions.WithSort(dsoptions.ByResource),
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
				require.False(t, duplicate, "relationship %s appeared in both partition %d and %d", key, prevPartition, pi)
				seen[key] = pi
				partitionCount++
			}
			t.Logf("Partition %d: %d relationships", pi, partitionCount)
		}

		require.Equal(t, totalWritten, len(seen), "partitions should cover all relationships with no gaps")
	})
}
