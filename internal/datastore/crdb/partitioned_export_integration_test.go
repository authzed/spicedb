//go:build ci && docker

package crdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	v1 "github.com/authzed/spicedb/pkg/services/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPartitionedExportEndToEnd(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())
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

	totalWritten := writeTestRelationships(t, ds, 10, 100)

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

	// Request plan.
	plan, err := v1.PlanPartitionedExport(ctx, ds, 4)
	require.NoError(t, err)
	require.Len(t, plan.Partitions, 4, "3 forced splits should produce 4 partitions")
	require.NotNil(t, plan.Revision)

	t.Logf("Plan returned %d partitions at revision %s", len(plan.Partitions), plan.Revision)

	// Stream each partition and collect all relationships.
	seen := make(map[string]int) // relationship key → partition index
	for pi, partition := range plan.Partitions {
		partitionCount := 0
		err := v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
			Partition: partition,
			Revision:  plan.Revision,
			BatchSize: 1000,
		}, func(batch v1.ExportBatch) error {
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
}

func TestStreamPartitionedExportBoundCombinations(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

	t.Run("all lower/upper bound combinations", func(t *testing.T) {
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

		totalWritten := writeTestRelationships(t, ds, 5, 10)

		rev, err := ds.HeadRevision(ctx)
		require.NoError(t, err)

		mid := dsoptions.ToCursor(tuple.MustParse("resource2:5#viewer@user:5"))

		tests := []struct {
			name            string
			lower           dsoptions.Cursor
			upper           dsoptions.Cursor
			expectedResults int
		}{
			{
				name:            "nil lower, nil upper (full table)",
				lower:           nil,
				upper:           nil,
				expectedResults: totalWritten,
			},
			{
				name:            "nil lower, non-nil upper (first partition)",
				lower:           nil,
				upper:           mid,
				expectedResults: 26, // resource0(10) + resource1(10) + resource2:0..5(6)
			},
			{
				name:            "non-nil lower, nil upper (last partition)",
				lower:           mid,
				upper:           nil,
				expectedResults: 24, // resource2:6..9(4) + resource3(10) + resource4(10)
			},
			{
				name:            "non-nil lower, non-nil upper (middle partition)",
				lower:           dsoptions.ToCursor(tuple.MustParse("resource1:0#viewer@user:0")),
				upper:           dsoptions.ToCursor(tuple.MustParse("resource3:0#viewer@user:0")),
				expectedResults: 20, // resource1:1..9(9) + resource2(10) + resource3:0(1)
			},
			{
				name:            "bounds with no data in range",
				lower:           dsoptions.ToCursor(tuple.MustParse("zzz:0#viewer@user:0")),
				upper:           dsoptions.ToCursor(tuple.MustParse("zzz:1#viewer@user:1")),
				expectedResults: 0,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var totalRels int
				err := v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
					Partition: v1.ExportPartition{
						LowerBound: tc.lower,
						UpperBound: tc.upper,
					},
					Revision:  rev,
					BatchSize: 100,
				}, func(batch v1.ExportBatch) error {
					totalRels += len(batch.Relationships)
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, tc.expectedResults, totalRels)
			})
		}

		// Verify adjacent partitions cover all data exactly once.
		t.Run("adjacent partitions cover all data exactly once", func(t *testing.T) {
			// Force a split to get a real boundary.
			conn, err := pgx.Connect(ctx, connectStr)
			require.NoError(t, err)
			defer conn.Close(ctx)

			_, err = conn.Exec(ctx, `ALTER TABLE relation_tuple SPLIT AT VALUES ('ns2', '5', 'viewer', 'user', '5', '...')`)
			require.NoError(t, err)

			seen := make(map[string]int)
			for pi, partition := range []v1.ExportPartition{
				{Index: 0, LowerBound: nil, UpperBound: mid},
				{Index: 1, LowerBound: mid, UpperBound: nil},
			} {
				err := v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
					Partition: partition,
					Revision:  rev,
					BatchSize: 100,
				}, func(batch v1.ExportBatch) error {
					for _, rel := range batch.Relationships {
						key := tuple.MustString(rel)
						prevPI, dup := seen[key]
						require.False(t, dup, "relationship %s in both partition %d and %d", key, prevPI, pi)
						seen[key] = pi
					}
					return nil
				})
				require.NoError(t, err)
			}
			require.Len(t, seen, totalWritten, "two adjacent partitions should cover all relationships")
		})
	})
}
