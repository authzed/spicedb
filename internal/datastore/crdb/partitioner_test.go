//go:build datastore

package crdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	v1 "github.com/authzed/spicedb/pkg/services/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestPlanPartitionedExport(t *testing.T) {
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

	writeTestRelationships(t, ds, 10, 100)

	// Force 5 splits → 6 parseable boundaries.
	conn, err := pgx.Connect(ctx, connectStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	for _, ns := range []string{"resource2", "resource4", "resource5", "resource7", "resource8"} {
		_, err := conn.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE relation_tuple SPLIT AT VALUES ('%s', '0', 'viewer', 'user', '0', '...')`, ns,
		))
		require.NoError(t, err)
	}

	tests := []struct {
		name              string
		desiredPartitions uint32
		// expectSinglePartition is true when we expect exactly one full-table
		// partition (lower=nil, upper=nil). When false, we just check that
		// the plan returns ≤desired partitions, all non-overlapping, and
		// covering all data exactly once. We avoid asserting exact split
		// positions because real CRDB's range_size for tiny test data is
		// not deterministic enough to pin them down.
		expectSinglePartition bool
	}{
		{name: "desired=0 defaults to 1", desiredPartitions: 0, expectSinglePartition: true},
		{name: "desired=1 returns single partition", desiredPartitions: 1, expectSinglePartition: true},
		{name: "desired=2 returns at most 2 partitions", desiredPartitions: 2},
		{name: "desired=3 returns at most 3 partitions", desiredPartitions: 3},
		{name: "desired=6 returns at most 6 partitions", desiredPartitions: 6},
		{name: "desired=100 caps at usable splits", desiredPartitions: 100},
	}

	rev, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := v1.PlanPartitionedExport(ctx, ds, tc.desiredPartitions)
			require.NoError(t, err)
			require.NotEmpty(t, plan.Partitions)

			if tc.expectSinglePartition {
				require.Len(t, plan.Partitions, 1)
				require.Nil(t, plan.Partitions[0].LowerBound)
				require.Nil(t, plan.Partitions[0].UpperBound)
			} else {
				require.LessOrEqual(t, len(plan.Partitions), int(tc.desiredPartitions),
					"got more partitions than requested")
				// First partition starts at -∞, last ends at +∞, and every
				// internal boundary is shared.
				require.Nil(t, plan.Partitions[0].LowerBound)
				require.Nil(t, plan.Partitions[len(plan.Partitions)-1].UpperBound)
				for i := 1; i < len(plan.Partitions); i++ {
					require.NotNil(t, plan.Partitions[i-1].UpperBound)
					require.NotNil(t, plan.Partitions[i].LowerBound)
					require.Equal(t,
						*dsoptions.ToRelationship(plan.Partitions[i-1].UpperBound),
						*dsoptions.ToRelationship(plan.Partitions[i].LowerBound),
						"partition %d/%d boundaries do not match", i-1, i)
				}
			}

			// All partitions together should cover all 1000 relationships
			// exactly once.
			seen := make(map[string]int)
			for pi, partition := range plan.Partitions {
				iter, err := v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
					Partition: partition,
					Revision:  rev.Revision,
				})
				require.NoError(t, err)
				for rel, err := range iter {
					require.NoError(t, err)
					key := tuple.MustString(rel)
					prev, dup := seen[key]
					require.False(t, dup, "rel %s appeared in partitions %d and %d", key, prev, pi)
					seen[key] = pi
				}
			}
			require.Len(t, seen, 1000, "all partitions should cover all 1000 relationships")
		})
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
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

	t.Run("partitions are non-overlapping with secondary indexes present", func(t *testing.T) {
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

		partitions, err := partitioner.PlanPartitions(ctx, 8)
		require.NoError(t, err)
		require.Greater(t, len(partitions), 1, "expected multiple partitions")
		t.Logf("Got %d partitions", len(partitions))

		// Verify partitions are non-overlapping and cover all data exactly once.
		rev, err := ds.HeadRevision(ctx)
		require.NoError(t, err)
		reader := ds.SnapshotReader(rev.Revision)
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

		require.Len(t, seen, totalWritten,
			"partitions should cover all %d relationships exactly once (got %d)", totalWritten, len(seen))
	})
}

func TestParseRangeStartKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		wantErr  bool
		wantNS   string
		wantOID  string
		wantRel  string
		wantUNS  string
		wantUOID string
		wantURel string
	}{
		{
			name:     "valid key with 6 PK columns",
			key:      `/Table/53/1/"document"/"doc123"/"viewer"/"user"/"alice"/"member"`,
			wantNS:   "document",
			wantOID:  "doc123",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "alice",
			wantURel: "member",
		},
		{
			name:     "valid key with extra trailing parts",
			key:      `/Table/53/1/"ns"/"oid"/"rel"/"uns"/"uoid"/"urel"/extra`,
			wantNS:   "ns",
			wantOID:  "oid",
			wantRel:  "rel",
			wantUNS:  "uns",
			wantUOID: "uoid",
			wantURel: "urel",
		},
		{
			name:     "valid key with slashes in PK values",
			key:      `…/4/"dev_v1/my_organization"/"..."/"dev_v1/my_organization"/"is_enabled"/"ca7fd83c-a53a-4928-a0b1-79f22699cd05"/"ca7fd83c-a53a-4928-a0b1-79f22699cd05"`,
			wantNS:   "dev_v1/my_organization",
			wantOID:  "...",
			wantRel:  "dev_v1/my_organization",
			wantUNS:  "is_enabled",
			wantUOID: "ca7fd83c-a53a-4928-a0b1-79f22699cd05",
			wantURel: "ca7fd83c-a53a-4928-a0b1-79f22699cd05",
		},
		{
			name:     "valid key with pipe in object_id",
			key:      `/Table/53/1/"resource"/"foo|bar"/"viewer"/"user"/"baz|qux"/"..."`,
			wantNS:   "resource",
			wantOID:  "foo|bar",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "baz|qux",
			wantURel: "...",
		},
		{
			name:     "valid key with equals and plus in object_id",
			key:      `/Table/53/1/"resource"/"key=value+extra"/"viewer"/"user"/"id=123+456"/"..."`,
			wantNS:   "resource",
			wantOID:  "key=value+extra",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "id=123+456",
			wantURel: "...",
		},
		{
			name:     "valid key with wildcard subject",
			key:      `/Table/53/1/"resource"/"123"/"viewer"/"user"/"*"/"..."`,
			wantNS:   "resource",
			wantOID:  "123",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "*",
			wantURel: "...",
		},
		{
			name:     "valid key with uppercase in object_id",
			key:      `/Table/53/1/"resource"/"AbCdEf123"/"viewer"/"user"/"XyZ789"/"..."`,
			wantNS:   "resource",
			wantOID:  "AbCdEf123",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "XyZ789",
			wantURel: "...",
		},
		{
			name:     "more than 6 quoted segments uses only first 6",
			key:      `/Table/53/1/"ns/with/slashes"/"obj/id"/"rel"/"user/ns"/"sub/id"/"sub/rel"/"extra_seventh"`,
			wantNS:   "ns/with/slashes",
			wantOID:  "obj/id",
			wantRel:  "rel",
			wantUNS:  "user/ns",
			wantUOID: "sub/id",
			wantURel: "sub/rel",
		},
		{
			name:     "valid key with backslash-escaped quotes in values",
			key:      `…/"test"/"has\"quote"/"viewer"/"user"/"al\"ice"/"..."`,
			wantNS:   "test",
			wantOID:  `has"quote`,
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: `al"ice`,
			wantURel: "...",
		},
		{
			name:     "valid key with ellipsis relation",
			key:      `/Table/77/1/"resource3"/"0"/"viewer"/"user"/"0"/"..."`,
			wantNS:   "resource3",
			wantOID:  "0",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "0",
			wantURel: "...",
		},
		{
			name:    "key with fewer than 6 quoted values",
			key:     `/Table/53/1/"document"/"doc123"/"viewer"`,
			wantErr: true,
		},
		{
			name:    "key with no quoted values",
			key:     `…/2`,
			wantErr: true,
		},
		{
			name:    "empty key",
			key:     ``,
			wantErr: true,
		},
		{
			name:    "key with only table and index",
			key:     `/Table/53/1`,
			wantErr: true,
		},
		{
			name:    "key with empty quoted value in namespace",
			key:     `/Table/53/1/""/"oid"/"rel"/"uns"/"uoid"/"urel"`,
			wantErr: true,
		},
		{
			name:    "key with empty quoted value in object_id",
			key:     `/Table/53/1/"ns"/""/"rel"/"uns"/"uoid"/"urel"`,
			wantErr: true,
		},
		{
			name:    "before-style key",
			key:     `<before:/Table/77/3>`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := parseRangeStartKey(tt.key)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cursor)

			rel := dsoptions.ToRelationship(cursor)
			require.Equal(t, tt.wantNS, rel.Resource.ObjectType)
			require.Equal(t, tt.wantOID, rel.Resource.ObjectID)
			require.Equal(t, tt.wantRel, rel.Resource.Relation)
			require.Equal(t, tt.wantUNS, rel.Subject.ObjectType)
			require.Equal(t, tt.wantUOID, rel.Subject.ObjectID)
			require.Equal(t, tt.wantURel, rel.Subject.Relation)
		})
	}
}

func TestPlanPartitionsLogic(t *testing.T) {
	t.Run("desiredCount=0 returns single partition", func(t *testing.T) {
		partitions := groupRanges(t.Context(), nil, 0)
		require.Len(t, partitions, 1)
		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[0].UpperBound)
	})

	t.Run("desiredCount=1 returns single partition", func(t *testing.T) {
		partitions := groupRanges(t.Context(), nil, 1)
		require.Len(t, partitions, 1)
	})

	t.Run("single boundary yields two partitions", func(t *testing.T) {
		// One parseable boundary is a usable split point: it separates the
		// unparseable prefix region from the parseable range, giving two
		// partitions [-∞, b) and [b, +∞).
		b := makeBoundary("ns1", "oid1", "rel1", "uns1", "uoid1", "urel1")
		partitions := groupRanges(t.Context(), equalSizeRanges(b), 4)
		require.Len(t, partitions, 2)
		require.Nil(t, partitions[0].LowerBound)
		require.Equal(t,
			*dsoptions.ToRelationship(b),
			*dsoptions.ToRelationship(partitions[0].UpperBound),
		)
		require.Equal(t,
			*dsoptions.ToRelationship(b),
			*dsoptions.ToRelationship(partitions[1].LowerBound),
		)
		require.Nil(t, partitions[1].UpperBound)
	})

	t.Run("equal-sized ranges split into K partitions", func(t *testing.T) {
		boundaries := []dsoptions.Cursor{
			makeBoundary("a", "0", "r", "u", "0", "..."),
			makeBoundary("b", "0", "r", "u", "0", "..."),
			makeBoundary("c", "0", "r", "u", "0", "..."),
			makeBoundary("d", "0", "r", "u", "0", "..."),
		}
		// 4 ranges, target = 4/4 = 1; greedy splits before each successor.
		partitions := groupRanges(t.Context(), equalSizeRanges(boundaries...), 4)
		require.Len(t, partitions, 4)

		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[3].UpperBound)
		for i := 1; i < len(partitions); i++ {
			require.Equal(t,
				*dsoptions.ToRelationship(partitions[i-1].UpperBound),
				*dsoptions.ToRelationship(partitions[i].LowerBound),
			)
		}
	})

	t.Run("partition count caps at usable splits", func(t *testing.T) {
		// 1 boundary, K=100: only one split point available, two partitions.
		b := makeBoundary("a", "0", "r", "u", "0", "...")
		partitions := groupRanges(t.Context(), equalSizeRanges(b), 100)
		require.Len(t, partitions, 2)
	})

	t.Run("many equal ranges with small K balances by size", func(t *testing.T) {
		boundaries := make([]dsoptions.Cursor, 20)
		for i := range boundaries {
			boundaries[i] = makeBoundary(
				fmt.Sprintf("ns%02d", i), "0", "r", "u", "0", "...",
			)
		}
		// 20 ranges of size 1, K=4 → target 5 → splits every 5 ranges.
		partitions := groupRanges(t.Context(), equalSizeRanges(boundaries...), 4)
		require.Len(t, partitions, 4)

		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[3].UpperBound)
		for i := 1; i < len(partitions); i++ {
			require.Equal(t,
				*dsoptions.ToRelationship(partitions[i-1].UpperBound),
				*dsoptions.ToRelationship(partitions[i].LowerBound),
			)
		}
	})

	t.Run("skewed sizes yield fewer but balanced partitions", func(t *testing.T) {
		// Sizes [10, 1, 1, 1, 1] (total 14), K=3, target = 4. The heavy
		// first range clears the target on its own, so we split right after
		// it; the remaining tiny ranges never reach 4 again, so they all
		// merge into the trailing partition. Result: 2 partitions of sizes
		// 10 and 4 — fewer than the 3 requested, but each close to its fair
		// share. This is the behavior trade-off of simple greedy: balance
		// over partition count.
		boundaries := make([]dsoptions.Cursor, 5)
		for i := range boundaries {
			boundaries[i] = makeBoundary(
				fmt.Sprintf("ns%02d", i), "0", "r", "u", "0", "...",
			)
		}
		ranges := []rangeInfo{
			{cursor: boundaries[0], size: 10},
			{cursor: boundaries[1], size: 1},
			{cursor: boundaries[2], size: 1},
			{cursor: boundaries[3], size: 1},
			{cursor: boundaries[4], size: 1},
		}
		partitions := groupRanges(t.Context(), ranges, 3)
		require.Len(t, partitions, 2)
		require.Equal(t,
			*dsoptions.ToRelationship(boundaries[1]),
			*dsoptions.ToRelationship(partitions[0].UpperBound),
		)
	})

	t.Run("empty ranges returns single partition", func(t *testing.T) {
		partitions := groupRanges(t.Context(), nil, 4)
		require.Len(t, partitions, 1)
		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[0].UpperBound)
	})

	t.Run("size-aware packing skews splits toward heavy ranges", func(t *testing.T) {
		// 5 ranges, the first carrying 90% of the data:
		//   R0 [b0 .. b1)  size=900
		//   R1 [b1 .. b2)  size= 25
		//   R2 [b2 .. b3)  size= 25
		//   R3 [b3 .. b4)  size= 25
		//   R4 [b4 .. -)   size= 25
		// Total=1000, K=2, target=500. The algorithm must split right after
		// the heavy first range — i.e., at b1 — even though that's the very
		// first usable boundary, which the old index-based algorithm would
		// never pick.
		b0 := makeBoundary("a", "0", "r", "u", "0", "...")
		b1 := makeBoundary("b", "0", "r", "u", "0", "...")
		b2 := makeBoundary("c", "0", "r", "u", "0", "...")
		b3 := makeBoundary("d", "0", "r", "u", "0", "...")
		b4 := makeBoundary("e", "0", "r", "u", "0", "...")
		ranges := []rangeInfo{
			{cursor: b0, size: 900},
			{cursor: b1, size: 25},
			{cursor: b2, size: 25},
			{cursor: b3, size: 25},
			{cursor: b4, size: 25},
		}

		partitions := groupRanges(t.Context(), ranges, 2)
		require.Len(t, partitions, 2)
		require.Nil(t, partitions[0].LowerBound)
		require.NotNil(t, partitions[0].UpperBound)
		require.Equal(t,
			*dsoptions.ToRelationship(b1),
			*dsoptions.ToRelationship(partitions[0].UpperBound),
			"split should land at b1, isolating the heavy first range",
		)
		require.Nil(t, partitions[1].UpperBound)
	})

	t.Run("zero total size falls back to per-boundary splits", func(t *testing.T) {
		// Pathological case: every range reports size 0. We should still
		// produce K partitions from the available split points rather than
		// dividing by zero or collapsing to one partition.
		ranges := []rangeInfo{
			{cursor: makeBoundary("a", "0", "r", "u", "0", "..."), size: 0},
			{cursor: makeBoundary("b", "0", "r", "u", "0", "..."), size: 0},
			{cursor: makeBoundary("c", "0", "r", "u", "0", "..."), size: 0},
			{cursor: makeBoundary("d", "0", "r", "u", "0", "..."), size: 0},
		}
		partitions := groupRanges(t.Context(), ranges, 4)
		require.Len(t, partitions, 4)
	})
}

// equalSizeRanges builds a rangeInfo slice modeling N ranges of equal size,
// each keyed by one of the supplied boundaries. All N boundaries are usable
// as split points (the first separates the unparseable prefix region from
// ranges[0]), so groupRanges produces at most N+1 partitions.
func equalSizeRanges(boundaries ...dsoptions.Cursor) []rangeInfo {
	ranges := make([]rangeInfo, 0, len(boundaries))
	for _, b := range boundaries {
		ranges = append(ranges, rangeInfo{cursor: b, size: 1})
	}
	return ranges
}

// TestPartitionerPrimaryKeyAssumptions verifies that the primary key of the
// relationship table matches the column order assumed by parseRangeStartKey.
// If the PK changes, this test will fail — update parseRangeStartKey's column
// mapping (values[0..5]) accordingly.
func TestPartitionerPrimaryKeyAssumptions(t *testing.T) {
	expectedColumnsSQL := `PRIMARY KEY (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)`
	require.Equal(t, expectedColumnsSQL, schema.IndexPrimaryKey.ColumnsSQL,
		"The partitioner assumes parseRangeStartKey maps quoted values positionally to "+
			"(namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation). "+
			"If the primary key columns or their order changed, update parseRangeStartKey in partitioner.go.")
}

func makeBoundary(ns, oid, rel, uns, uoid, urel string) dsoptions.Cursor {
	return dsoptions.ToCursor(tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: ns,
				ObjectID:   oid,
				Relation:   rel,
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: uns,
				ObjectID:   uoid,
				Relation:   urel,
			},
		},
	})
}

// writeTestRelationships writes numNamespaces * relsPerNamespace relationships
// to the datastore using the pattern "resource{ns}:{i}#viewer@user:{i}".
func writeTestRelationships(t *testing.T, ds datastore.Datastore, numNamespaces, relsPerNamespace int) int {
	t.Helper()
	totalWritten := 0
	_, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		var updates []tuple.RelationshipUpdate
		for ns := 0; ns < numNamespaces; ns++ {
			for i := 0; i < relsPerNamespace; i++ {
				updates = append(updates, tuple.Create(tuple.MustParse(
					fmt.Sprintf("resource%d:%d#viewer@user:%d", ns, i, i),
				)))
				totalWritten++
			}
		}
		return rwt.WriteRelationships(ctx, updates)
	})
	require.NoError(t, err)
	return totalWritten
}
