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

	// Split points: resource2, resource4, resource5, resource7, resource8
	// These are the boundaries available for groupBoundaries to select from.
	b2 := "resource2:0#viewer@user:0"
	b4 := "resource4:0#viewer@user:0"
	b5 := "resource5:0#viewer@user:0"
	b7 := "resource7:0#viewer@user:0"
	b8 := "resource8:0#viewer@user:0"

	type bound struct {
		lower string // "" = nil (start of table)
		upper string // "" = nil (end of table)
	}

	tests := []struct {
		name              string
		desiredPartitions uint32
		expectedBounds    []bound
	}{
		{
			name:              "desired=0 defaults to 1",
			desiredPartitions: 0,
			expectedBounds:    []bound{{"", ""}},
		},
		{
			name:              "desired=1 returns single partition",
			desiredPartitions: 1,
			expectedBounds:    []bound{{"", ""}},
		},
		{
			name:              "desired=2 groups boundaries into 2",
			desiredPartitions: 2,
			// idx for upper[0] = (1*5)/2 = 2 → boundary[2] = resource5
			// idx for lower[1] = (1*5)/2 = 2 → boundary[2] = resource5
			expectedBounds: []bound{
				{"", b5},
				{b5, ""},
			},
		},
		{
			name:              "desired=3 downsamples from 5 boundaries",
			desiredPartitions: 3,
			// upper[0] = boundary[(1*5)/3=1] = resource4
			// lower[1] = boundary[(1*5)/3=1] = resource4, upper[1] = boundary[(2*5)/3=3] = resource7
			// lower[2] = boundary[(2*5)/3=3] = resource7
			expectedBounds: []bound{
				{"", b4},
				{b4, b7},
				{b7, ""},
			},
		},
		{
			name:              "desired=exact boundary count+1",
			desiredPartitions: 6, // 5 boundaries → 6 partitions, all boundaries used
			expectedBounds: []bound{
				{"", b2},
				{b2, b4},
				{b4, b5},
				{b5, b7},
				{b7, b8},
				{b8, ""},
			},
		},
		{
			name:              "desired exceeds boundary count+1, capped at 6",
			desiredPartitions: 100,
			expectedBounds: []bound{
				{"", b2},
				{b2, b4},
				{b4, b5},
				{b5, b7},
				{b7, b8},
				{b8, ""},
			},
		},
	}

	rev, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := v1.PlanPartitionedExport(ctx, ds, tc.desiredPartitions)
			require.NoError(t, err)
			require.Len(t, plan.Partitions, len(tc.expectedBounds))

			for i, expected := range tc.expectedBounds {
				p := plan.Partitions[i]

				if expected.lower == "" {
					require.Nil(t, p.LowerBound, "partition %d lower bound should be nil", i)
				} else {
					require.NotNil(t, p.LowerBound, "partition %d lower bound should not be nil", i)
					require.Equal(t, expected.lower, tuple.MustString(*dsoptions.ToRelationship(p.LowerBound)),
						"partition %d lower bound mismatch", i)
				}

				if expected.upper == "" {
					require.Nil(t, p.UpperBound, "partition %d upper bound should be nil", i)
				} else {
					require.NotNil(t, p.UpperBound, "partition %d upper bound should not be nil", i)
					require.Equal(t, expected.upper, tuple.MustString(*dsoptions.ToRelationship(p.UpperBound)),
						"partition %d upper bound mismatch", i)
				}
			}

			// All partitions together should cover all data.
			totalRels := 0
			for _, partition := range plan.Partitions {
				iter, err := v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
					Partition: partition,
					Revision:  rev.Revision,
				})
				require.NoError(t, err)
				for _, err := range iter {
					require.NoError(t, err)
					totalRels++
				}
			}
			require.Equal(t, 1000, totalRels, "all partitions should cover all 1000 relationships")
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
		// PlanPartitions with 0 should behave like 1.
		// We can't call the real method without a datastore, so test the
		// boundary grouping logic directly.
		partitions := groupBoundaries(nil, 0)
		require.Len(t, partitions, 1)
		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[0].UpperBound)
	})

	t.Run("desiredCount=1 returns single partition", func(t *testing.T) {
		partitions := groupBoundaries(nil, 1)
		require.Len(t, partitions, 1)
	})

	t.Run("1 boundary produces 2 partitions", func(t *testing.T) {
		b := makeBoundary("ns1", "oid1", "rel1", "uns1", "uoid1", "urel1")
		partitions := groupBoundaries([]dsoptions.Cursor{b}, 4)
		require.Len(t, partitions, 2)

		require.Nil(t, partitions[0].LowerBound)
		require.NotNil(t, partitions[0].UpperBound)
		require.NotNil(t, partitions[1].LowerBound)
		require.Nil(t, partitions[1].UpperBound)

		// Shared boundary.
		require.Equal(t,
			*dsoptions.ToRelationship(partitions[0].UpperBound),
			*dsoptions.ToRelationship(partitions[1].LowerBound),
		)
	})

	t.Run("3 boundaries produce 4 partitions when K=4", func(t *testing.T) {
		boundaries := []dsoptions.Cursor{
			makeBoundary("a", "0", "r", "u", "0", "..."),
			makeBoundary("b", "0", "r", "u", "0", "..."),
			makeBoundary("c", "0", "r", "u", "0", "..."),
		}
		partitions := groupBoundaries(boundaries, 4)
		require.Len(t, partitions, 4)

		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[3].UpperBound)

		// All adjacent boundaries match.
		for i := 1; i < len(partitions); i++ {
			require.Equal(t,
				*dsoptions.ToRelationship(partitions[i-1].UpperBound),
				*dsoptions.ToRelationship(partitions[i].LowerBound),
			)
		}
	})

	t.Run("K larger than boundaries+1 is capped", func(t *testing.T) {
		boundaries := []dsoptions.Cursor{
			makeBoundary("a", "0", "r", "u", "0", "..."),
		}
		partitions := groupBoundaries(boundaries, 100)
		require.Len(t, partitions, 2) // 1 boundary → max 2 partitions
	})

	t.Run("many boundaries with small K downsamples", func(t *testing.T) {
		boundaries := make([]dsoptions.Cursor, 20)
		for i := range boundaries {
			boundaries[i] = makeBoundary(
				fmt.Sprintf("ns%02d", i), "0", "r", "u", "0", "...",
			)
		}
		partitions := groupBoundaries(boundaries, 4)
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

	t.Run("empty boundaries returns single partition", func(t *testing.T) {
		partitions := groupBoundaries([]dsoptions.Cursor{}, 4)
		require.Len(t, partitions, 1)
		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[0].UpperBound)
	})
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
