//go:build datastore

package crdb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	dsoptions "github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
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

	rev, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	// Request plan.
	plan, err := v1.PlanPartitionedExport(ctx, ds, 4)
	require.NoError(t, err)
	require.Len(t, plan.Partitions, 4, "3 forced splits should produce 4 partitions")

	t.Logf("Plan returned %d partitions", len(plan.Partitions))

	// Stream each partition and collect all relationships.
	seen := make(map[string]int) // relationship key → partition index
	for pi, partition := range plan.Partitions {
		partitionCount := 0
		iter, err := v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
			Partition: partition,
			Revision:  rev.Revision,
		})
		require.NoError(t, err)
		for rel, err := range iter {
			require.NoError(t, err)
			key := tuple.MustString(rel)
			prevPartition, duplicate := seen[key]
			require.False(t, duplicate,
				"relationship %s appeared in both partition %d and %d", key, prevPartition, pi)
			seen[key] = pi
			partitionCount++
		}
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
				iter, err := v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
					Partition: v1.ExportPartition{
						LowerBound: tc.lower,
						UpperBound: tc.upper,
					},
					Revision: rev.Revision,
				})
				require.NoError(t, err)

				var totalRels int
				for _, err := range iter {
					require.NoError(t, err)
					totalRels++
				}
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
				iter, err := v1.StreamPartitionedExport(ctx, ds, v1.StreamRequest{
					Partition: partition,
					Revision:  rev.Revision,
				})
				require.NoError(t, err)
				for rel, err := range iter {
					require.NoError(t, err)
					key := tuple.MustString(rel)
					prevPI, dup := seen[key]
					require.False(t, dup, "relationship %s in both partition %d and %d", key, prevPI, pi)
					seen[key] = pi
				}
			}
			require.Len(t, seen, totalWritten, "two adjacent partitions should cover all relationships")
		})
	})
}

// TestCRDBSpecialCharEncodingInRangeKeys verifies exactly which characters CRDB
// backslash-escapes in SHOW RANGES pretty-printed keys, and that parseRangeStartKey
// round-trips all of them correctly. If CRDB changes its escaping behavior in a
// future version, this test will fail — alerting us to update extractQuotedValues.
func TestCRDBSpecialCharEncodingInRangeKeys(t *testing.T) {
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
	_ = ds // need the datastore to run migrations

	conn, err := pgx.Connect(ctx, connectStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Characters that CRDB backslash-escapes in pretty-printed range keys.
	backslashEscaped := []struct {
		name    string
		char    string // the raw character to embed in object_id
		escaped string // how CRDB renders it inside quotes in start_key
	}{
		{`double quote`, `"`, `\"`},
		{`backslash`, `\`, `\\`},
		{`newline`, "\n", `\n`},
		{`tab`, "\t", `\t`},
		{`null byte`, "\x00", `\x00`},
	}

	// Characters that CRDB does NOT escape — passed through literally.
	notEscaped := []struct {
		name string
		char string
	}{
		{`single quote`, `'`},
		{`slash`, `/`},
		{`pipe`, `|`},
		{`equals`, `=`},
		{`plus`, `+`},
		{`space`, ` `},
		{`percent`, `%`},
		{`at sign`, `@`},
		{`curly brace`, `{`},
		{`angle bracket`, `<`},
		{`unicode accent`, "\u00e9"},
		{`asterisk`, `*`},
		{`hyphen`, `-`},
		{`underscore`, `_`},
	}

	// Helper: insert a row, split at it, find the start_key containing the object_id,
	// and return the raw start_key string.
	getStartKeyForObjectID := func(t *testing.T, namespace, objectID string) string {
		t.Helper()
		_, err := conn.Exec(ctx,
			`INSERT INTO relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation) VALUES ($1, $2, 'viewer', 'user', 'alice', '...')`,
			namespace, objectID,
		)
		require.NoError(t, err)

		_, err = conn.Exec(ctx,
			`ALTER TABLE relation_tuple SPLIT AT VALUES ($1, $2, 'viewer', 'user', 'alice', '...')`,
			namespace, objectID,
		)
		require.NoError(t, err)

		rows, err := conn.Query(ctx, "SELECT start_key FROM [SHOW RANGES FROM INDEX relation_tuple@primary] ORDER BY start_key")
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var startKey string
			require.NoError(t, rows.Scan(&startKey))
			// Look for the key containing our namespace.
			if strings.Contains(startKey, `"`+namespace+`"`) {
				return startKey
			}
		}
		require.NoError(t, rows.Err())
		t.Fatalf("start_key containing namespace %q not found", namespace)
		return ""
	}

	// Characters that MUST be backslash-escaped.
	for _, tc := range backslashEscaped {
		t.Run("escaped/"+tc.name, func(t *testing.T) {
			ns := "esc_" + tc.name
			oid := "x" + tc.char + "y"

			startKey := getStartKeyForObjectID(t, ns, oid)
			t.Logf("start_key: %s", startKey)

			// Verify CRDB did backslash-escape this character.
			expectedInKey := `"x` + tc.escaped + `y"`
			require.Contains(t, startKey, expectedInKey,
				"CRDB should backslash-escape %s as %s in start_key", tc.name, tc.escaped)

			// Verify parseRangeStartKey round-trips correctly.
			cursor, err := parseRangeStartKey(startKey)
			require.NoError(t, err)
			rel := dsoptions.ToRelationship(cursor)
			require.Equal(t, oid, rel.Resource.ObjectID,
				"parseRangeStartKey should unescape %s correctly", tc.name)
		})
	}

	// Characters that must NOT be backslash-escaped.
	for _, tc := range notEscaped {
		t.Run("literal/"+tc.name, func(t *testing.T) {
			ns := "lit_" + tc.name
			oid := "x" + tc.char + "y"

			startKey := getStartKeyForObjectID(t, ns, oid)
			t.Logf("start_key: %s", startKey)

			// Verify CRDB did NOT backslash-escape this character — it appears literally.
			expectedInKey := `"x` + tc.char + `y"`
			require.Contains(t, startKey, expectedInKey,
				"CRDB should NOT backslash-escape %s — it should appear literally in start_key", tc.name)

			// Verify parseRangeStartKey round-trips correctly.
			cursor, err := parseRangeStartKey(startKey)
			require.NoError(t, err)
			rel := dsoptions.ToRelationship(cursor)
			require.Equal(t, oid, rel.Resource.ObjectID,
				"parseRangeStartKey should pass through %s literally", tc.name)
		})
	}
}

// TestCRDBExhaustiveEscapeScan scans all byte values 0-255 to verify that
// CRDB's backslash-escape set in SHOW RANGES matches our known set exactly.
// If CRDB adds or removes escaping for any byte, this test fails — alerting
// us to update extractQuotedValues.
func TestCRDBExhaustiveEscapeScan(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())
	ctx := t.Context()

	var connStr string
	ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		connStr = uri
		ds, err := NewCRDBDatastore(ctx, uri,
			GCWindow(veryLargeGCWindow),
			RevisionQuantization(0),
			WithAcquireTimeout(30*time.Second),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ds.Close() })
		return ds
	})
	_ = ds

	conn, err := pgx.Connect(ctx, connStr)
	require.NoError(t, err)
	defer conn.Close(ctx)

	// Characters that CRDB backslash-escapes in pretty-printed range keys:
	// all control characters (0x00-0x1F), DEL (0x7F), C1 control characters
	// (0x80-0x9F), non-breaking space (0xA0), soft hyphen (0xAD), double
	// quote, and backslash.
	knownEscaped := map[byte]bool{
		'"':  true,
		'\\': true,
		0x7F: true, // DEL
		0xA0: true, // non-breaking space
		0xAD: true, // soft hyphen
	}
	for c := byte(0); c <= 0x1F; c++ {
		knownEscaped[c] = true
	}
	for c := byte(0x80); c <= 0x9F; c++ {
		knownEscaped[c] = true
	}

	for i := 0; i <= 255; i++ {
		char := byte(i)
		ns := fmt.Sprintf("scan_%03d", i)
		oid := fmt.Sprintf("x%cy", char)

		_, err := conn.Exec(ctx,
			`INSERT INTO relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation) VALUES ($1, $2, 'viewer', 'user', 'alice', '...')`,
			ns, oid,
		)
		require.NoError(t, err, "INSERT failed for byte 0x%02x", char)

		_, err = conn.Exec(ctx,
			`ALTER TABLE relation_tuple SPLIT AT VALUES ($1, $2, 'viewer', 'user', 'alice', '...')`,
			ns, oid,
		)
		require.NoError(t, err, "SPLIT AT failed for byte 0x%02x", char)

		rows, err := conn.Query(ctx, "SELECT start_key FROM [SHOW RANGES FROM INDEX relation_tuple@primary] ORDER BY start_key")
		require.NoError(t, err)

		var startKey string
		for rows.Next() {
			var sk string
			require.NoError(t, rows.Scan(&sk))
			if strings.Contains(sk, `"`+ns+`"`) {
				startKey = sk
				break
			}
		}
		rows.Close()
		require.NoError(t, rows.Err(), "rows iteration error for byte 0x%02x", char)

		require.NotEmpty(t, startKey, "start_key not found for byte 0x%02x", char)

		literalForm := fmt.Sprintf(`"x%cy"`, char)
		isEscaped := !strings.Contains(startKey, literalForm)

		require.False(t, isEscaped && !knownEscaped[char],
			"byte 0x%02x (%q) is backslash-escaped by CRDB but not in our known set — add it to extractQuotedValues. start_key: %s",
			char, string([]byte{char}), startKey)
		require.False(t, !isEscaped && knownEscaped[char],
			"byte 0x%02x (%q) is in our known escaped set but CRDB did NOT escape it — remove it from extractQuotedValues. start_key: %s",
			char, string([]byte{char}), startKey)

		// Verify round-trip.
		cursor, err := parseRangeStartKey(startKey)
		require.NoError(t, err, "byte 0x%02x: parseRangeStartKey failed (start_key: %s)", char, startKey)
		rel := dsoptions.ToRelationship(cursor)
		require.Equal(t, oid, rel.Resource.ObjectID,
			"byte 0x%02x: round-trip failed (start_key: %s)", char, startKey)
	}
}

func TestExplainPartitionedQuery(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

	t.Run("compare query plans", func(t *testing.T) {
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

		// Write some data.
		rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			var updates []tuple.RelationshipUpdate
			for i := 0; i < 100; i++ {
				updates = append(updates, tuple.Create(tuple.MustParse(
					fmt.Sprintf("resource:%d#viewer@user:%d", i, i),
				)))
			}
			return rwt.WriteRelationships(ctx, updates)
		})
		require.NoError(t, err)

		afterCursor := dsoptions.ToCursor(tuple.MustParse("resource:10#viewer@user:10"))
		beforeCursor := dsoptions.ToCursor(tuple.MustParse("resource:90#viewer@user:90"))
		reader := ds.SnapshotReader(rev)

		tests := []struct {
			name           string
			opts           []dsoptions.QueryOptionsOption
			expectScan     bool
			expectDistinct bool
			expectUnion    bool
		}{
			{
				name: "EXPANDED (After + BeforeOrEqual)",
				opts: []dsoptions.QueryOptionsOption{
					dsoptions.WithSort(dsoptions.ByResource),
					dsoptions.WithQueryShape(queryshape.Varying),
					dsoptions.WithAfter(afterCursor),
					dsoptions.WithBeforeOrEqual(beforeCursor),
				},
				expectScan: true,
			},
			{
				name: "TUPLE COMPARISON (After + BeforeOrEqual)",
				opts: []dsoptions.QueryOptionsOption{
					dsoptions.WithSort(dsoptions.ByResource),
					dsoptions.WithQueryShape(queryshape.Varying),
					dsoptions.WithAfter(afterCursor),
					dsoptions.WithBeforeOrEqual(beforeCursor),
					dsoptions.WithUseTupleComparison(true),
				},
				expectScan:     true,
				expectDistinct: false,
				expectUnion:    false,
			},
			{
				name: "EXPANDED (After only - BulkExport style)",
				opts: []dsoptions.QueryOptionsOption{
					dsoptions.WithSort(dsoptions.ByResource),
					dsoptions.WithQueryShape(queryshape.Varying),
					dsoptions.WithAfter(afterCursor),
				},
				expectScan: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var capturedSQL string
				var capturedExplain string

				explainOpts := make([]dsoptions.QueryOptionsOption, 0, len(tc.opts)+1)
				explainOpts = append(explainOpts, tc.opts...)
				explainOpts = append(explainOpts, dsoptions.WithSQLExplainCallbackForTest(
					func(_ context.Context, sql string, _ []any, _ queryshape.Shape, explain string, _ dsoptions.SQLIndexInformation) error {
						capturedSQL = sql
						capturedExplain = explain
						return nil
					},
				))

				iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{}, explainOpts...)
				require.NoError(t, err)
				for _, err := range iter {
					require.NoError(t, err)
				}

				require.NotEmpty(t, capturedSQL, "SQL was not captured")
				require.NotEmpty(t, capturedExplain, "EXPLAIN was not captured")

				t.Logf("SQL: %s", capturedSQL)
				t.Logf("EXPLAIN:\n%s", capturedExplain)

				if tc.expectScan {
					require.Contains(t, capturedExplain, "scan", "expected index scan in query plan")
				}
				if tc.expectDistinct {
					require.Contains(t, capturedExplain, "distinct", "expected distinct in query plan")
				} else {
					require.NotContains(t, capturedExplain, "distinct", "unexpected distinct in query plan")
				}
				if tc.expectUnion {
					require.Contains(t, capturedExplain, "union", "expected union in query plan")
				} else {
					require.NotContains(t, capturedExplain, "union", "unexpected union in query plan")
				}
			})
		}
	})
}
