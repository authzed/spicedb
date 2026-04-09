//go:build ci && docker

package crdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestExplainPartitionedQuery(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

	t.Run("compare query plans", func(t *testing.T) {
		ctx := context.Background()

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

		afterCursor := options.ToCursor(tuple.MustParse("resource:10#viewer@user:10"))
		beforeCursor := options.ToCursor(tuple.MustParse("resource:90#viewer@user:90"))
		reader := ds.SnapshotReader(rev)

		tests := []struct {
			name           string
			opts           []options.QueryOptionsOption
			expectScan     bool
			expectDistinct bool
			expectUnion    bool
		}{
			{
				name: "EXPANDED (After + BeforeOrEqual)",
				opts: []options.QueryOptionsOption{
					options.WithSort(options.ByResource),
					options.WithQueryShape(queryshape.Varying),
					options.WithAfter(afterCursor),
					options.WithBeforeOrEqual(beforeCursor),
				},
				expectScan: true,
			},
			{
				name: "TUPLE COMPARISON (After + BeforeOrEqual)",
				opts: []options.QueryOptionsOption{
					options.WithSort(options.ByResource),
					options.WithQueryShape(queryshape.Varying),
					options.WithAfter(afterCursor),
					options.WithBeforeOrEqual(beforeCursor),
					options.WithUseTupleComparison(true),
				},
				expectScan:     true,
				expectDistinct: false,
				expectUnion:    false,
			},
			{
				name: "EXPANDED (After only - BulkExport style)",
				opts: []options.QueryOptionsOption{
					options.WithSort(options.ByResource),
					options.WithQueryShape(queryshape.Varying),
					options.WithAfter(afterCursor),
				},
				expectScan: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var capturedSQL string
				var capturedExplain string

				explainOpts := append(tc.opts, options.WithSQLExplainCallbackForTest(
					func(_ context.Context, sql string, _ []any, _ queryshape.Shape, explain string, _ options.SQLIndexInformation) error {
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
