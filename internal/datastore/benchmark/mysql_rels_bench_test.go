package benchmark

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/mysql"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	mysqlBenchNumDocuments = 5000
	mysqlBenchUsersPerDoc  = 10
)

// BenchmarkMySQLRelQueries benchmarks the relationship-query shapes affected by the
// MySQL index-hint / pagination work. Requires Docker (spins up mysql:8.4).
//
// Run with:
//
//	go test ./internal/datastore/benchmark/ -bench BenchmarkMySQLRelQueries \
//	    -benchmem -run '^$' -timeout 30m
func BenchmarkMySQLRelQueries(b *testing.B) {
	b.StopTimer()
	ctx := b.Context()

	engine := testdatastore.RunDatastoreEngine(b, mysql.Engine)
	ds := engine.NewDatastore(b, config.DatastoreConfigInitFunc(
		b,
		dsconfig.WithRevisionQuantization(5*time.Second),
		dsconfig.WithGCWindow(2*time.Hour),
		dsconfig.WithGCInterval(1*time.Hour),
		dsconfig.WithWatchBufferLength(1000),
		dsconfig.WithWriteAcquisitionTimeout(5*time.Second),
	))
	b.Cleanup(func() { _ = ds.Close() })

	ds, _ = testfixtures.StandardDatastoreWithSchema(b, ds)

	// Write a meaningful amount of data. Subject user "0" ends up a viewer of every
	// document, which exercises the reverse (MatchingResourcesForSubject) shape well.
	for docNum := range mysqlBenchNumDocuments {
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			updates := make([]tuple.RelationshipUpdate, 0, mysqlBenchUsersPerDoc)
			for userNum := range mysqlBenchUsersPerDoc {
				updates = append(updates, tuple.Create(docViewer(strconv.Itoa(docNum), strconv.Itoa(userNum))))
			}
			return rwt.WriteRelationships(ctx, updates)
		})
		require.NoError(b, err)
	}

	time.Sleep(1 * time.Second)

	headRevResult, err := ds.HeadRevision(ctx)
	require.NoError(b, err)
	headRev := headRevResult.Revision

	drain := func(b *testing.B, iter datastore.RelationshipIterator) int {
		count := 0
		for _, err := range iter {
			require.NoError(b, err)
			count++
		}
		return count
	}

	b.StartTimer()

	// Forward: full resource + subject (direct check).
	b.Run("ForwardDirectCheck", func(b *testing.B) {
		for range b.N {
			iter, err := ds.SnapshotReader(headRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType:     testfixtures.DocumentNS.Name,
				OptionalResourceIds:      []string{"100"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
					OptionalSubjectType: testfixtures.UserNS.Name,
					OptionalSubjectIds:  []string{"0"},
				}},
			}, options.WithQueryShape(queryshape.CheckPermissionSelectDirectSubjects))
			require.NoError(b, err)
			require.Equal(b, 1, drain(b, iter))
		}
	})

	// Forward: all subjects for one resource.
	b.Run("ForwardAllSubjects", func(b *testing.B) {
		for range b.N {
			iter, err := ds.SnapshotReader(headRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType:     testfixtures.DocumentNS.Name,
				OptionalResourceIds:      []string{"100"},
				OptionalResourceRelation: "viewer",
			}, options.WithQueryShape(queryshape.AllSubjectsForResources))
			require.NoError(b, err)
			require.Equal(b, mysqlBenchUsersPerDoc, drain(b, iter))
		}
	})

	// Reverse: all resources for a specific subject (Finding 1 main target).
	b.Run("ReverseMatchingResources", func(b *testing.B) {
		for range b.N {
			iter, err := ds.SnapshotReader(headRev).ReverseQueryRelationships(ctx, datastore.SubjectsFilter{
				SubjectType:        testfixtures.UserNS.Name,
				OptionalSubjectIds: []string{"0"},
			},
				options.WithResRelation(&options.ResourceRelation{
					Namespace: testfixtures.DocumentNS.Name,
					Relation:  "viewer",
				}),
				options.WithSortForReverse(options.BySubject),
				options.WithQueryShapeForReverse(queryshape.MatchingResourcesForSubject),
			)
			require.NoError(b, err)
			require.Equal(b, mysqlBenchNumDocuments, drain(b, iter))
		}
	})

	// Enumerate by type + relation, no object_id (Finding 3 candidate workload).
	b.Run("EnumerateByTypeAndRelation", func(b *testing.B) {
		for range b.N {
			iter, err := ds.SnapshotReader(headRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
				OptionalResourceType:     testfixtures.DocumentNS.Name,
				OptionalResourceRelation: "viewer",
			}, options.WithQueryShape(queryshape.Varying))
			require.NoError(b, err)
			require.Equal(b, mysqlBenchNumDocuments*mysqlBenchUsersPerDoc, drain(b, iter))
		}
	})

	// Cursor-paginated read (Finding 4 target).
	b.Run("PaginatedRead", func(b *testing.B) {
		const pageSize = uint64(500)
		for range b.N {
			var after options.Cursor
			total := 0
			for {
				limit := pageSize
				opts := []options.QueryOptionsOption{
					options.WithSort(options.ByResource),
					options.WithLimit(&limit),
					options.WithQueryShape(queryshape.Varying),
				}
				if after != nil {
					opts = append(opts, options.WithAfter(after))
				}
				iter, err := ds.SnapshotReader(headRev).QueryRelationships(ctx, datastore.RelationshipsFilter{
					OptionalResourceType:     testfixtures.DocumentNS.Name,
					OptionalResourceRelation: "viewer",
				}, opts...)
				require.NoError(b, err)

				pageCount := 0
				var last tuple.Relationship
				for rel, err := range iter {
					require.NoError(b, err)
					last = rel
					pageCount++
				}
				total += pageCount
				if uint64(pageCount) < pageSize {
					break
				}
				lastCopy := last
				after = &lastCopy
			}
			require.Equal(b, mysqlBenchNumDocuments*mysqlBenchUsersPerDoc, total)
		}
	})
}
