package benchmarks

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/pkg/datalayer"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func BenchmarkLookupSubjectsWildcardExclusion(b *testing.B) {
	for _, count := range []int{0, 1, 10, 100, 1000, 5000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			ctx := b.Context()
			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(b, err)
			b.Cleanup(func() { _ = ds.Close() })
			_, err = datalayer.WriteStoredSchemaForTest(ctx, ds, `
                definition user {}
                definition document {
                    relation public: user:*
                    relation banned: user
                    permission view = public - banned
                }
            `)
			require.NoError(b, err)
			rels := []tuple.Relationship{tuple.MustParse("document:doc#public@user:*")}
			for i := range count {
				rels = append(rels, tuple.MustParse(fmt.Sprintf("document:doc#banned@user:u%d", i)))
			}
			_, err = common.WriteRelationships(ctx, ds, tuple.UpdateOperationCreate, rels...)
			require.NoError(b, err)
			rev, err := ds.HeadRevision(ctx)
			require.NoError(b, err)
			dispatcher, err := graph.NewLocalOnlyDispatcher(graph.MustNewDefaultDispatcherParametersForTesting())
			require.NoError(b, err)
			b.Cleanup(func() { _ = dispatcher.Close() })
			ctx = datalayer.ContextWithDataLayer(ctx, datalayer.NewDataLayer(ds))
			b.ReportAllocs()
			b.ResetTimer()
			var lastResults []*v1.DispatchLookupSubjectsResponse
			for b.Loop() {
				stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
				err := dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
					ResourceRelation: &core.RelationReference{Namespace: "document", Relation: "view"},
					ResourceIds:      []string{"doc"},
					SubjectRelation:  &core.RelationReference{Namespace: "user", Relation: tuple.Ellipsis},
					Metadata: &v1.ResolverMeta{
						AtRevision: rev.Revision.String(), DepthRemaining: 50, SchemaHash: []byte(rev.SchemaHash),
					},
				}, stream)
				require.NoError(b, err)
				lastResults = stream.Results()
				found := 0
				for _, result := range stream.Results() {
					for _, subject := range result.FoundSubjectsByResourceId["doc"].FoundSubjects {
						require.Equal(b, "*", subject.SubjectId)
						require.Len(b, subject.ExcludedSubjects, count)
						found++
					}
				}
				require.Equal(b, 1, found)
			}
			seen := make(map[string]bool, count)
			for _, result := range lastResults {
				for _, subject := range result.FoundSubjectsByResourceId["doc"].FoundSubjects {
					require.Nil(b, subject.CaveatExpression)
					for _, exclusion := range subject.ExcludedSubjects {
						require.False(b, seen[exclusion.SubjectId])
						seen[exclusion.SubjectId] = true
						require.Nil(b, exclusion.CaveatExpression)
						require.Empty(b, exclusion.ExcludedSubjects)
					}
				}
			}
			for i := range count {
				require.True(b, seen[fmt.Sprintf("u%d", i)])
			}
		})
	}
}
