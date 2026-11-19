//go:build datastore && postgres
// +build datastore,postgres

package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// BenchmarkLookupResources3Fanout measures LookupResources over a single subject with a large
// number of directly related resources, which LookupResources reads from the datastore in pages.
func BenchmarkLookupResources3Fanout(b *testing.B) {
	engine := testdatastore.RunPostgresForTesting(b, postgresConfig.pgVersion, postgresConfig.pgbouncer)

	for _, resourceCount := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("resources=%d", resourceCount), func(b *testing.B) {
			ds := engine.NewDatastore(b, func(_, uri string) datastore.Datastore {
				ds, err := NewPostgresDatastore(b.Context(), uri, RevisionQuantization(0))
				require.NoError(b, err)
				return ds
			})

			_, err := datalayer.WriteStoredSchemaForTest(b.Context(), ds, `
				definition user {}

				definition document {
					relation viewer: user
					permission view = viewer
				}
			`)
			require.NoError(b, err)

			var revision datastore.Revision
			const writeBatchSize = 5_000
			for start := 0; start < resourceCount; start += writeBatchSize {
				revision, err = ds.ReadWriteTx(b.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					mutations := make([]tuple.RelationshipUpdate, 0, writeBatchSize)
					for i := start; i < min(start+writeBatchSize, resourceCount); i++ {
						mutations = append(mutations, tuple.Create(tuple.MustParse(fmt.Sprintf("document:doc%d#viewer@user:tom", i))))
					}
					return rwt.WriteRelationships(ctx, mutations)
				})
				require.NoError(b, err)
			}

			head, err := ds.HeadRevision(b.Context())
			require.NoError(b, err)

			params := graph.MustNewDefaultDispatcherParametersForTesting()
			dispatcher, err := graph.NewLocalOnlyDispatcher(params)
			require.NoError(b, err)
			b.Cleanup(func() { _ = dispatcher.Close() })

			ctx := datalayer.ContextWithHandle(b.Context())
			require.NoError(b, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteNew))))

			for _, limit := range []uint32{0, 100} {
				b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
					expectedCount := resourceCount
					if limit > 0 {
						expectedCount = int(limit)
					}

					for b.Loop() {
						stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
						err := dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
							ResourceRelation: &core.RelationReference{Namespace: "document", Relation: "view"},
							SubjectRelation:  &core.RelationReference{Namespace: "user", Relation: tuple.Ellipsis},
							SubjectIds:       []string{"tom"},
							TerminalSubject:  tuple.CoreONR("user", "tom", tuple.Ellipsis),
							Metadata: &v1.ResolverMeta{
								AtRevision:     revision.String(),
								DepthRemaining: 50,
								SchemaHash:     []byte(head.SchemaHash),
							},
							OptionalLimit: limit,
						}, stream)
						require.NoError(b, err)

						count := 0
						for _, result := range stream.Results() {
							count += len(result.Items)
						}
						require.Equal(b, expectedCount, count)
					}
				})
			}
		})
	}
}
