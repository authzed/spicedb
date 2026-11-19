//go:build datastore && postgres
// +build datastore,postgres

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const poolNestingSchema = `
	caveat allowed(flag bool) {
		flag
	}

	definition user {}

	definition group {
		relation member: user
	}

	definition document {
		relation viewer: user with allowed | group#member
		permission view = viewer
	}
`

// TestLookupResourcesWithSingleReadConnection covers
// https://github.com/authzed/spicedb/issues/3433 against a real pgxpool: with a single
// read connection, LookupResources must never wait for a second read connection while a
// relationships query still holds the first.
//
// The schema is read from the single-store schema, bypassing the schema cache, so that
// every schema read goes to the datastore.
func TestLookupResourcesWithSingleReadConnection(t *testing.T) {
	b := testdatastore.RunPostgresForTesting(t, postgresConfig.pgVersion, postgresConfig.pgbouncer)

	// More groups than LookupResources reads from the datastore per page (10 chunks of 100).
	const fanoutGroupCount = 1050
	fanout := make([]tuple.Relationship, 0, 2*fanoutGroupCount)
	for i := range fanoutGroupCount {
		fanout = append(fanout,
			tuple.MustParse(fmt.Sprintf("group:g%d#member@user:bob", i)),
			tuple.MustParse(fmt.Sprintf("document:fanout#viewer@group:g%d#member", i)),
		)
	}

	tcs := []struct {
		name          string
		relationships []tuple.Relationship
		subjectID     string
		expected      string
	}{
		{
			"caveated relationship",
			[]tuple.Relationship{tuple.MustParse(`document:one#viewer@user:alice[allowed:{"flag":true}]`)},
			"alice",
			"one",
		},
		{
			"fanout larger than a datastore page",
			fanout,
			"bob",
			"fanout",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// Migrate a fresh database, then open our own datastore over it: the datastore
			// returned by NewDatastore is closed in a test cleanup, which blocks forever on
			// a deadlocked pool.
			var uri string
			b.NewDatastore(t, func(engine, migratedURI string) datastore.Datastore {
				uri = migratedURI
				ds, err := NewPostgresDatastore(t.Context(), migratedURI)
				require.NoError(t, err)
				return ds
			})

			ds, err := NewPostgresDatastore(context.Background(), uri,
				RevisionQuantization(0),
				ReadConnsMinOpen(1),
				ReadConnsMaxOpen(1),
			)
			require.NoError(t, err)

			_, err = datalayer.WriteStoredSchemaForTest(t.Context(), ds, poolNestingSchema)
			require.NoError(t, err)

			revision, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				mutations := make([]tuple.RelationshipUpdate, 0, len(tc.relationships))
				for _, rel := range tc.relationships {
					mutations = append(mutations, tuple.Create(rel))
				}
				return rwt.WriteRelationships(ctx, mutations)
			})
			require.NoError(t, err)

			params := graph.MustNewDefaultDispatcherParametersForTesting()
			params.ConcurrencyLimits = graph.SharedConcurrencyLimits(1)
			dispatcher, err := graph.NewLocalOnlyDispatcher(params)
			require.NoError(t, err)

			caveatContext, err := structpb.NewStruct(map[string]any{"flag": true})
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(datalayer.ContextWithHandle(t.Context()), 30*time.Second)
			defer cancel()
			require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteNew))))

			stream := dispatch.NewCloningCollectingDispatchStream[*v1.DispatchLookupResources3Response](ctx)
			done := make(chan error, 1)
			go func() {
				done <- dispatcher.DispatchLookupResources3(&v1.DispatchLookupResources3Request{
					ResourceRelation: &core.RelationReference{Namespace: "document", Relation: "view"},
					SubjectRelation:  &core.RelationReference{Namespace: "user", Relation: tuple.Ellipsis},
					SubjectIds:       []string{tc.subjectID},
					TerminalSubject:  tuple.CoreONR("user", tc.subjectID, tuple.Ellipsis),
					Metadata: &v1.ResolverMeta{
						AtRevision:     revision.String(),
						DepthRemaining: 50,
						SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
					},
					Context: caveatContext,
				}, stream)
			}()

			select {
			case err := <-done:
				require.NoError(t, err)
				found := map[string]struct{}{}
				for _, result := range stream.Results() {
					for _, item := range result.Items {
						found[item.ResourceId] = struct{}{}
					}
				}
				require.Equal(t, map[string]struct{}{tc.expected: {}}, found)
				require.NoError(t, dispatcher.Close())
				require.NoError(t, ds.Close())

			case <-time.After(60 * time.Second):
				// The blocked acquisition runs under a context with cancellation removed, so
				// neither the request deadline above nor closing the pool can unblock it. Fail
				// without cleanup rather than hanging the test binary.
				t.Fatal("LookupResources deadlocked waiting for a second read connection while holding the only one (#3433)")
			}
		})
	}
}
