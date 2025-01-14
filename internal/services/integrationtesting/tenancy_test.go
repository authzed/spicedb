//go:build ci && docker && !skipintegrationtests
// +build ci,docker,!skipintegrationtests

package integrationtesting_test

import (
	"context"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type simpleTestCase struct {
	schema string
	engine string
	ctx    func() context.Context
	runOp  func(t *testing.T, client v1.PermissionsServiceClient, ctx context.Context)
}

func TestRelationshipsWithTenantID(t *testing.T) {
	tests := map[string]simpleTestCase{
		"without tenant ID in ctx": {
			schema: `
				definition user {} 
				definition resource {
					relation parent: resource
					relation viewer: user
					permission can_view = viewer + parent->view
				}`,
			engine: postgres.Engine,
			ctx: func() context.Context {
				return context.Background()
			},
			runOp: func(t *testing.T, client v1.PermissionsServiceClient, ctx context.Context) {
				_, err := client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
					Updates: []*v1.RelationshipUpdate{
						{
							Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
							Relationship: tuple.ToV1Relationship(tuple.MustParse("resource:foo#viewer@user:tom")),
						},
						{
							Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
							Relationship: tuple.ToV1Relationship(tuple.MustParse("resource:foo#parent@resource:bar")),
						},
						{
							Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
							Relationship: tuple.ToV1Relationship(tuple.MustParse("resource:bar#viewer@user:jill")),
						},
					},
				})
				require.NoError(t, err)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			engine := testdatastore.RunDatastoreEngine(t, test.engine)
			ds := engine.NewDatastore(t, config.DatastoreConfigInitFunc(t,
				dsconfig.WithWatchBufferLength(0),
				dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
				dsconfig.WithRevisionQuantization(10),
				dsconfig.WithMaxRetries(50),
				dsconfig.WithRequestHedgingEnabled(false),
			))
			connections, cleanup := testserver.TestClusterWithDispatch(t, 1, ds)
			t.Cleanup(cleanup)

			schemaClient := v1.NewSchemaServiceClient(connections[0])
			_, err := schemaClient.WriteSchema(test.ctx(), &v1.WriteSchemaRequest{
				Schema: test.schema,
			})
			require.NoError(t, err)

			client := v1.NewPermissionsServiceClient(connections[0])
			test.runOp(t, client, test.ctx())
		})
	}
}
