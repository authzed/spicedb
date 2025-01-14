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
	metadata "google.golang.org/grpc/metadata"
	"testing"
	"time"
)

type simpleTestCase struct {
	schema string
	engine string
	runOp  func(t *testing.T, client v1.PermissionsServiceClient)
}

func TestRelationshipsWithTenantID(t *testing.T) {
	tests := map[string]simpleTestCase{
		"write and read from same tenant": {
			schema: `
				definition user {}
				definition resource {
					relation parent: resource
					relation viewer: user
					permission can_view = viewer + parent->view
				}`,
			engine: postgres.Engine,
			runOp: func(t *testing.T, client v1.PermissionsServiceClient) {
				testTenantMd := metadata.Pairs("tenantID", "test-tenant")
				testTenantCtx := metadata.NewOutgoingContext(context.Background(), testTenantMd)

				_, err := client.WriteRelationships(testTenantCtx, &v1.WriteRelationshipsRequest{
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
				_, err = client.ReadRelationships(testTenantCtx, &v1.ReadRelationshipsRequest{
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType:       "resource",
						OptionalResourceId: "foo",
					},
				})
				require.NoError(t, err)
			},
		},
		"write and read from different tenants": {
			schema: `
				definition user {}
				definition resource {
					relation parent: resource
					relation viewer: user
					permission can_view = viewer + parent->view
				}`,
			engine: postgres.Engine,
			runOp: func(t *testing.T, client v1.PermissionsServiceClient) {
				testTenantMd := metadata.Pairs("tenantID", "test-tenant")
				testTenantCtx := metadata.NewOutgoingContext(context.Background(), testTenantMd)

				_, err := client.WriteRelationships(testTenantCtx, &v1.WriteRelationshipsRequest{
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

				otherTenantMd := metadata.Pairs("tenantID", "other-tenant")
				otherTenantCtx := metadata.NewOutgoingContext(context.Background(), otherTenantMd)

				readResponse, err := client.ReadRelationships(otherTenantCtx, &v1.ReadRelationshipsRequest{
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType:       "resource",
						OptionalResourceId: "foo",
					},
				})
				require.NoError(t, err)
				require.NotNil(t, readResponse)
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
			_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
				Schema: test.schema,
			})
			require.NoError(t, err)

			client := v1.NewPermissionsServiceClient(connections[0])
			test.runOp(t, client)
		})
	}
}
