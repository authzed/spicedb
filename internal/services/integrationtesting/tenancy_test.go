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
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"io"
	"testing"
	"time"
)

func TestReadRelationships(t *testing.T) {
	tests := map[string]struct {
		tenantID            string
		readFromSameTenant  bool
		assertRelationships func(t *testing.T, relationships []*v1.Relationship)
	}{
		"same tenant": {
			tenantID:           uuid.Must(uuid.NewV4()).String(),
			readFromSameTenant: true,
			assertRelationships: func(t *testing.T, relationships []*v1.Relationship) {
				assert.Len(t, relationships, 3)
			},
		},
		"other tenant": {
			tenantID:           uuid.Must(uuid.NewV4()).String(),
			readFromSameTenant: false,
			assertRelationships: func(t *testing.T, relationships []*v1.Relationship) {
				assert.Len(t, relationships, 0)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			testTenantCtx, client := setupTenancyTest(t, test.tenantID)
			readCtx := testTenantCtx
			if !test.readFromSameTenant {
				otherTenantMd := metadata.Pairs("tenantID", uuid.Must(uuid.NewV4()).String())
				readCtx = metadata.NewOutgoingContext(context.Background(), otherTenantMd)
			}
			readResponse, err := client.ReadRelationships(readCtx, &v1.ReadRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType: "resource",
				},
			})
			require.NoError(t, err)
			relationships := make([]*v1.Relationship, 0)
			for {
				recv, recvErr := readResponse.Recv()
				if recvErr != nil {
					if recvErr == io.EOF {
						break
					} else {
						require.NoError(t, recvErr)
					}
				}
				relationships = append(relationships, recv.Relationship)
			}
			test.assertRelationships(t, relationships)
		})
	}
}

func TestCheckPermission(t *testing.T) {
	tests := map[string]struct {
		tenantID           string
		readFromSameTenant bool
	}{
		"same tenant": {
			tenantID:           uuid.Must(uuid.NewV4()).String(),
			readFromSameTenant: true,
		},
		"other tenant": {
			tenantID:           uuid.Must(uuid.NewV4()).String(),
			readFromSameTenant: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			testTenantCtx, client := setupTenancyTest(t, test.tenantID)
			readCtx := testTenantCtx
			if !test.readFromSameTenant {
				otherTenantMd := metadata.Pairs("tenantID", uuid.Must(uuid.NewV4()).String())
				readCtx = metadata.NewOutgoingContext(context.Background(), otherTenantMd)
			}
			response, err := client.CheckPermission(readCtx, &v1.CheckPermissionRequest{
				Resource: &v1.ObjectReference{
					ObjectType: "resource",
					ObjectId:   "foo",
				},
				Permission: "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{
						ObjectType: "user",
						ObjectId:   "tom",
					},
				},
			})
			require.NoError(t, err)
			require.NotNil(t, response)
			permissionShip := v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION
			if !test.readFromSameTenant {
				permissionShip = v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION
			}
			assert.Equal(t, permissionShip, response.GetPermissionship())
		})
	}
}

func setupTenancyTest(t *testing.T, tenantID string) (context.Context, v1.PermissionsServiceClient) {
	b := testdatastore.RunDatastoreEngine(t, postgres.Engine)
	ds := b.NewDatastore(t, config.DatastoreConfigInitFunc(t,
		dsconfig.WithWatchBufferLength(0),
		dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
		dsconfig.WithRevisionQuantization(10),
		dsconfig.WithMaxRetries(50),
		dsconfig.WithRequestHedgingEnabled(false)))

	connections, cleanup := testserver.TestClusterWithDispatch(t, 1, ds)
	t.Cleanup(cleanup)

	schemaClient := v1.NewSchemaServiceClient(connections[0])
	_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
		Schema: `
			definition user {}
			definition resource {
				relation parent: resource
				relation viewer: user
				permission view = viewer + parent->view
			}`,
	})
	require.NoError(t, err)

	client := v1.NewPermissionsServiceClient(connections[0])

	testTenantMd := metadata.Pairs("tenantID", tenantID)
	testTenantCtx := metadata.NewOutgoingContext(context.Background(), testTenantMd)

	_, err = client.WriteRelationships(testTenantCtx, &v1.WriteRelationshipsRequest{
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

	return testTenantCtx, client
}
