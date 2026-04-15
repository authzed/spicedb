package v1_test

import (
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

func TestAllMethods(t *testing.T) {
	req := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(req, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	t.Cleanup(cleanup)

	ctx := t.Context()

	checkServiceMethods(
		t,
		v1.NewPermissionsServiceClient(conn),
		map[string]func(t *testing.T, client v1.PermissionsServiceClient){
			"CheckPermission": func(t *testing.T, client v1.PermissionsServiceClient) {
				_, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision),
						},
					},
					Resource:   obj("document", "masterplan"),
					Permission: "view",
					Subject:    sub("user", "eng_lead", ""),
				})
				require.NoError(t, err)
			},
			"CheckBulkPermissions": func(t *testing.T, client v1.PermissionsServiceClient) {
				_, err := client.CheckBulkPermissions(ctx, &v1.CheckBulkPermissionsRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision),
						},
					},
					Items: []*v1.CheckBulkPermissionsRequestItem{{
						Resource:   obj("document", "masterplan"),
						Permission: "view",
						Subject:    sub("user", "eng_lead", ""),
					}},
				})
				require.NoError(t, err)
			},
			"DeleteRelationships": func(t *testing.T, client v1.PermissionsServiceClient) {
				_, err := client.DeleteRelationships(ctx, &v1.DeleteRelationshipsRequest{
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType:       "folder",
						OptionalResourceId: "somefolder",
					},
				})
				require.NoError(t, err)
			},
			"WriteRelationships": func(t *testing.T, client v1.PermissionsServiceClient) {
				_, err := client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
					Updates: []*v1.RelationshipUpdate{{
						Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
						Relationship: tuple.ToV1Relationship(tuple.MustParse("document:anotherdoc#viewer@user:tom")),
					}},
				})
				require.NoError(t, err)
			},
			"ExpandPermissionTree": func(t *testing.T, client v1.PermissionsServiceClient) {
				_, err := client.ExpandPermissionTree(ctx, &v1.ExpandPermissionTreeRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision),
						},
					},
					Resource:   obj("document", "masterplan"),
					Permission: "view",
				})
				require.NoError(t, err)
			},
			"ReadRelationships": func(t *testing.T, client v1.PermissionsServiceClient) {
				stream, err := client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
					RelationshipFilter: &v1.RelationshipFilter{ResourceType: "folder"},
				})
				require.NoError(t, err)
				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
				}
			},
			"LookupResources": func(t *testing.T, client v1.PermissionsServiceClient) {
				stream, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision),
						},
					},
					ResourceObjectType: "document",
					Permission:         "view",
					Subject:            sub("user", "tom", ""),
				})
				require.NoError(t, err)
				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
				}
			},
			"LookupSubjects": func(t *testing.T, client v1.PermissionsServiceClient) {
				stream, err := client.LookupSubjects(ctx, &v1.LookupSubjectsRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision),
						},
					},
					Resource:          obj("document", "masterplan"),
					Permission:        "view",
					SubjectObjectType: "user",
				})
				require.NoError(t, err)
				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
				}
			},
			"ImportBulkRelationships": func(t *testing.T, client v1.PermissionsServiceClient) {
				writer, err := client.ImportBulkRelationships(ctx)
				require.NoError(t, err)
				_, err = writer.CloseAndRecv()
				require.NoError(t, err)
			},
			"ExportBulkRelationships": func(t *testing.T, client v1.PermissionsServiceClient) {
				stream, err := client.ExportBulkRelationships(ctx, &v1.ExportBulkRelationshipsRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision),
						},
					},
				})
				require.NoError(t, err)
				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
				}
			},
		},
	)

	checkServiceMethods(
		t,
		v1.NewSchemaServiceClient(conn),
		map[string]func(t *testing.T, client v1.SchemaServiceClient){
			"ReadSchema": func(t *testing.T, client v1.SchemaServiceClient) {
				_, err := client.ReadSchema(ctx, &v1.ReadSchemaRequest{})
				require.NoError(t, err)
			},
			"WriteSchema": func(t *testing.T, client v1.SchemaServiceClient) {
				resp, err := client.ReadSchema(ctx, &v1.ReadSchemaRequest{})
				require.NoError(t, err)
				_, err = client.WriteSchema(ctx, &v1.WriteSchemaRequest{
					Schema: resp.SchemaText + "\ndefinition foo {}",
				})
				require.NoError(t, err)
			},
		},
	)
}

func checkServiceMethods[T any](
	t *testing.T,
	client T,
	handlers map[string]func(t *testing.T, client T),
) {
	et := reflect.TypeFor[T]()
	for i := 0; i < et.NumMethod(); i++ {
		methodName := et.Method(i).Name
		t.Run(methodName, func(t *testing.T) {
			handler, ok := handlers[methodName]
			if !ok {
				return
			}
			require.True(t, ok, "missing handler for method %s under %T", methodName, new(T))
			handler(t, client)
		})
	}
}
