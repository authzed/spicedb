package v1_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/authzed/authzed-go/pkg/responsemeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

func TestAllMethodsReturnMetadata(t *testing.T) {
	req := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(req, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// PermissionsService
	checkServiceMethods(
		t,
		v1.NewPermissionsServiceClient(conn),
		map[string]func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD{
			"CheckPermission": func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD {
				var trailer metadata.MD
				_, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
						},
					},
					Resource:   obj("document", "masterplan"),
					Permission: "view",
					Subject:    sub("user", "eng_lead", ""),
				}, grpc.Trailer(&trailer))
				require.NoError(t, err)
				return trailer
			},
			"CheckBulkPermissions": func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD {
				var trailer metadata.MD
				_, err := client.CheckBulkPermissions(ctx, &v1.CheckBulkPermissionsRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
						},
					},
					Items: []*v1.CheckBulkPermissionsRequestItem{
						{
							Resource:   obj("document", "masterplan"),
							Permission: "view",
							Subject:    sub("user", "eng_lead", ""),
						},
					},
				}, grpc.Trailer(&trailer))
				require.NoError(t, err)
				return trailer
			},
			"DeleteRelationships": func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD {
				var trailer metadata.MD
				_, err := client.DeleteRelationships(ctx, &v1.DeleteRelationshipsRequest{
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType:       "folder",
						OptionalResourceId: "somefolder",
					},
				}, grpc.Trailer(&trailer))
				require.NoError(t, err)
				return trailer
			},
			"WriteRelationships": func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD {
				var trailer metadata.MD
				_, err := client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
					Updates: []*v1.RelationshipUpdate{
						{
							Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
							Relationship: tuple.ToV1Relationship(tuple.MustParse("document:anotherdoc#viewer@user:tom")),
						},
					},
				}, grpc.Trailer(&trailer))
				require.NoError(t, err)
				return trailer
			},
			"ExpandPermissionTree": func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD {
				var trailer metadata.MD
				_, err := client.ExpandPermissionTree(ctx, &v1.ExpandPermissionTreeRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
						},
					},
					Resource:   obj("document", "masterplan"),
					Permission: "view",
				}, grpc.Trailer(&trailer))
				require.NoError(t, err)
				return trailer
			},
			"ReadRelationships": func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD {
				var trailer metadata.MD
				stream, err := client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType: "folder",
					},
				}, grpc.Trailer(&trailer))
				require.NoError(t, err)

				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}

					require.NoError(t, err)
				}

				return trailer
			},
			"LookupResources": func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD {
				var trailer metadata.MD
				stream, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
						},
					},
					ResourceObjectType: "document",
					Permission:         "view",
					Subject:            sub("user", "tom", ""),
				}, grpc.Trailer(&trailer))
				require.NoError(t, err)

				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}

					require.NoError(t, err)
				}

				return trailer
			},
			"LookupSubjects": func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD {
				var trailer metadata.MD
				stream, err := client.LookupSubjects(ctx, &v1.LookupSubjectsRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
						},
					},
					Resource:          obj("document", "masterplan"),
					Permission:        "view",
					SubjectObjectType: "user",
				}, grpc.Trailer(&trailer))
				require.NoError(t, err)

				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}

					require.NoError(t, err)
				}

				return trailer
			},
			"ImportBulkRelationships": func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD {
				var trailer metadata.MD
				writer, err := client.ImportBulkRelationships(ctx, grpc.Trailer(&trailer))
				require.NoError(t, err)
				_, err = writer.CloseAndRecv()
				require.NoError(t, err)
				return trailer
			},
			"ExportBulkRelationships": func(t *testing.T, client v1.PermissionsServiceClient) metadata.MD {
				var trailer metadata.MD
				stream, err := client.ExportBulkRelationships(ctx, &v1.ExportBulkRelationshipsRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
						},
					},
				}, grpc.Trailer(&trailer))
				require.NoError(t, err)

				for {
					_, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}

					require.NoError(t, err)
				}

				return trailer
			},
		},
	)

	// SchemaService
	checkServiceMethods(
		t,
		v1.NewSchemaServiceClient(conn),
		map[string]func(t *testing.T, client v1.SchemaServiceClient) metadata.MD{
			"ReadSchema": func(t *testing.T, client v1.SchemaServiceClient) metadata.MD {
				var trailer metadata.MD
				_, err := client.ReadSchema(ctx, &v1.ReadSchemaRequest{}, grpc.Trailer(&trailer))
				require.NoError(t, err)
				return trailer
			},
			"WriteSchema": func(t *testing.T, client v1.SchemaServiceClient) metadata.MD {
				resp, err := client.ReadSchema(ctx, &v1.ReadSchemaRequest{})
				require.NoError(t, err)

				var trailer metadata.MD
				_, err = client.WriteSchema(ctx, &v1.WriteSchemaRequest{
					Schema: resp.SchemaText + "\ndefinition foo {}",
				}, grpc.Trailer(&trailer))
				require.NoError(t, err)
				return trailer
			},
		},
	)
}

func checkServiceMethods[T any](
	t *testing.T,
	client T,
	handlers map[string]func(t *testing.T, client T) metadata.MD,
) {
	et := reflect.TypeOf(new(T)).Elem()
	for i := 0; i < et.NumMethod(); i++ {
		methodName := et.Method(i).Name
		t.Run(methodName, func(t *testing.T) {
			handler, ok := handlers[methodName]
			if !ok {
				return
			}
			require.True(t, ok, "missing handler for method %s under %T", methodName, new(T))

			trailer := handler(t, client)
			require.Greater(t, trailer.Len(), 0)

			dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
			require.NoError(t, err)
			require.Greater(t, dispatchCount, 0)
		})
	}
}
