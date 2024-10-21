//go:build ci && docker && !skipintegrationtests
// +build ci,docker,!skipintegrationtests

package integrationtesting_test

import (
	"context"
	"slices"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/spanner"
	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

type testCase struct {
	name   string
	schema string
	runOp  func(t *testing.T, client v1.PermissionsServiceClient)
}

func TestDispatchIntegration(t *testing.T) {
	blacklist := []string{
		spanner.Engine, // spanner emulator doesn't support parallel transactions
	}

	testCases := []testCase{
		{
			"basic dispatched permissions checks",
			`definition user {}
			
			definition resource {
				relation parent: resource
				relation viewer: user
				permission view = viewer + parent->view
			}`,
			func(t *testing.T, client v1.PermissionsServiceClient) {
				resp, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
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

				cresp, err := client.CheckPermission(context.Background(), &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: resp.WrittenAt,
						},
					},
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
				require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, cresp.Permissionship)

				cresp2, err := client.CheckPermission(context.Background(), &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: resp.WrittenAt,
						},
					},
					Resource: &v1.ObjectReference{
						ObjectType: "resource",
						ObjectId:   "foo",
					},
					Permission: "view",
					Subject: &v1.SubjectReference{
						Object: &v1.ObjectReference{
							ObjectType: "user",
							ObjectId:   "jill",
						},
					},
				})
				require.NoError(t, err)
				require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, cresp2.Permissionship)
			},
		},
		{
			"unknown parent relation test",
			`definition user {}
			
			definition someothertype {}

			definition resource {
				relation parent: someothertype
				relation viewer: user
				permission view = viewer + parent->unknown
			}`,
			func(t *testing.T, client v1.PermissionsServiceClient) {
				resp, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
					Updates: []*v1.RelationshipUpdate{
						{
							Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
							Relationship: tuple.ToV1Relationship(tuple.MustParse("resource:foo#parent@someothertype:bar")),
						},
					},
				})
				require.NoError(t, err)

				cresp, err := client.CheckPermission(context.Background(), &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: resp.WrittenAt,
						},
					},
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
				require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, cresp.Permissionship)
			},
		},
		{
			"unknown relation test",
			`definition user {}

			definition resource {
				relation viewer: user
				permission view = viewer
			}`,
			func(t *testing.T, client v1.PermissionsServiceClient) {
				resp, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
					Updates: []*v1.RelationshipUpdate{
						{
							Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
							Relationship: tuple.ToV1Relationship(tuple.MustParse("resource:foo#viewer@user:someuser")),
						},
					},
				})
				require.NoError(t, err)

				_, cerr := client.CheckPermission(context.Background(), &v1.CheckPermissionRequest{
					Consistency: &v1.Consistency{
						Requirement: &v1.Consistency_AtLeastAsFresh{
							AtLeastAsFresh: resp.WrittenAt,
						},
					},
					Resource: &v1.ObjectReference{
						ObjectType: "resource",
						ObjectId:   "foo",
					},
					Permission: "unknown",
					Subject: &v1.SubjectReference{
						Object: &v1.ObjectReference{
							ObjectType: "user",
							ObjectId:   "tom",
						},
					},
				})
				require.Error(t, cerr)
			},
		},
		{
			"delete preconditions test",
			`definition user {}

			definition resource {
				relation viewer: user
				permission view = viewer
			}`,
			func(t *testing.T, client v1.PermissionsServiceClient) {
				// Ensure the delete fails on the precondition.
				_, derr := client.DeleteRelationships(context.Background(), &v1.DeleteRelationshipsRequest{
					OptionalPreconditions: []*v1.Precondition{
						{
							Operation: v1.Precondition_OPERATION_MUST_MATCH,
							Filter: &v1.RelationshipFilter{
								ResourceType:       "resource",
								OptionalResourceId: "someresource",
								OptionalRelation:   "viewer",
								OptionalSubjectFilter: &v1.SubjectFilter{
									SubjectType:       "user",
									OptionalSubjectId: "sarah",
								},
							},
						},
					},
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType: "resource",
					},
				})
				require.Error(t, derr)
				require.Contains(t, derr.Error(), "unable to satisfy write precondition")

				// Write a relationship, but not the one we want.
				_, werr := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
					Updates: []*v1.RelationshipUpdate{
						{
							Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
							Relationship: tuple.ToV1Relationship(tuple.MustParse("resource:someresource#viewer@user:someuser")),
						},
					},
				})
				require.NoError(t, werr)

				// Ensure the delete still fails on the precondition.
				_, derr = client.DeleteRelationships(context.Background(), &v1.DeleteRelationshipsRequest{
					OptionalPreconditions: []*v1.Precondition{
						{
							Operation: v1.Precondition_OPERATION_MUST_MATCH,
							Filter: &v1.RelationshipFilter{
								ResourceType:       "resource",
								OptionalResourceId: "someresource",
								OptionalRelation:   "viewer",
								OptionalSubjectFilter: &v1.SubjectFilter{
									SubjectType:       "user",
									OptionalSubjectId: "sarah",
								},
							},
						},
					},
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType: "resource",
					},
				})
				require.Error(t, derr)
				require.Contains(t, derr.Error(), "unable to satisfy write precondition")

				// Write the relationship needed.
				_, werr = client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
					Updates: []*v1.RelationshipUpdate{
						{
							Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
							Relationship: tuple.ToV1Relationship(tuple.MustParse("resource:someresource#viewer@user:sarah")),
						},
					},
				})
				require.NoError(t, werr)

				// Ensure a delete with an inverse precondition now fails.
				_, derr = client.DeleteRelationships(context.Background(), &v1.DeleteRelationshipsRequest{
					OptionalPreconditions: []*v1.Precondition{
						{
							Operation: v1.Precondition_OPERATION_MUST_NOT_MATCH,
							Filter: &v1.RelationshipFilter{
								ResourceType:       "resource",
								OptionalResourceId: "someresource",
								OptionalRelation:   "viewer",
								OptionalSubjectFilter: &v1.SubjectFilter{
									SubjectType:       "user",
									OptionalSubjectId: "sarah",
								},
							},
						},
					},
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType: "resource",
					},
				})
				require.Error(t, derr)
				require.Contains(t, derr.Error(), "unable to satisfy write precondition")

				// Ensure the delete with MUST_MATCH now works.
				resp, derr := client.DeleteRelationships(context.Background(), &v1.DeleteRelationshipsRequest{
					OptionalPreconditions: []*v1.Precondition{
						{
							Operation: v1.Precondition_OPERATION_MUST_MATCH,
							Filter: &v1.RelationshipFilter{
								ResourceType:       "resource",
								OptionalResourceId: "someresource",
								OptionalRelation:   "viewer",
								OptionalSubjectFilter: &v1.SubjectFilter{
									SubjectType:       "user",
									OptionalSubjectId: "sarah",
								},
							},
						},
					},
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType: "resource",
					},
				})
				require.NoError(t, derr)
				require.NotNil(t, resp.DeletedAt)
			},
		},
	}

	for _, engine := range datastore.Engines {
		if slices.Contains(blacklist, engine) {
			continue
		}
		b := testdatastore.RunDatastoreEngine(t, engine)
		t.Run(engine, func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					ds := b.NewDatastore(t, config.DatastoreConfigInitFunc(t,
						dsconfig.WithWatchBufferLength(0),
						dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
						dsconfig.WithRevisionQuantization(10),
						dsconfig.WithMaxRetries(50),
						dsconfig.WithRequestHedgingEnabled(false)))

					conns, cleanup := testserver.TestClusterWithDispatch(t, 1, ds)
					t.Cleanup(cleanup)

					schemaClient := v1.NewSchemaServiceClient(conns[0])
					_, err := schemaClient.WriteSchema(context.Background(), &v1.WriteSchemaRequest{
						Schema: tc.schema,
					})
					require.NoError(t, err)

					client := v1.NewPermissionsServiceClient(conns[0])
					tc.runOp(t, client)
				})
			}
		})
	}
}
