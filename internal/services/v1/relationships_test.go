package v1_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

func TestReadRelationships(t *testing.T) {
	testCases := []struct {
		name         string
		filter       *v1.RelationshipFilter
		expectedCode codes.Code
		expected     map[string]struct{}
	}{
		{
			"namespace only",
			&v1.RelationshipFilter{ResourceType: tf.DocumentNS.Name},
			codes.OK,
			map[string]struct{}{
				"document:companyplan#parent@folder:company":                 {},
				"document:masterplan#parent@folder:strategy":                 {},
				"document:masterplan#owner@user:product_manager":             {},
				"document:masterplan#viewer@user:eng_lead":                   {},
				"document:masterplan#parent@folder:plans":                    {},
				"document:healthplan#parent@folder:plans":                    {},
				"document:specialplan#editor@user:multiroleguy":              {},
				"document:specialplan#viewer_and_editor@user:multiroleguy":   {},
				"document:specialplan#viewer_and_editor@user:missingrolegal": {},
			},
		},
		{
			"namespace and object id",
			&v1.RelationshipFilter{
				ResourceType:       tf.DocumentNS.Name,
				OptionalResourceId: "healthplan",
			},
			codes.OK,
			map[string]struct{}{
				"document:healthplan#parent@folder:plans": {},
			},
		},
		{
			"namespace and relation",
			&v1.RelationshipFilter{
				ResourceType:     tf.DocumentNS.Name,
				OptionalRelation: "parent",
			},
			codes.OK,
			map[string]struct{}{
				"document:companyplan#parent@folder:company": {},
				"document:masterplan#parent@folder:strategy": {},
				"document:masterplan#parent@folder:plans":    {},
				"document:healthplan#parent@folder:plans":    {},
			},
		},
		{
			"namespace and userset",
			&v1.RelationshipFilter{
				ResourceType: tf.DocumentNS.Name,
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "folder",
					OptionalSubjectId: "plans",
				},
			},
			codes.OK,
			map[string]struct{}{
				"document:masterplan#parent@folder:plans": {},
				"document:healthplan#parent@folder:plans": {},
			},
		},
		{
			"multiple filters",
			&v1.RelationshipFilter{
				ResourceType:       tf.DocumentNS.Name,
				OptionalResourceId: "masterplan",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "folder",
					OptionalSubjectId: "plans",
				},
			},
			codes.OK,
			map[string]struct{}{
				"document:masterplan#parent@folder:plans": {},
			},
		},
		{
			"bad namespace",
			&v1.RelationshipFilter{ResourceType: ""},
			codes.InvalidArgument,
			nil,
		},
		{
			"bad objectId",
			&v1.RelationshipFilter{
				ResourceType:       tf.DocumentNS.Name,
				OptionalResourceId: "🍣",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "folder",
					OptionalSubjectId: "plans",
				},
			},
			codes.InvalidArgument,
			nil,
		},
		{
			"bad object relation",
			&v1.RelationshipFilter{
				ResourceType:     tf.DocumentNS.Name,
				OptionalRelation: "ad",
			},
			codes.InvalidArgument,
			nil,
		},
		{
			"bad subject filter",
			&v1.RelationshipFilter{
				ResourceType:       tf.DocumentNS.Name,
				OptionalResourceId: "ma",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType: "doesnotexist",
				},
			},
			codes.FailedPrecondition,
			nil,
		},
		{
			"empty argument for required filter value",
			&v1.RelationshipFilter{
				ResourceType:          tf.DocumentNS.Name,
				OptionalSubjectFilter: &v1.SubjectFilter{},
			},
			codes.InvalidArgument,
			nil,
		},
		{
			"bad relation filter",
			&v1.RelationshipFilter{
				ResourceType: tf.DocumentNS.Name,
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType: "folder",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "...",
					},
				},
			},
			codes.InvalidArgument,
			nil,
		},
		{
			"missing namespace",
			&v1.RelationshipFilter{
				ResourceType: "doesnotexist",
			},
			codes.FailedPrecondition,
			nil,
		},
		{
			"missing relation",
			&v1.RelationshipFilter{
				ResourceType:     tf.DocumentNS.Name,
				OptionalRelation: "invalidrelation",
			},
			codes.FailedPrecondition,
			nil,
		},
		{
			"missing subject relation",
			&v1.RelationshipFilter{
				ResourceType: tf.DocumentNS.Name,
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType: "folder",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "doesnotexist",
					},
				},
			},
			codes.FailedPrecondition,
			nil,
		},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					require := require.New(t)
					conn, cleanup, _, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
					client := v1.NewPermissionsServiceClient(conn)
					t.Cleanup(cleanup)

					stream, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_AtLeastAsFresh{
								AtLeastAsFresh: zedtoken.NewFromRevision(revision),
							},
						},
						RelationshipFilter: tc.filter,
					})
					require.NoError(err)

					if tc.expectedCode == codes.OK {
						// Make a copy of the expected map
						testExpected := make(map[string]struct{}, len(tc.expected))
						for k := range tc.expected {
							testExpected[k] = struct{}{}
						}

						for {
							rel, err := stream.Recv()
							if errors.Is(err, io.EOF) {
								break
							}

							require.NoError(err)

							relString := tuple.MustRelString(rel.Relationship)
							_, found := tc.expected[relString]
							require.True(found, "relationship was not expected: %s", relString)

							_, notFoundTwice := testExpected[relString]
							require.True(notFoundTwice, "relationship was received from service twice: %s", relString)

							delete(testExpected, relString)
						}

						require.Empty(testExpected, "expected relationships were not received: %v", testExpected)
					} else {
						_, err := stream.Recv()
						grpcutil.RequireStatus(t, tc.expectedCode, err)
					}
				})
			}
		})
	}
}

func TestWriteRelationships(t *testing.T) {
	require := require.New(t)

	conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	toWrite := tuple.MustParse("document:totallynew#parent@folder:plans")

	// Write with a failing precondition
	resp, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: tuple.MustToRelationship(toWrite),
		}},
		OptionalPreconditions: []*v1.Precondition{{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.MustToFilter(toWrite),
		}},
	})
	require.Nil(resp)
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)

	existing := tuple.Parse(tf.StandardTuples[0])
	require.NotNil(existing)

	// Write with a succeeding precondition
	resp, err = client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: tuple.MustToRelationship(toWrite),
		}},
		OptionalPreconditions: []*v1.Precondition{{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.MustToFilter(existing),
		}},
	})
	require.NoError(err)
	require.NotNil(resp.WrittenAt)
	require.NotZero(resp.WrittenAt.Token)

	findWritten := &v1.RelationshipFilter{
		ResourceType:       "document",
		OptionalResourceId: "totallynew",
	}

	// Ensure the written relationship is exists
	stream, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
		RelationshipFilter: findWritten,
	})
	require.NoError(err)

	rel, err := stream.Recv()
	require.NoError(err)
	require.Equal(tuple.String(toWrite), tuple.MustRelString(rel.Relationship))

	_, err = stream.Recv()
	require.ErrorIs(err, io.EOF)

	// Delete the written relationship
	deleted, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
			Relationship: tuple.MustToRelationship(toWrite),
		}},
	})
	require.NoError(err)

	// Ensure the relationship was deleted
	stream, err = client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{AtLeastAsFresh: deleted.WrittenAt},
		},
		RelationshipFilter: findWritten,
	})
	require.NoError(err)
	_, err = stream.Recv()
	require.ErrorIs(err, io.EOF)
}

func precondFilter(resType, resID, relation, subType, subID string, subRel *string) *v1.RelationshipFilter {
	var optionalRel *v1.SubjectFilter_RelationFilter
	if subRel != nil {
		optionalRel = &v1.SubjectFilter_RelationFilter{
			Relation: *subRel,
		}
	}

	return &v1.RelationshipFilter{
		ResourceType:       resType,
		OptionalResourceId: resID,
		OptionalRelation:   relation,
		OptionalSubjectFilter: &v1.SubjectFilter{
			SubjectType:       subType,
			OptionalSubjectId: subID,
			OptionalRelation:  optionalRel,
		},
	}
}

func rel(resType, resID, relation, subType, subID, subRel string) *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: resType,
			ObjectId:   resID,
		},
		Relation: relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: subType,
				ObjectId:   subID,
			},
			OptionalRelation: subRel,
		},
	}
}

func TestInvalidWriteRelationshipArgs(t *testing.T) {
	testCases := []struct {
		name          string
		preconditions []*v1.RelationshipFilter
		relationships []*v1.Relationship
		expectedCode  codes.Code
		errorContains string
	}{
		{
			"empty relationship",
			nil,
			[]*v1.Relationship{{}},
			codes.InvalidArgument,
			"value is required",
		},
		{
			"empty precondition",
			[]*v1.RelationshipFilter{{}},
			nil,
			codes.InvalidArgument,
			"value does not match regex pattern",
		},
		{
			"good precondition, invalid update",
			[]*v1.RelationshipFilter{precondFilter("document", "newdoc", "parent", "folder", "afolder", nil)},
			[]*v1.Relationship{rel("document", "🍣", "parent", "folder", "afolder", "")},
			codes.InvalidArgument,
			"caused by: invalid ObjectReference.ObjectId: value does not match regex pattern",
		},
		{
			"invalid precondition, good write",
			[]*v1.RelationshipFilter{precondFilter("document", "🍣", "parent", "folder", "afolder", nil)},
			[]*v1.Relationship{rel("document", "newdoc", "parent", "folder", "afolder", "")},
			codes.InvalidArgument,
			"caused by: invalid RelationshipFilter.OptionalResourceId: value does not match regex pattern",
		},
		{
			"write non-existing resource namespace",
			nil,
			[]*v1.Relationship{rel("notdocument", "newdoc", "parent", "folder", "afolder", "")},
			codes.FailedPrecondition,
			"`notdocument` not found",
		},
		{
			"write non-existing relation",
			nil,
			[]*v1.Relationship{rel("document", "newdoc", "notparent", "folder", "afolder", "")},
			codes.FailedPrecondition,
			"`notparent` not found",
		},
		{
			"write non-existing subject type",
			nil,
			[]*v1.Relationship{rel("document", "newdoc", "parent", "notfolder", "afolder", "")},
			codes.FailedPrecondition,
			"`notfolder` not found",
		},
		{
			"write non-existing subject relation",
			nil,
			[]*v1.Relationship{rel("document", "newdoc", "parent", "folder", "afolder", "none")},
			codes.FailedPrecondition,
			"`none` not found",
		},
		{
			"bad write wrong relation type",
			nil,
			[]*v1.Relationship{rel("document", "newdoc", "parent", "user", "someuser", "")},
			codes.InvalidArgument,
			"user:someuser is not allowed",
		},
		{
			"bad write wildcard object",
			nil,
			[]*v1.Relationship{rel("document", "*", "parent", "user", "someuser", "")},
			codes.InvalidArgument,
			"alphanumeric",
		},
		{
			"disallowed wildcard subject",
			nil,
			[]*v1.Relationship{rel("document", "somedoc", "parent", "user", "*", "")},
			codes.InvalidArgument,
			"wildcard",
		},
		{
			"duplicate relationship",
			nil,
			[]*v1.Relationship{
				rel("document", "somedoc", "parent", "user", "tom", ""),
				rel("document", "somedoc", "parent", "user", "tom", ""),
			},
			codes.InvalidArgument,
			"duplicate",
		},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					require := require.New(t)
					conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
					client := v1.NewPermissionsServiceClient(conn)
					t.Cleanup(cleanup)

					var preconditions []*v1.Precondition
					for _, filter := range tc.preconditions {
						preconditions = append(preconditions, &v1.Precondition{
							Operation: v1.Precondition_OPERATION_MUST_MATCH,
							Filter:    filter,
						})
					}

					var mutations []*v1.RelationshipUpdate
					for _, rel := range tc.relationships {
						mutations = append(mutations, &v1.RelationshipUpdate{
							Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
							Relationship: rel,
						})
					}

					_, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
						Updates:               mutations,
						OptionalPreconditions: preconditions,
					})
					grpcutil.RequireStatus(t, tc.expectedCode, err)
					errStatus, ok := status.FromError(err)
					if !ok {
						panic("failed to find error in status")
					}
					require.True(strings.Contains(errStatus.Message(), tc.errorContains))
				})
			}
		})
	}
}

func TestDeleteRelationships(t *testing.T) {
	testCases := []struct {
		name          string
		req           *v1.DeleteRelationshipsRequest
		deleted       map[string]struct{}
		expectedCode  codes.Code
		errorContains string
	}{
		{
			name: "delete fully specified",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "folder",
					OptionalResourceId: "auditors",
					OptionalRelation:   "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "user",
						OptionalSubjectId: "auditor",
					},
				},
			},
			deleted: map[string]struct{}{
				"folder:auditors#viewer@user:auditor": {},
			},
		},
		{
			name: "delete resource + relation + subject type",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "document",
					OptionalResourceId: "masterplan",
					OptionalRelation:   "parent",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType: "folder",
					},
				},
			},
			deleted: map[string]struct{}{
				"document:masterplan#parent@folder:strategy": {},
				"document:masterplan#parent@folder:plans":    {},
			},
		},
		{
			name: "delete resource + relation",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "document",
					OptionalResourceId: "specialplan",
					OptionalRelation:   "viewer_and_editor",
				},
			},
			deleted: map[string]struct{}{
				"document:specialplan#viewer_and_editor@user:multiroleguy":   {},
				"document:specialplan#viewer_and_editor@user:missingrolegal": {},
			},
		},
		{
			name: "delete resource",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "document",
					OptionalResourceId: "specialplan",
				},
			},
			deleted: map[string]struct{}{
				"document:specialplan#viewer_and_editor@user:multiroleguy":   {},
				"document:specialplan#editor@user:multiroleguy":              {},
				"document:specialplan#viewer_and_editor@user:missingrolegal": {},
			},
		},
		{
			name: "delete resource type",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType: "document",
				},
			},
			deleted: map[string]struct{}{
				"document:companyplan#parent@folder:company":                 {},
				"document:masterplan#parent@folder:strategy":                 {},
				"document:masterplan#owner@user:product_manager":             {},
				"document:masterplan#viewer@user:eng_lead":                   {},
				"document:masterplan#parent@folder:plans":                    {},
				"document:healthplan#parent@folder:plans":                    {},
				"document:specialplan#viewer_and_editor@user:multiroleguy":   {},
				"document:specialplan#editor@user:multiroleguy":              {},
				"document:specialplan#viewer_and_editor@user:missingrolegal": {},
			},
		},
		{
			name: "delete relation",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:     "document",
					OptionalRelation: "parent",
				},
			},
			deleted: map[string]struct{}{
				"document:companyplan#parent@folder:company": {},
				"document:masterplan#parent@folder:strategy": {},
				"document:masterplan#parent@folder:plans":    {},
				"document:healthplan#parent@folder:plans":    {},
			},
		},
		{
			name: "delete relation + subject type",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:     "folder",
					OptionalRelation: "parent",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType: "folder",
					},
				},
			},
			deleted: map[string]struct{}{
				"folder:strategy#parent@folder:company": {},
			},
		},
		{
			name: "delete relation + subject type + subject",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:     "document",
					OptionalRelation: "parent",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "folder",
						OptionalSubjectId: "plans",
					},
				},
			},
			deleted: map[string]struct{}{
				"document:masterplan#parent@folder:plans": {},
				"document:healthplan#parent@folder:plans": {},
			},
		},
		{
			name: "delete relation + subject type + subject + relation",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:     "folder",
					OptionalRelation: "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "folder",
						OptionalSubjectId: "auditors",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "viewer",
						},
					},
				},
			},
			deleted: map[string]struct{}{
				"folder:company#viewer@folder:auditors#viewer": {},
			},
		},
		{
			name: "delete unknown relation",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:     "folder",
					OptionalRelation: "spotter",
				},
			},
			expectedCode:  codes.FailedPrecondition,
			errorContains: "failed precondition: relation/permission `spotter` not found under definition `folder`",
		},
		{
			name: "delete unknown subject type",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:     "folder",
					OptionalRelation: "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType: "patron",
					},
				},
			},
			expectedCode:  codes.FailedPrecondition,
			errorContains: "failed precondition: object definition `patron` not found",
		},
		{
			name: "delete unknown subject",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:     "folder",
					OptionalRelation: "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "folder",
						OptionalSubjectId: "nonexistent",
					},
				},
			},
			expectedCode: codes.OK,
		},
		{
			name: "delete unknown subject relation",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:     "folder",
					OptionalRelation: "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType: "folder",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "nonexistent",
						},
					},
				},
			},
			expectedCode:  codes.FailedPrecondition,
			errorContains: "failed precondition: relation/permission `nonexistent` not found under definition `folder`",
		},
		{
			name: "delete no resource type",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					OptionalResourceId: "specialplan",
				},
			},
			expectedCode:  codes.InvalidArgument,
			errorContains: "invalid DeleteRelationshipsRequest.RelationshipFilter: embedded message failed validation",
		},
		{
			name: "delete unknown resource type",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType: "blah",
				},
			},
			expectedCode:  codes.FailedPrecondition,
			errorContains: "failed precondition: object definition `blah` not found",
		},
		{
			name: "preconditions met",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "folder",
					OptionalResourceId: "auditors",
					OptionalRelation:   "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "user",
						OptionalSubjectId: "auditor",
					},
				},
				OptionalPreconditions: []*v1.Precondition{{
					Operation: v1.Precondition_OPERATION_MUST_MATCH,
					Filter:    &v1.RelationshipFilter{ResourceType: "document"},
				}},
			},
			deleted: map[string]struct{}{
				"folder:auditors#viewer@user:auditor": {},
			},
		},
		{
			name: "preconditions not met",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType:       "folder",
					OptionalResourceId: "auditors",
					OptionalRelation:   "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "user",
						OptionalSubjectId: "auditor",
					},
				},
				OptionalPreconditions: []*v1.Precondition{{
					Operation: v1.Precondition_OPERATION_MUST_MATCH,
					Filter: &v1.RelationshipFilter{
						ResourceType:       "folder",
						OptionalResourceId: "auditors",
						OptionalRelation:   "viewer",
						OptionalSubjectFilter: &v1.SubjectFilter{
							SubjectType:       "user",
							OptionalSubjectId: "jeshk",
						},
					},
				}},
			},
			expectedCode:  codes.FailedPrecondition,
			errorContains: "unable to satisfy write precondition",
		},
	}
	for _, delta := range testTimedeltas {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("fuzz%d/%s", delta/time.Millisecond, tc.name), func(t *testing.T) {
				require := require.New(t)
				conn, cleanup, _, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
				client := v1.NewPermissionsServiceClient(conn)
				t.Cleanup(cleanup)

				resp, err := client.DeleteRelationships(context.Background(), tc.req)

				if tc.expectedCode != codes.OK {
					grpcutil.RequireStatus(t, tc.expectedCode, err)
					errStatus, ok := status.FromError(err)
					require.True(ok)
					require.Contains(errStatus.Message(), tc.errorContains)
					return
				}
				require.NoError(err)
				require.NotNil(resp.DeletedAt)
				rev, err := zedtoken.DecodeRevision(resp.DeletedAt)
				require.NoError(err)
				require.True(rev.GreaterThan(revision))
				require.EqualValues(standardTuplesWithout(tc.deleted), readAll(require, client, resp.DeletedAt))
			})
		}
	}
}

func TestDeleteRelationshipsPreconditionsOverLimit(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, _ := testserver.NewTestServerWithConfig(
		require,
		testTimedeltas[0],
		memdb.DisableGC,
		true,
		testserver.ServerConfig{
			MaxPreconditionsCount: 1,
			MaxUpdatesPerWrite:    1,
		},
		tf.StandardDatastoreWithData,
	)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	_, err := client.DeleteRelationships(context.Background(), &v1.DeleteRelationshipsRequest{
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType:       "folder",
			OptionalResourceId: "auditors",
			OptionalRelation:   "viewer",
			OptionalSubjectFilter: &v1.SubjectFilter{
				SubjectType:       "user",
				OptionalSubjectId: "auditor",
			},
		},
		OptionalPreconditions: []*v1.Precondition{
			{
				Operation: v1.Precondition_OPERATION_MUST_MATCH,
				Filter: &v1.RelationshipFilter{
					ResourceType:       "folder",
					OptionalResourceId: "auditors",
					OptionalRelation:   "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "user",
						OptionalSubjectId: "jeshk",
					},
				},
			},
			{
				Operation: v1.Precondition_OPERATION_MUST_MATCH,
				Filter: &v1.RelationshipFilter{
					ResourceType:       "folder",
					OptionalResourceId: "auditors",
					OptionalRelation:   "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "user",
						OptionalSubjectId: "jeshk",
					},
				},
			},
		},
	})

	require.Error(err)
	require.Contains(err.Error(), "precondition count of 2 is greater than maximum allowed of 1")
}

func TestWriteRelationshipsPreconditionsOverLimit(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, _ := testserver.NewTestServerWithConfig(
		require,
		testTimedeltas[0],
		memdb.DisableGC,
		true,
		testserver.ServerConfig{
			MaxPreconditionsCount: 1,
			MaxUpdatesPerWrite:    1,
		},
		tf.StandardDatastoreWithData,
	)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	_, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		OptionalPreconditions: []*v1.Precondition{
			{
				Operation: v1.Precondition_OPERATION_MUST_MATCH,
				Filter: &v1.RelationshipFilter{
					ResourceType:       "folder",
					OptionalResourceId: "auditors",
					OptionalRelation:   "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "user",
						OptionalSubjectId: "jeshk",
					},
				},
			},
			{
				Operation: v1.Precondition_OPERATION_MUST_MATCH,
				Filter: &v1.RelationshipFilter{
					ResourceType:       "folder",
					OptionalResourceId: "auditors",
					OptionalRelation:   "viewer",
					OptionalSubjectFilter: &v1.SubjectFilter{
						SubjectType:       "user",
						OptionalSubjectId: "jeshk",
					},
				},
			},
		},
	})

	require.Error(err)
	require.Contains(err.Error(), "precondition count of 2 is greater than maximum allowed of 1")
}

func TestWriteRelationshipsUpdatesOverLimit(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, _ := testserver.NewTestServerWithConfig(
		require,
		testTimedeltas[0],
		memdb.DisableGC,
		true,
		testserver.ServerConfig{
			MaxPreconditionsCount: 1,
			MaxUpdatesPerWrite:    1,
		},
		tf.StandardDatastoreWithData,
	)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	_, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			{
				Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
				Relationship: rel("document", "newdoc", "parent", "folder", "afolder", ""),
			},
			{
				Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
				Relationship: rel("document", "newdoc", "parent", "folder", "afolder", ""),
			},
		},
	})

	require.Error(err)
	require.Contains(err.Error(), "update count of 2 is greater than maximum allowed of 1")
}

func readAll(require *require.Assertions, client v1.PermissionsServiceClient, token *v1.ZedToken) map[string]struct{} {
	got := make(map[string]struct{})
	namespaces := []string{"document", "folder"}
	for _, n := range namespaces {
		stream, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_AtExactSnapshot{
					AtExactSnapshot: token,
				},
			},
			RelationshipFilter: &v1.RelationshipFilter{
				ResourceType: n,
			},
		})
		require.NoError(err)

		for {
			rel, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(err)

			got[tuple.MustRelString(rel.Relationship)] = struct{}{}
		}
	}
	return got
}

func standardTuplesWithout(without map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(tf.StandardTuples)-len(without))
	for _, t := range tf.StandardTuples {
		t = tuple.String(tuple.MustParse(t))
		if _, ok := without[t]; ok {
			continue
		}
		out[t] = struct{}{}
	}
	return out
}
