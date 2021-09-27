package v1

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
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
					client, stop, revision := newPermissionsServicer(require, delta, memdb.DisableGC, 0)
					defer stop()

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
							if err == io.EOF {
								break
							}

							require.NoError(err)

							relString := tuple.RelString(rel.Relationship)
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

	client, stop, _ := newPermissionsServicer(require, 0, memdb.DisableGC, 0)
	defer stop()

	toWriteStr := "document:totallynew#parent@folder:plans"
	toWrite := tuple.Parse(toWriteStr)
	require.NotNil(toWrite)

	// Write with a failing precondition
	resp, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: tuple.ToRelationship(toWrite),
		}},
		OptionalPreconditions: []*v1.Precondition{{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.ToFilter(toWrite),
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
			Relationship: tuple.ToRelationship(toWrite),
		}},
		OptionalPreconditions: []*v1.Precondition{{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.ToFilter(existing),
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
	require.Equal(tuple.String(toWrite), tuple.RelString(rel.Relationship))

	_, err = stream.Recv()
	require.ErrorIs(err, io.EOF)

	// Delete the written relationship
	deleted, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
			Relationship: tuple.ToRelationship(toWrite),
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
	}{
		{
			"empty relationship",
			nil,
			[]*v1.Relationship{{}},
			codes.InvalidArgument,
		},
		{
			"empty precondition",
			[]*v1.RelationshipFilter{{}},
			nil,
			codes.InvalidArgument,
		},
		{
			"good precondition, invalid update",
			[]*v1.RelationshipFilter{precondFilter("document", "newdoc", "parent", "folder", "afolder", nil)},
			[]*v1.Relationship{rel("document", "🍣", "parent", "folder", "afolder", "")},
			codes.InvalidArgument,
		},
		{
			"invalid precondition, good write",
			[]*v1.RelationshipFilter{precondFilter("document", "🍣", "parent", "folder", "afolder", nil)},
			[]*v1.Relationship{rel("document", "newdoc", "parent", "folder", "afolder", "")},
			codes.InvalidArgument,
		},
		{
			"write non-existing resource namespace",
			nil,
			[]*v1.Relationship{rel("notdocument", "newdoc", "parent", "folder", "afolder", "")},
			codes.FailedPrecondition,
		},
		{
			"write non-existing relation",
			nil,
			[]*v1.Relationship{rel("document", "newdoc", "notparent", "folder", "afolder", "")},
			codes.FailedPrecondition,
		},
		{
			"write non-existing subject type",
			nil,
			[]*v1.Relationship{rel("document", "newdoc", "parent", "notfolder", "afolder", "")},
			codes.FailedPrecondition,
		},
		{
			"write non-existing subject relation",
			nil,
			[]*v1.Relationship{rel("document", "newdoc", "parent", "folder", "afolder", "none")},
			codes.FailedPrecondition,
		},
		{
			"bad write wrong relation type",
			nil,
			[]*v1.Relationship{rel("document", "newdoc", "parent", "user", "someuser", "")},
			codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			client, stop, _ := newPermissionsServicer(require, 0, memdb.DisableGC, 0)
			defer stop()

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
		})
	}
}
