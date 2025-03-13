package v1_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/ccoveille/go-safecast"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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
				"document:ownerplan#viewer@user:owner":                       {},
				"document:companyplan#parent@folder:company":                 {},
				"document:masterplan#parent@folder:strategy":                 {},
				"document:masterplan#owner@user:product_manager":             {},
				"document:masterplan#viewer@user:eng_lead":                   {},
				"document:masterplan#parent@folder:plans":                    {},
				"document:healthplan#parent@folder:plans":                    {},
				"document:specialplan#editor@user:multiroleguy":              {},
				"document:specialplan#viewer_and_editor@user:multiroleguy":   {},
				"document:specialplan#viewer_and_editor@user:missingrolegal": {},
				"document:base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#owner@user:base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==": {},
				"document:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong#owner@user:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong": {},
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
			"resource id prefix and resource id",
			&v1.RelationshipFilter{
				OptionalResourceId:       "master",
				OptionalResourceIdPrefix: "master",
				OptionalRelation:         "parent",
			},
			codes.InvalidArgument,
			nil,
		},
		{
			"just relation",
			&v1.RelationshipFilter{
				OptionalRelation: "parent",
			},
			codes.OK,
			map[string]struct{}{
				"document:companyplan#parent@folder:company": {},
				"document:masterplan#parent@folder:strategy": {},
				"document:masterplan#parent@folder:plans":    {},
				"document:healthplan#parent@folder:plans":    {},
				"folder:strategy#parent@folder:company":      {},
			},
		},
		{
			"just resource ID",
			&v1.RelationshipFilter{
				OptionalResourceId: "masterplan",
			},
			codes.OK,
			map[string]struct{}{
				"document:masterplan#parent@folder:strategy":     {},
				"document:masterplan#parent@folder:plans":        {},
				"document:masterplan#owner@user:product_manager": {},
				"document:masterplan#viewer@user:eng_lead":       {},
			},
		},
		{
			"just resource ID prefix",
			&v1.RelationshipFilter{
				OptionalResourceIdPrefix: "masterpl",
			},
			codes.OK,
			map[string]struct{}{
				"document:masterplan#parent@folder:strategy":     {},
				"document:masterplan#parent@folder:plans":        {},
				"document:masterplan#owner@user:product_manager": {},
				"document:masterplan#viewer@user:eng_lead":       {},
			},
		},
		{
			"relation and resource ID prefix",
			&v1.RelationshipFilter{
				OptionalRelation:         "parent",
				OptionalResourceIdPrefix: "masterpl",
			},
			codes.OK,
			map[string]struct{}{
				"document:masterplan#parent@folder:strategy": {},
				"document:masterplan#parent@folder:plans":    {},
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
		{
			"invalid filter",
			&v1.RelationshipFilter{
				OptionalResourceId:       "auditors",
				OptionalResourceIdPrefix: "aud",
			},
			codes.InvalidArgument,
			nil,
		},
	}

	for _, pageSize := range []int{0, 1, 5, 10} {
		pageSize := pageSize
		t.Run(fmt.Sprintf("page%d_", pageSize), func(t *testing.T) {
			for _, delta := range testTimedeltas {
				delta := delta
				t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
					for _, tc := range testCases {
						tc := tc
						t.Run(tc.name, func(t *testing.T) {
							require := require.New(t)
							conn, cleanup, _, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
							client := v1.NewPermissionsServiceClient(conn)
							t.Cleanup(cleanup)

							var currentCursor *v1.Cursor

							// Make a copy of the expected map
							testExpected := make(map[string]struct{}, len(tc.expected))
							for k := range tc.expected {
								testExpected[k] = struct{}{}
							}

							uintPageSize, err := safecast.ToUint32(pageSize)
							require.NoError(err)
							for i := 0; i < 20; i++ {
								stream, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
									Consistency: &v1.Consistency{
										Requirement: &v1.Consistency_AtLeastAsFresh{
											AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
										},
									},
									RelationshipFilter: tc.filter,
									OptionalLimit:      uintPageSize,
									OptionalCursor:     currentCursor,
								})
								require.NoError(err)

								if tc.expectedCode != codes.OK {
									_, err := stream.Recv()
									grpcutil.RequireStatus(t, tc.expectedCode, err)
									return
								}

								foundCount := 0
								for {
									rel, err := stream.Recv()
									if errors.Is(err, io.EOF) {
										break
									}

									require.NoError(err)

									dsFilter, err := datastore.RelationshipsFilterFromPublicFilter(tc.filter)
									require.NoError(err)

									require.True(dsFilter.Test(tuple.FromV1Relationship(rel.Relationship)), "relationship did not match filter: %v", rel.Relationship)

									relString := tuple.MustV1RelString(rel.Relationship)
									_, found := tc.expected[relString]
									require.True(found, "relationship was not expected: %s", relString)

									_, notFoundTwice := testExpected[relString]
									require.True(notFoundTwice, "relationship was received from service twice: %s", relString)

									delete(testExpected, relString)
									currentCursor = rel.AfterResultCursor
									foundCount++
								}

								if pageSize == 0 {
									break
								}

								require.LessOrEqual(foundCount, pageSize)
								if foundCount < pageSize {
									break
								}
							}

							require.Empty(testExpected, "expected relationships were not received: %v", testExpected)
						})
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

	toWrite := []tuple.Relationship{
		tuple.MustParse("document:totallynew#parent@folder:plans"),
		tuple.MustParse("document:--base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#owner@user:--base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK=="),
		tuple.MustParse("document:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryincrediblysuuperlong#owner@user:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryincrediblysuuperlong"),
	}

	// Write with a failing precondition
	resp, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: tuple.ToV1Relationship(toWrite[0]),
		}},
		OptionalPreconditions: []*v1.Precondition{{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.ToV1Filter(toWrite[0]),
		}},
	})
	require.Nil(resp)
	grpcutil.RequireStatus(t, codes.FailedPrecondition, err)
	spiceerrors.RequireReason(t, v1.ErrorReason_ERROR_REASON_WRITE_OR_DELETE_PRECONDITION_FAILURE, err,
		"precondition_operation",
		"precondition_relation",
		"precondition_resource_id",
		"precondition_resource_type",
		"precondition_subject_id",
		"precondition_subject_relation",
		"precondition_subject_type",
	)

	existing := tuple.MustParse(tf.StandardRelationships[0])
	require.NotNil(existing)

	// Write with a succeeding precondition
	toWriteUpdates := make([]*v1.RelationshipUpdate, 0, len(toWrite))
	for _, tpl := range toWrite {
		toWriteUpdates = append(toWriteUpdates, &v1.RelationshipUpdate{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: tuple.ToV1Relationship(tpl),
		})
	}
	resp, err = client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: toWriteUpdates,
		OptionalPreconditions: []*v1.Precondition{{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.ToV1Filter(existing),
		}},
	})
	require.NoError(err)
	require.NotNil(resp.WrittenAt)
	require.NotZero(resp.WrittenAt.Token)

	// Ensure the written relationships exist
	for _, tpl := range toWrite {
		findWritten := &v1.RelationshipFilter{
			ResourceType:       tpl.Resource.ObjectType,
			OptionalResourceId: tpl.Resource.ObjectID,
		}

		stream, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
			RelationshipFilter: findWritten,
		})
		require.NoError(err)
		rel, err := stream.Recv()
		require.NoError(err)

		relStr, err := tuple.V1StringRelationship(rel.Relationship)
		require.NoError(err)
		require.Equal(tuple.MustString(tpl), relStr)

		_, err = stream.Recv()
		require.ErrorIs(err, io.EOF)

		// Delete the written relationship
		deleted, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
			Updates: []*v1.RelationshipUpdate{{
				Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
				Relationship: tuple.ToV1Relationship(tpl),
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
}

func TestDeleteRelationshipViaWriteNoop(t *testing.T) {
	require := require.New(t)

	conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	toDelete := tuple.MustParse("document:totallynew#parent@folder:plans")

	// Delete the non-existent relationship
	_, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
			Relationship: tuple.ToV1Relationship(toDelete),
		}},
	})
	require.NoError(err)
}

func TestWriteExpiringRelationships(t *testing.T) {
	req := require.New(t)

	conn, cleanup, _, _ := testserver.NewTestServer(req, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	toWrite := tuple.MustParse("document:companyplan#expiring_viewer@user:johndoe#...[expiration:2300-01-01T00:00:00Z]")
	relWritten := tuple.ToV1Relationship(toWrite)
	writeReq := &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{{
			Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
			Relationship: relWritten,
		}},
	}

	resp, err := client.WriteRelationships(context.Background(), writeReq)
	req.NoError(err)

	// read relationship back
	relRead := readFirst(req, client, resp.WrittenAt, relWritten)
	req.True(proto.Equal(relWritten, relRead))
}

func TestWriteCaveatedRelationships(t *testing.T) {
	for _, deleteWithCaveat := range []bool{true, false} {
		t.Run(fmt.Sprintf("with-caveat-%v", deleteWithCaveat), func(t *testing.T) {
			req := require.New(t)

			conn, cleanup, _, _ := testserver.NewTestServer(req, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
			client := v1.NewPermissionsServiceClient(conn)
			t.Cleanup(cleanup)

			toWrite := tuple.MustParse("document:companyplan#caveated_viewer@user:johndoe#...")
			caveatCtx, err := structpb.NewStruct(map[string]any{"expectedSecret": "hi"})
			req.NoError(err)

			toWrite.OptionalCaveat = &core.ContextualizedCaveat{
				CaveatName: "doesnotexist",
				Context:    caveatCtx,
			}
			toWrite.OptionalCaveat.Context = caveatCtx

			relWritten := tuple.ToV1Relationship(toWrite)
			writeReq := &v1.WriteRelationshipsRequest{
				Updates: []*v1.RelationshipUpdate{{
					Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
					Relationship: relWritten,
				}},
			}

			// Should fail due to non-existing caveat
			ctx := context.Background()
			_, err = client.WriteRelationships(ctx, writeReq)
			grpcutil.RequireStatus(t, codes.InvalidArgument, err)

			req.Contains(err.Error(), "subjects of type `user with doesnotexist` are not allowed on relation `document#caveated_viewer`")

			// should succeed
			relWritten.OptionalCaveat.CaveatName = "test"
			resp, err := client.WriteRelationships(context.Background(), writeReq)
			req.NoError(err)

			// read relationship back
			relRead := readFirst(req, client, resp.WrittenAt, relWritten)
			req.True(proto.Equal(relWritten, relRead))

			// issue the deletion
			relToDelete := tuple.ToV1Relationship(tuple.MustParse("document:companyplan#caveated_viewer@user:johndoe#..."))
			if deleteWithCaveat {
				relToDelete = tuple.ToV1Relationship(tuple.MustParse("document:companyplan#caveated_viewer@user:johndoe#...[test]"))
			}

			deleteReq := &v1.WriteRelationshipsRequest{
				Updates: []*v1.RelationshipUpdate{{
					Operation:    v1.RelationshipUpdate_OPERATION_DELETE,
					Relationship: relToDelete,
				}},
			}

			resp, err = client.WriteRelationships(context.Background(), deleteReq)
			req.NoError(err)

			// ensure the relationship is no longer present.
			stream, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_AtExactSnapshot{
						AtExactSnapshot: resp.WrittenAt,
					},
				},
				RelationshipFilter: tuple.RelToFilter(relWritten),
			})
			require.NoError(t, err)

			_, err = stream.Recv()
			require.True(t, errors.Is(err, io.EOF))
		})
	}
}

func readFirst(require *require.Assertions, client v1.PermissionsServiceClient, token *v1.ZedToken, rel *v1.Relationship) *v1.Relationship {
	stream, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: token,
			},
		},
		RelationshipFilter: tuple.RelToFilter(rel),
	})
	require.NoError(err)

	result, err := stream.Recv()
	require.NoError(err)
	return result.Relationship
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

func relWithExpiration(resType, resID, relation, subType, subID, subRel string, expiration time.Time) *v1.Relationship {
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
		OptionalExpiresAt: timestamppb.New(expiration),
	}
}

func relWithCaveat(resType, resID, relation, subType, subID, subRel, caveatName string) *v1.Relationship {
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
		OptionalCaveat: &v1.ContextualizedCaveat{
			CaveatName: caveatName,
		},
	}
}

func mustRelWithCaveatAndContext(resType, resID, relation, subType, subID, subRel, caveatName string, context map[string]any) *v1.Relationship {
	sctx, err := structpb.NewStruct(context)
	if err != nil {
		panic(err)
	}

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
		OptionalCaveat: &v1.ContextualizedCaveat{
			CaveatName: caveatName,
			Context:    sctx,
		},
	}
}

func relationshipForBulkTesting(nsAndRel struct {
	namespace string
	relation  string
}, i int,
) *v1.Relationship {
	if nsAndRel.relation == "caveated_viewer" {
		return mustRelWithCaveatAndContext(
			nsAndRel.namespace,
			strconv.Itoa(i),
			nsAndRel.relation,
			tf.UserNS.Name,
			strconv.Itoa(i),
			"",
			"test",
			map[string]any{"secret": strconv.Itoa(i)},
		)
	}

	if nsAndRel.relation == "expiring_viewer" {
		return relWithExpiration(
			nsAndRel.namespace,
			strconv.Itoa(i),
			nsAndRel.relation,
			tf.UserNS.Name,
			strconv.Itoa(i),
			"",
			time.Now().Add(time.Hour),
		)
	}

	return rel(
		nsAndRel.namespace,
		strconv.Itoa(i),
		nsAndRel.relation,
		tf.UserNS.Name,
		strconv.Itoa(i),
		"",
	)
}

func TestInvalidWriteRelationship(t *testing.T) {
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
			"the relationship filter provided is not valid",
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
			"write permission",
			nil,
			[]*v1.Relationship{rel("document", "newdoc", "view", "user", "tom", "")},
			codes.InvalidArgument,
			"cannot write a relationship to permission `view`",
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
			"user",
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
			"user:*",
		},
		{
			"duplicate relationship",
			nil,
			[]*v1.Relationship{
				rel("document", "somedoc", "parent", "user", "tom", ""),
				rel("document", "somedoc", "parent", "user", "tom", ""),
			},
			codes.InvalidArgument,
			"found more than one update",
		},
		{
			"disallowed caveat",
			nil,
			[]*v1.Relationship{relWithCaveat("document", "somedoc", "viewer", "user", "tom", "", "somecaveat")},
			codes.InvalidArgument,
			"user with somecaveat",
		},
		{
			"wildcard disallowed caveat",
			nil,
			[]*v1.Relationship{relWithCaveat("document", "somedoc", "viewer", "user", "*", "", "somecaveat")},
			codes.InvalidArgument,
			"user:* with somecaveat",
		},
		{
			"disallowed relation caveat",
			nil,
			[]*v1.Relationship{relWithCaveat("document", "somedoc", "viewer", "folder", "foo", "owner", "somecaveat")},
			codes.InvalidArgument,
			"folder#owner with somecaveat",
		},
		{
			"caveated and uncaveated versions of the same relationship",
			nil,
			[]*v1.Relationship{
				rel("document", "somedoc", "viewer", "user", "tom", ""),
				relWithCaveat("document", "somedoc", "viewer", "user", "tom", "", "somecaveat"),
			},
			codes.InvalidArgument,
			"found more than one update with relationship",
		},
	}

	for _, delta := range testTimedeltas {
		delta := delta
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				tc := tc
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
					require.Contains(errStatus.Message(), tc.errorContains, "found unexpected error message: %s", errStatus.Message())
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
			name: "delete by resource ID",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					OptionalResourceId: "auditors",
				},
			},
			deleted: map[string]struct{}{
				"folder:auditors#viewer@user:auditor": {},
			},
		},
		{
			name: "delete by resource ID prefix",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					OptionalResourceIdPrefix: "a",
				},
			},
			deleted: map[string]struct{}{
				"folder:auditors#viewer@user:auditor": {},
			},
		},
		{
			name: "delete by relation and resource ID prefix",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					OptionalResourceIdPrefix: "s",
					OptionalRelation:         "editor",
				},
			},
			deleted: map[string]struct{}{
				"document:specialplan#editor@user:multiroleguy": {},
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
				"document:ownerplan#viewer@user:owner":                       {},
				"document:companyplan#parent@folder:company":                 {},
				"document:masterplan#parent@folder:strategy":                 {},
				"document:masterplan#owner@user:product_manager":             {},
				"document:masterplan#viewer@user:eng_lead":                   {},
				"document:masterplan#parent@folder:plans":                    {},
				"document:healthplan#parent@folder:plans":                    {},
				"document:specialplan#viewer_and_editor@user:multiroleguy":   {},
				"document:specialplan#editor@user:multiroleguy":              {},
				"document:specialplan#viewer_and_editor@user:missingrolegal": {},
				"document:base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#owner@user:base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==": {},
				"document:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong#owner@user:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong": {},
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
			errorContains: "relation/permission `spotter` not found under definition `folder`",
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
			errorContains: "object definition `patron` not found",
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
			errorContains: "relation/permission `nonexistent` not found under definition `folder`",
		},
		{
			name: "delete no resource type",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					OptionalResourceId: "someunknownid",
				},
			},
			expectedCode: codes.OK,
			deleted:      map[string]struct{}{},
		},
		{
			name: "delete unknown resource type",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					ResourceType: "blah",
				},
			},
			expectedCode:  codes.FailedPrecondition,
			errorContains: "object definition `blah` not found",
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
		{
			name: "invalid filter",
			req: &v1.DeleteRelationshipsRequest{
				RelationshipFilter: &v1.RelationshipFilter{
					OptionalResourceId:       "auditors",
					OptionalResourceIdPrefix: "aud",
				},
			},
			expectedCode:  codes.InvalidArgument,
			errorContains: "resource_id and resource_id_prefix cannot be set at the same time",
		},
	}
	for _, delta := range testTimedeltas {
		delta := delta
		for _, tc := range testCases {
			tc := tc
			t.Run(fmt.Sprintf("fuzz%d/%s", delta/time.Millisecond, tc.name), func(t *testing.T) {
				require := require.New(t)
				conn, cleanup, ds, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
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

				rev, err := zedtoken.DecodeRevision(resp.DeletedAt, ds)
				require.NoError(err)
				require.True(rev.GreaterThan(revision))

				require.Equal(uint64(len(tc.deleted)), resp.RelationshipsDeletedCount)
				require.EqualValues(standardTuplesWithout(tc.deleted), readAll(require, client, resp.DeletedAt))
			})
		}
	}
}

func TestDeleteRelationshipsBeyondLimit(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	_, err := client.DeleteRelationships(context.Background(), &v1.DeleteRelationshipsRequest{
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType: "document",
		},
		OptionalLimit:                 5,
		OptionalAllowPartialDeletions: false,
	})
	require.Error(err)
	require.Contains(err.Error(), "found more than 5 relationships to be deleted and partial deletion was not requested")
}

func TestDeleteRelationshipsBeyondAllowedLimit(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	_, err := client.DeleteRelationships(context.Background(), &v1.DeleteRelationshipsRequest{
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType: "document",
		},
		OptionalLimit:                 1005,
		OptionalAllowPartialDeletions: false,
	})
	require.Error(err)
	require.Contains(err.Error(), "provided limit 1005 is greater than maximum allowed of 1000")
}

func TestReadRelationshipsBeyondAllowedLimit(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	resp, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType: "document",
		},
		OptionalLimit: 1005,
	})
	require.NoError(err)

	_, err = resp.Recv()
	require.Error(err)
	require.Contains(err.Error(), "provided limit 1005 is greater than maximum allowed of 1000")
}

func TestDeleteRelationshipsBeyondLimitPartial(t *testing.T) {
	expected := map[string]struct{}{
		"document:ownerplan#viewer@user:owner":                       {},
		"document:companyplan#parent@folder:company":                 {},
		"document:masterplan#parent@folder:strategy":                 {},
		"document:masterplan#owner@user:product_manager":             {},
		"document:masterplan#viewer@user:eng_lead":                   {},
		"document:masterplan#parent@folder:plans":                    {},
		"document:healthplan#parent@folder:plans":                    {},
		"document:specialplan#viewer_and_editor@user:multiroleguy":   {},
		"document:specialplan#editor@user:multiroleguy":              {},
		"document:specialplan#viewer_and_editor@user:missingrolegal": {},
		"document:base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==#owner@user:base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==": {},
		"document:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong#owner@user:veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylong": {},
	}

	for _, batchSize := range []int{5, 6, 7, 10} {
		batchSize := batchSize
		require.Greater(t, len(expected), batchSize)

		t.Run(fmt.Sprintf("batchsize-%d", batchSize), func(t *testing.T) {
			require := require.New(t)
			conn, cleanup, ds, revision := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
			client := v1.NewPermissionsServiceClient(conn)
			t.Cleanup(cleanup)

			iterations := 0
			uintBatchSize, err := safecast.ToUint32(batchSize)
			require.NoError(err)
			for i := 0; i < 10; i++ {
				iterations++

				headRev, err := ds.HeadRevision(context.Background())
				require.NoError(err)

				beforeDelete := readOfType(require, "document", client, zedtoken.MustNewFromRevision(headRev))

				resp, err := client.DeleteRelationships(context.Background(), &v1.DeleteRelationshipsRequest{
					RelationshipFilter: &v1.RelationshipFilter{
						ResourceType: "document",
					},
					OptionalLimit:                 uintBatchSize,
					OptionalAllowPartialDeletions: true,
				})
				require.NoError(err)

				afterDelete := readOfType(require, "document", client, resp.DeletedAt)
				require.LessOrEqual(len(beforeDelete)-len(afterDelete), batchSize)

				bs, _ := safecast.ToUint64(batchSize)
				require.LessOrEqual(resp.RelationshipsDeletedCount, bs)

				if i == 0 {
					require.Equal(v1.DeleteRelationshipsResponse_DELETION_PROGRESS_PARTIAL, resp.DeletionProgress)
				}

				if resp.DeletionProgress == v1.DeleteRelationshipsResponse_DELETION_PROGRESS_COMPLETE {
					require.NoError(err)
					require.NotNil(resp.DeletedAt)

					rev, err := zedtoken.DecodeRevision(resp.DeletedAt, ds)
					require.NoError(err)
					require.True(rev.GreaterThan(revision))
					require.EqualValues(standardTuplesWithout(expected), readAll(require, client, resp.DeletedAt))
					break
				}
			}

			require.LessOrEqual(iterations, (len(expected)/batchSize)+1)
		})
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

func TestWriteRelationshipsWithMetadata(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, beforeWriteRev := testserver.NewTestServerWithConfig(
		require,
		testTimedeltas[0],
		memdb.DisableGC,
		true,
		testserver.ServerConfig{
			MaxPreconditionsCount: 10,
			MaxUpdatesPerWrite:    10,
		},
		tf.StandardDatastoreWithData,
	)
	t.Cleanup(cleanup)

	client := v1.NewPermissionsServiceClient(conn)

	metadata, err := structpb.NewStruct(map[string]any{
		"foo": "123546",
	})
	require.NoError(err)

	_, err = client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		OptionalTransactionMetadata: metadata,
		Updates: []*v1.RelationshipUpdate{
			{
				Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
				Relationship: rel("document", "newdoc", "parent", "folder", "afolder", ""),
			},
		},
	})

	require.NoError(err)

	beforeWriteToken := zedtoken.MustNewFromRevision(beforeWriteRev)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watchClient := v1.NewWatchServiceClient(conn)

	stream, err := watchClient.Watch(ctx, &v1.WatchRequest{OptionalStartCursor: beforeWriteToken})
	require.NoError(err)

	resp, err := stream.Recv()
	require.NoError(err)
	require.Equal(metadata, resp.OptionalTransactionMetadata)
}

func TestWriteRelationshipsMetadataOverLimit(t *testing.T) {
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

	metadata, err := structpb.NewStruct(map[string]any{
		"foo": strings.Repeat("hithere", 65000),
	})
	require.NoError(err)

	_, err = client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		OptionalTransactionMetadata: metadata,
	})

	require.Error(err)
	require.Contains(err.Error(), "metadata size of 455010 is greater than maximum allowed of 65000")
}

func TestDeleteRelationshipsMetadataOverLimit(t *testing.T) {
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

	metadata, err := structpb.NewStruct(map[string]any{
		"foo": strings.Repeat("hithere", 65000),
	})
	require.NoError(err)

	_, err = client.DeleteRelationships(context.Background(), &v1.DeleteRelationshipsRequest{
		OptionalTransactionMetadata: metadata,
		RelationshipFilter:          &v1.RelationshipFilter{},
	})

	require.Error(err)
	require.Contains(err.Error(), "metadata size of 455010 is greater than maximum allowed of 65000")
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

func TestWriteRelationshipsCaveatExceedsMaxSize(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, _ := testserver.NewTestServerWithConfig(
		require,
		testTimedeltas[0],
		memdb.DisableGC,
		true,
		testserver.ServerConfig{
			MaxRelationshipContextSize: 1,
		},
		tf.StandardDatastoreWithCaveatedData,
	)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	rel := relWithCaveat("document", "newdoc", "parent", "folder", "afolder", "", "test")
	strct, err := structpb.NewStruct(map[string]any{"key": "value"})
	require.NoError(err)
	rel.OptionalCaveat.Context = strct

	_, err = client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			{
				Operation:    v1.RelationshipUpdate_OPERATION_TOUCH,
				Relationship: rel,
			},
		},
	})

	require.Error(err)
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
	require.ErrorContains(err, "exceeded maximum allowed caveat size of 1")
}

func TestReadRelationshipsWithTimeout(t *testing.T) {
	require := require.New(t)

	conn, cleanup, _, _ := testserver.NewTestServerWithConfig(
		require,
		0,
		memdb.DisableGC,
		false,
		testserver.ServerConfig{
			MaxUpdatesPerWrite:    1000,
			MaxPreconditionsCount: 1000,
			StreamingAPITimeout:   1,
		},
		tf.StandardDatastoreWithData,
	)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	// Write additional test data.
	counter := 0
	for i := 0; i < 10; i++ {
		updates := make([]*v1.RelationshipUpdate, 0, 100)
		for j := 0; j < 1000; j++ {
			counter++
			updates = append(updates, &v1.RelationshipUpdate{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: tuple.ToV1Relationship(tuple.MustParse(fmt.Sprintf("document:doc%d#viewer@user:someguy", counter))),
			})
		}

		_, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
			Updates: updates,
		})
		require.NoError(err)
	}

	retryCount := 5
	for i := 0; i < retryCount; i++ {
		// Perform a read and ensures it times out.
		stream, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
			},
			RelationshipFilter: &v1.RelationshipFilter{
				ResourceType: "document",
			},
		})
		require.NoError(err)

		// Ensure the recv fails with a context cancelation.
		_, err = stream.Recv()
		if err == nil {
			continue
		}

		require.ErrorContains(err, "operation took longer than allowed 1ns to complete")
		grpcutil.RequireStatus(t, codes.DeadlineExceeded, err)
	}
}

func TestReadRelationshipsInvalidCursor(t *testing.T) {
	require := require.New(t)

	conn, cleanup, _, revision := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	stream, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType:       "folder",
			OptionalResourceId: "auditors",
			OptionalRelation:   "viewer",
			OptionalSubjectFilter: &v1.SubjectFilter{
				SubjectType:       "user",
				OptionalSubjectId: "jeshk",
			},
		},
		OptionalLimit:  42,
		OptionalCursor: &v1.Cursor{Token: "someinvalidtoken"},
	})
	require.NoError(err)

	_, err = stream.Recv()
	require.Error(err)
	require.ErrorContains(err, "error decoding cursor")
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func readOfType(require *require.Assertions, resourceType string, client v1.PermissionsServiceClient, token *v1.ZedToken) map[string]struct{} {
	got := make(map[string]struct{})
	stream, err := client.ReadRelationships(context.Background(), &v1.ReadRelationshipsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtExactSnapshot{
				AtExactSnapshot: token,
			},
		},
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType: resourceType,
		},
	})
	require.NoError(err)

	for {
		rel, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(err)

		got[tuple.MustV1RelString(rel.Relationship)] = struct{}{}
	}
	return got
}

func readAll(require *require.Assertions, client v1.PermissionsServiceClient, token *v1.ZedToken) map[string]struct{} {
	got := make(map[string]struct{})
	namespaces := []string{"document", "folder"}
	for _, n := range namespaces {
		found := readOfType(require, n, client, token)
		maps.Copy(got, found)
	}
	return got
}

func standardTuplesWithout(without map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(tf.StandardRelationships)-len(without))
	for _, t := range tf.StandardRelationships {
		t = tuple.MustString(tuple.MustParse(t))
		if _, ok := without[t]; ok {
			continue
		}
		out[t] = struct{}{}
	}
	return out
}

func TestManyConcurrentWriteRelationshipsReturnsSerializationErrorOnMemdb(t *testing.T) {
	require := require.New(t)

	conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	// Kick off a number of writes to ensure at least one hits an error, as memdb's write throughput
	// is limited.
	g := errgroup.Group{}

	for i := 0; i < 50; i++ {
		i := i
		g.Go(func() error {
			updates := []*v1.RelationshipUpdate{}
			for j := 0; j < 500; j++ {
				updates = append(updates, &v1.RelationshipUpdate{
					Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
					Relationship: tuple.ToV1Relationship(tuple.MustParse(fmt.Sprintf("document:doc-%d-%d#viewer@user:tom", i, j))),
				})
			}

			_, err := client.WriteRelationships(context.Background(), &v1.WriteRelationshipsRequest{
				Updates: updates,
			})
			return err
		})
	}

	werr := g.Wait()
	require.Error(werr)
	require.ErrorContains(werr, "serialization max retries exceeded")
	grpcutil.RequireStatus(t, codes.DeadlineExceeded, werr)
}
