package v1_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"github.com/authzed/authzed-go/pkg/responsemeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	pgraph "github.com/authzed/spicedb/pkg/graph"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

var testTimedeltas = []time.Duration{0, 1 * time.Second}

func obj(objType, objID string) *v1.ObjectReference {
	return &v1.ObjectReference{
		ObjectType: objType,
		ObjectId:   objID,
	}
}

func sub(subType string, subID string, subRel string) *v1.SubjectReference {
	return &v1.SubjectReference{
		Object:           obj(subType, subID),
		OptionalRelation: subRel,
	}
}

func TestCheckPermissions(t *testing.T) {
	testCases := []struct {
		resource       *v1.ObjectReference
		permission     string
		subject        *v1.SubjectReference
		expected       v1.CheckPermissionResponse_Permissionship
		expectedStatus codes.Code
	}{
		{
			obj("document", "masterplan"),
			"view",
			sub("user", "eng_lead", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view",
			sub("user", "product_manager", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view",
			sub("user", "chief_financial_officer", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "healthplan"),
			"view",
			sub("user", "chief_financial_officer", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view",
			sub("user", "auditor", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "companyplan"),
			"view",
			sub("user", "auditor", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view",
			sub("user", "vp_product", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view",
			sub("user", "legal", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "companyplan"),
			"view",
			sub("user", "legal", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view",
			sub("user", "owner", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "companyplan"),
			"view",
			sub("user", "owner", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view",
			sub("user", "villain", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view",
			sub("user", "unknowngal", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			codes.OK,
		},

		{
			obj("document", "masterplan"),
			"view_and_edit",
			sub("user", "eng_lead", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "specialplan"),
			"view_and_edit",
			sub("user", "multiroleguy", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view_and_edit",
			sub("user", "missingrolegal", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"invalidrelation",
			sub("user", "missingrolegal", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.FailedPrecondition,
		},
		{
			obj("document", "masterplan"),
			"view_and_edit",
			sub("user", "someuser", "invalidrelation"),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.FailedPrecondition,
		},
		{
			obj("invalidnamespace", "masterplan"),
			"view_and_edit",
			sub("user", "someuser", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.FailedPrecondition,
		},
		{
			obj("document", "masterplan"),
			"view_and_edit",
			sub("invalidnamespace", "someuser", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.FailedPrecondition,
		},
		{
			obj("document", "*"),
			"view_and_edit",
			sub("invalidnamespace", "someuser", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.InvalidArgument,
		},
		{
			obj("document", "something"),
			"view",
			sub("user", "*", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.InvalidArgument,
		},
		{
			obj("document", "something"),
			"unknown",
			sub("user", "foo", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.FailedPrecondition,
		},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, debug := range []bool{false, true} {
				t.Run(fmt.Sprintf("debug%v", debug), func(t *testing.T) {
					for _, tc := range testCases {
						t.Run(fmt.Sprintf(
							"%s:%s#%s@%s:%s#%s",
							tc.resource.ObjectType,
							tc.resource.ObjectId,
							tc.permission,
							tc.subject.Object.ObjectType,
							tc.subject.Object.ObjectId,
							tc.subject.OptionalRelation,
						), func(t *testing.T) {
							require := require.New(t)
							conn, cleanup, _, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
							client := v1.NewPermissionsServiceClient(conn)
							t.Cleanup(cleanup)

							ctx := context.Background()
							if debug {
								ctx = requestmeta.AddRequestHeaders(ctx, requestmeta.RequestDebugInformation)
							}

							var trailer metadata.MD
							checkResp, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
								Consistency: &v1.Consistency{
									Requirement: &v1.Consistency_AtLeastAsFresh{
										AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
									},
								},
								Resource:   tc.resource,
								Permission: tc.permission,
								Subject:    tc.subject,
							}, grpc.Trailer(&trailer))

							if tc.expectedStatus == codes.OK {
								require.NoError(err)
								require.Equal(tc.expected, checkResp.Permissionship)

								dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
								require.NoError(err)
								require.GreaterOrEqual(dispatchCount, 0)

								encodedDebugInfo, err := responsemeta.GetResponseTrailerMetadataOrNil(trailer, responsemeta.DebugInformation)
								require.NoError(err)

								if debug {
									require.NotNil(encodedDebugInfo)

									debugInfo := &v1.DebugInformation{}
									err = protojson.Unmarshal([]byte(*encodedDebugInfo), debugInfo)
									require.NoError(err)
									require.NotNil(debugInfo.Check)
									require.Equal(tuple.StringObjectRef(tc.resource), tuple.StringObjectRef(debugInfo.Check.Resource))
									require.Equal(tc.permission, debugInfo.Check.Permission)
									require.Equal(tuple.StringSubjectRef(tc.subject), tuple.StringSubjectRef(debugInfo.Check.Subject))
								} else {
									require.Nil(encodedDebugInfo)
								}
							} else {
								grpcutil.RequireStatus(t, tc.expectedStatus, err)
							}
						})
					}
				})
			}
		})
	}
}

func TestCheckPermissionWithDebugInfo(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(require, testTimedeltas[0], memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	ctx := context.Background()
	ctx = requestmeta.AddRequestHeaders(ctx, requestmeta.RequestDebugInformation)

	var trailer metadata.MD
	checkResp, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		Resource:   obj("document", "masterplan"),
		Permission: "view",
		Subject:    sub("user", "auditor", ""),
	}, grpc.Trailer(&trailer))

	require.NoError(err)
	require.Equal(v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, checkResp.Permissionship)

	encodedDebugInfo, err := responsemeta.GetResponseTrailerMetadataOrNil(trailer, responsemeta.DebugInformation)
	require.NoError(err)

	require.NotNil(encodedDebugInfo)

	debugInfo := &v1.DebugInformation{}
	err = protojson.Unmarshal([]byte(*encodedDebugInfo), debugInfo)
	require.NoError(err)

	require.GreaterOrEqual(len(debugInfo.Check.GetSubProblems().Traces), 1)
	require.NotEmpty(debugInfo.SchemaUsed)

	// Compile the schema into the namespace definitions.
	emptyDefaultPrefix := ""
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: debugInfo.SchemaUsed,
	}, &emptyDefaultPrefix)
	require.NoError(err, "Invalid schema: %s", debugInfo.SchemaUsed)
	require.Equal(4, len(compiled.OrderedDefinitions))
}

func TestLookupResources(t *testing.T) {
	testCases := []struct {
		objectType        string
		permission        string
		subject           *v1.SubjectReference
		expectedObjectIds []string
		expectedErrorCode codes.Code
	}{
		{
			"document", "view",
			sub("user", "eng_lead", ""),
			[]string{"masterplan"},
			codes.OK,
		},
		{
			"document", "view",
			sub("user", "product_manager", ""),
			[]string{"masterplan"},
			codes.OK,
		},
		{
			"document", "view",
			sub("user", "chief_financial_officer", ""),
			[]string{"masterplan", "healthplan"},
			codes.OK,
		},
		{
			"document", "view",
			sub("user", "auditor", ""),
			[]string{"masterplan", "companyplan"},
			codes.OK,
		},
		{
			"document", "view",
			sub("user", "vp_product", ""),
			[]string{"masterplan"},
			codes.OK,
		},
		{
			"document", "view",
			sub("user", "legal", ""),
			[]string{"masterplan", "companyplan"},
			codes.OK,
		},
		{
			"document", "view",
			sub("user", "owner", ""),
			[]string{"masterplan", "companyplan"},
			codes.OK,
		},
		{
			"document", "view",
			sub("user", "villain", ""),
			nil,
			codes.OK,
		},
		{
			"document", "view",
			sub("user", "unknowngal", ""),
			nil,
			codes.OK,
		},

		{
			"document", "view_and_edit",
			sub("user", "eng_lead", ""),
			nil,
			codes.OK,
		},
		{
			"document", "view_and_edit",
			sub("user", "multiroleguy", ""),
			[]string{"specialplan"},
			codes.OK,
		},
		{
			"document", "view_and_edit",
			sub("user", "missingrolegal", ""),
			nil,
			codes.OK,
		},
		{
			"document", "invalidrelation",
			sub("user", "missingrolegal", ""),
			[]string{},
			codes.FailedPrecondition,
		},
		{
			"document", "view_and_edit",
			sub("user", "someuser", "invalidrelation"),
			[]string{},
			codes.FailedPrecondition,
		},
		{
			"invalidnamespace", "view_and_edit",
			sub("user", "someuser", ""),
			[]string{},
			codes.FailedPrecondition,
		},
		{
			"document", "view_and_edit",
			sub("invalidnamespace", "someuser", ""),
			[]string{},
			codes.FailedPrecondition,
		},
		{
			"document", "view_and_edit",
			sub("user", "*", ""),
			[]string{},
			codes.InvalidArgument,
		},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("%s::%s from %s:%s#%s", tc.objectType, tc.permission, tc.subject.Object.ObjectType, tc.subject.Object.ObjectId, tc.subject.OptionalRelation), func(t *testing.T) {
					require := require.New(t)
					conn, cleanup, _, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
					client := v1.NewPermissionsServiceClient(conn)
					t.Cleanup(func() {
						goleak.VerifyNone(t, goleak.IgnoreCurrent())
					})
					t.Cleanup(cleanup)

					var trailer metadata.MD
					lookupClient, err := client.LookupResources(context.Background(), &v1.LookupResourcesRequest{
						ResourceObjectType: tc.objectType,
						Permission:         tc.permission,
						Subject:            tc.subject,
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_AtLeastAsFresh{
								AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
							},
						},
					}, grpc.Trailer(&trailer))

					require.NoError(err)
					if tc.expectedErrorCode == codes.OK {
						var resolvedObjectIds []string
						for {
							resp, err := lookupClient.Recv()
							if errors.Is(err, io.EOF) {
								break
							}

							require.NoError(err)

							resolvedObjectIds = append(resolvedObjectIds, resp.ResourceObjectId)
						}

						sort.Strings(tc.expectedObjectIds)
						sort.Strings(resolvedObjectIds)

						require.Equal(tc.expectedObjectIds, resolvedObjectIds)

						dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
						require.NoError(err)
						require.GreaterOrEqual(dispatchCount, 0)
					} else {
						_, err := lookupClient.Recv()
						grpcutil.RequireStatus(t, tc.expectedErrorCode, err)
					}
				})
			}
		})
	}
}

func TestExpand(t *testing.T) {
	testCases := []struct {
		startObjectType    string
		startObjectID      string
		startPermission    string
		expandRelatedCount int
		expectedErrorCode  codes.Code
	}{
		{"document", "masterplan", "owner", 1, codes.OK},
		{"document", "masterplan", "view", 7, codes.OK},
		{"document", "masterplan", "fakerelation", 0, codes.FailedPrecondition},
		{"fake", "masterplan", "owner", 0, codes.FailedPrecondition},
		{"document", "", "owner", 1, codes.InvalidArgument},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("%s:%s#%s", tc.startObjectType, tc.startObjectID, tc.startPermission), func(t *testing.T) {
					require := require.New(t)
					conn, cleanup, _, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
					client := v1.NewPermissionsServiceClient(conn)
					t.Cleanup(cleanup)

					var trailer metadata.MD
					expanded, err := client.ExpandPermissionTree(context.Background(), &v1.ExpandPermissionTreeRequest{
						Resource: &v1.ObjectReference{
							ObjectType: tc.startObjectType,
							ObjectId:   tc.startObjectID,
						},
						Permission: tc.startPermission,
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_AtLeastAsFresh{
								AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
							},
						},
					}, grpc.Trailer(&trailer))
					if tc.expectedErrorCode == codes.OK {
						require.NoError(err)
						require.Equal(tc.expandRelatedCount, countLeafs(expanded.TreeRoot))

						dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
						require.NoError(err)
						require.GreaterOrEqual(dispatchCount, 0)
					} else {
						grpcutil.RequireStatus(t, tc.expectedErrorCode, err)
					}
				})
			}
		})
	}
}

func countLeafs(node *v1.PermissionRelationshipTree) int {
	switch t := node.TreeType.(type) {
	case *v1.PermissionRelationshipTree_Leaf:
		return len(t.Leaf.Subjects)

	case *v1.PermissionRelationshipTree_Intermediate:
		count := 0
		for _, child := range t.Intermediate.Children {
			count += countLeafs(child)
		}
		return count

	default:
		panic("Unknown node type")
	}
}

var ONR = tuple.ObjectAndRelation

func DS(objectType string, objectID string, objectRelation string) *core.DirectSubject {
	return &core.DirectSubject{
		Subject: ONR(objectType, objectID, objectRelation),
	}
}

func TestTranslateExpansionTree(t *testing.T) {
	table := []struct {
		name  string
		input *core.RelationTupleTreeNode
	}{
		{"simple leaf", pgraph.Leaf(nil, (DS("user", "user1", "...")))},
		{
			"simple union",
			pgraph.Union(nil,
				pgraph.Leaf(nil, (DS("user", "user1", "..."))),
				pgraph.Leaf(nil, (DS("user", "user2", "..."))),
				pgraph.Leaf(nil, (DS("user", "user3", "..."))),
			),
		},
		{
			"simple intersection",
			pgraph.Intersection(nil,
				pgraph.Leaf(nil,
					(DS("user", "user1", "...")),
					(DS("user", "user2", "...")),
				),
				pgraph.Leaf(nil,
					(DS("user", "user2", "...")),
					(DS("user", "user3", "...")),
				),
				pgraph.Leaf(nil,
					(DS("user", "user2", "...")),
					(DS("user", "user4", "...")),
				),
			),
		},
		{
			"empty intersection",
			pgraph.Intersection(nil,
				pgraph.Leaf(nil,
					(DS("user", "user1", "...")),
					(DS("user", "user2", "...")),
				),
				pgraph.Leaf(nil,
					(DS("user", "user3", "...")),
					(DS("user", "user4", "...")),
				),
			),
		},
		{
			"simple exclusion",
			pgraph.Exclusion(nil,
				pgraph.Leaf(nil,
					(DS("user", "user1", "...")),
					(DS("user", "user2", "...")),
				),
				pgraph.Leaf(nil, (DS("user", "user2", "..."))),
				pgraph.Leaf(nil, (DS("user", "user3", "..."))),
			),
		},
		{
			"empty exclusion",
			pgraph.Exclusion(nil,
				pgraph.Leaf(nil,
					(DS("user", "user1", "...")),
					(DS("user", "user2", "...")),
				),
				pgraph.Leaf(nil, (DS("user", "user1", "..."))),
				pgraph.Leaf(nil, (DS("user", "user2", "..."))),
			),
		},
	}

	for _, tt := range table {
		t.Run(tt.name, func(t *testing.T) {
			out := v1svc.TranslateRelationshipTree(v1svc.TranslateExpansionTree(tt.input))
			require.Equal(t, tt.input, out)
		})
	}
}

func TestLookupSubjects(t *testing.T) {
	testCases := []struct {
		resource        *v1.ObjectReference
		permission      string
		subjectType     string
		subjectRelation string

		expectedSubjectIds []string
		expectedErrorCode  codes.Code
	}{
		{
			obj("document", "companyplan"),
			"view",
			"user",
			"",
			[]string{"auditor", "legal", "owner"},
			codes.OK,
		},
		{
			obj("document", "healthplan"),
			"view",
			"user",
			"",
			[]string{"chief_financial_officer"},
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view",
			"user",
			"",
			[]string{"auditor", "chief_financial_officer", "eng_lead", "legal", "owner", "product_manager", "vp_product"},
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"view_and_edit",
			"user",
			"",
			nil,
			codes.OK,
		},
		{
			obj("document", "specialplan"),
			"view_and_edit",
			"user",
			"",
			[]string{"multiroleguy"},
			codes.OK,
		},
		{
			obj("document", "unknownobj"),
			"view",
			"user",
			"",
			nil,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"invalidperm",
			"user",
			"",
			nil,
			codes.FailedPrecondition,
		},
		{
			obj("document", "masterplan"),
			"view",
			"invalidsubtype",
			"",
			nil,
			codes.FailedPrecondition,
		},
		{
			obj("unknown", "masterplan"),
			"view",
			"user",
			"",
			nil,
			codes.FailedPrecondition,
		},
		{
			obj("document", "masterplan"),
			"view",
			"user",
			"invalidrel",
			nil,
			codes.FailedPrecondition,
		},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("%s:%s#%s for %s#%s", tc.resource.ObjectType, tc.resource.ObjectId, tc.permission, tc.subjectType, tc.subjectRelation), func(t *testing.T) {
					require := require.New(t)
					conn, cleanup, _, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
					client := v1.NewPermissionsServiceClient(conn)
					t.Cleanup(func() {
						goleak.VerifyNone(t, goleak.IgnoreCurrent())
					})
					t.Cleanup(cleanup)

					var trailer metadata.MD
					lookupClient, err := client.LookupSubjects(context.Background(), &v1.LookupSubjectsRequest{
						Resource:                tc.resource,
						Permission:              tc.permission,
						SubjectObjectType:       tc.subjectType,
						OptionalSubjectRelation: tc.subjectRelation,
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_AtLeastAsFresh{
								AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
							},
						},
					}, grpc.Trailer(&trailer))

					require.NoError(err)
					if tc.expectedErrorCode == codes.OK {
						var resolvedObjectIds []string
						for {
							resp, err := lookupClient.Recv()
							if errors.Is(err, io.EOF) {
								break
							}

							require.NoError(err)

							resolvedObjectIds = append(resolvedObjectIds, resp.Subject.SubjectObjectId)
						}

						sort.Strings(tc.expectedSubjectIds)
						sort.Strings(resolvedObjectIds)

						require.Equal(tc.expectedSubjectIds, resolvedObjectIds)

						dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
						require.NoError(err)
						require.GreaterOrEqual(dispatchCount, 0)
					} else {
						_, err := lookupClient.Recv()
						grpcutil.RequireStatus(t, tc.expectedErrorCode, err)
					}
				})
			}
		})
	}
}

func TestCheckWithCaveats(t *testing.T) {
	req := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(req, testTimedeltas[0], memdb.DisableGC, true, tf.StandardDatastoreWithCaveatedData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	ctx := context.Background()

	request := &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		Resource:   obj("document", "companyplan"),
		Permission: "view",
		Subject:    sub("user", "owner", ""),
	}

	// caveat evaluated and returned false
	var err error
	request.Context, err = structpb.NewStruct(map[string]any{"secret": "incorrect_value"})
	req.NoError(err)

	checkResp, err := client.CheckPermission(ctx, request)
	req.NoError(err)
	req.Equal(v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION, checkResp.Permissionship)

	// caveat evaluated and returned true
	request.Context, err = structpb.NewStruct(map[string]any{"secret": "1234"})
	req.NoError(err)

	checkResp, err = client.CheckPermission(ctx, request)
	req.NoError(err)
	req.Equal(v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, checkResp.Permissionship)

	// caveat evaluated but context variable was missing
	request.Context = nil
	checkResp, err = client.CheckPermission(ctx, request)
	req.NoError(err)
	req.Equal(v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION, checkResp.Permissionship)
	req.EqualValues([]string{"secret"}, checkResp.PartialCaveatInfo.MissingRequiredContext)

	// context exceeds length limit
	request.Context, err = structpb.NewStruct(generateMap(64))
	req.NoError(err)

	_, err = client.CheckPermission(ctx, request)
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
}

func TestLookupResourcesWithCaveats(t *testing.T) {
	req := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(req, testTimedeltas[0], memdb.DisableGC, true,
		func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
			return tf.DatastoreFromSchemaAndTestRelationships(ds, `
				definition user {}

				caveat testcaveat(somecondition int) {
					somecondition == 42
				}

				definition document {
					relation viewer: user | user with testcaveat
					permission view = viewer
				}
			`, []*core.RelationTuple{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustWithCaveat(tuple.MustParse("document:second#viewer@user:tom"), "testcaveat"),
			}, require)
		})

	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Run with empty context.
	caveatContext, err := structpb.NewStruct(map[string]any{})
	require.NoError(t, err)

	request := &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		ResourceObjectType: "document",
		Permission:         "view",
		Subject:            sub("user", "tom", ""),
		Context:            caveatContext,
	}

	cli, err := client.LookupResources(ctx, request)
	req.NoError(err)

	var responses []*v1.LookupResourcesResponse
	for {
		res, err := cli.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		responses = append(responses, res)
	}

	sort.Sort(byIDAndPermission(responses))

	require.Equal(t, 2, len(responses))

	require.Equal(t, "first", responses[0].ResourceObjectId)
	require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, responses[0].Permissionship)

	require.Equal(t, "second", responses[1].ResourceObjectId)
	require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION, responses[1].Permissionship)
	require.Equal(t, []string{"somecondition"}, responses[1].PartialCaveatInfo.MissingRequiredContext)

	// Run with full context.
	caveatContext, err = structpb.NewStruct(map[string]any{
		"somecondition": 42,
	})
	require.NoError(t, err)

	request = &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		ResourceObjectType: "document",
		Permission:         "view",
		Subject:            sub("user", "tom", ""),
		Context:            caveatContext,
	}

	cli, err = client.LookupResources(ctx, request)
	req.NoError(err)

	responses = make([]*v1.LookupResourcesResponse, 0)
	for {
		res, err := cli.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		responses = append(responses, res)
	}

	sort.Sort(byIDAndPermission(responses))

	require.Equal(t, 2, len(responses))

	require.Equal(t, "first", responses[0].ResourceObjectId)
	require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, responses[0].Permissionship)

	require.Equal(t, "second", responses[1].ResourceObjectId)
	require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, responses[1].Permissionship)
}

type byIDAndPermission []*v1.LookupResourcesResponse

func (a byIDAndPermission) Len() int { return len(a) }
func (a byIDAndPermission) Less(i, j int) bool {
	return strings.Compare(
		fmt.Sprintf("%s:%v", a[i].ResourceObjectId, a[i].Permissionship),
		fmt.Sprintf("%s:%v", a[j].ResourceObjectId, a[j].Permissionship),
	) < 0
}
func (a byIDAndPermission) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func TestLookupSubjectsWithCaveats(t *testing.T) {
	req := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(req, testTimedeltas[0], memdb.DisableGC, true,
		func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
			return tf.DatastoreFromSchemaAndTestRelationships(ds, `
				definition user {}

				caveat testcaveat(somecondition int) {
					somecondition == 42
				}

				definition document {
					relation viewer: user | user with testcaveat
					permission view = viewer
				}
			`, []*core.RelationTuple{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:sarah"), "testcaveat"),
			}, require)
		})

	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Call with empty context.
	caveatContext, err := structpb.NewStruct(map[string]any{})
	req.NoError(err)

	request := &v1.LookupSubjectsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		Resource:          obj("document", "first"),
		Permission:        "view",
		SubjectObjectType: "user",
		Context:           caveatContext,
	}

	lookupClient, err := client.LookupSubjects(ctx, request)
	req.NoError(err)

	var resolvedSubjects []expectedSubject
	for {
		resp, err := lookupClient.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		resolvedSubjects = append(resolvedSubjects, expectedSubject{
			resp.Subject.SubjectObjectId,
			resp.Subject.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
		})
	}

	expectedSubjects := []expectedSubject{
		{"sarah", true},
		{"tom", false},
	}

	sort.Sort(sortByID(resolvedSubjects))
	sort.Sort(sortByID(expectedSubjects))

	req.Equal(expectedSubjects, resolvedSubjects)

	// Call with proper context.
	caveatContext, err = structpb.NewStruct(map[string]any{
		"somecondition": 42,
	})
	req.NoError(err)

	request = &v1.LookupSubjectsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		Resource:          obj("document", "first"),
		Permission:        "view",
		SubjectObjectType: "user",
		Context:           caveatContext,
	}

	lookupClient, err = client.LookupSubjects(ctx, request)
	req.NoError(err)

	resolvedSubjects = []expectedSubject{}
	for {
		resp, err := lookupClient.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		resolvedSubjects = append(resolvedSubjects, expectedSubject{
			resp.Subject.SubjectObjectId,
			resp.Subject.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
		})
	}

	expectedSubjects = []expectedSubject{
		{"sarah", false},
		{"tom", false},
	}

	sort.Sort(sortByID(resolvedSubjects))
	sort.Sort(sortByID(expectedSubjects))

	req.Equal(expectedSubjects, resolvedSubjects)

	// Call with negative context.
	caveatContext, err = structpb.NewStruct(map[string]any{
		"somecondition": 32,
	})
	req.NoError(err)

	request = &v1.LookupSubjectsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		Resource:          obj("document", "first"),
		Permission:        "view",
		SubjectObjectType: "user",
		Context:           caveatContext,
	}

	lookupClient, err = client.LookupSubjects(ctx, request)
	req.NoError(err)

	resolvedSubjects = []expectedSubject{}
	for {
		resp, err := lookupClient.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		resolvedSubjects = append(resolvedSubjects, expectedSubject{
			resp.Subject.SubjectObjectId,
			resp.Subject.Permissionship == v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
		})
	}

	expectedSubjects = []expectedSubject{
		{"tom", false},
	}

	sort.Sort(sortByID(resolvedSubjects))
	sort.Sort(sortByID(expectedSubjects))

	req.Equal(expectedSubjects, resolvedSubjects)
}

func TestLookupSubjectsWithCaveatedWildcards(t *testing.T) {
	req := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(req, testTimedeltas[0], memdb.DisableGC, true,
		func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
			return tf.DatastoreFromSchemaAndTestRelationships(ds, `
				definition user {}

				caveat testcaveat(somecondition int) {
					somecondition == 42
				}

				caveat anothercaveat(anothercondition int) {
					anothercondition == 42
				}

				definition document {
					relation viewer: user:* with testcaveat
					relation banned: user with testcaveat
					permission view = viewer - banned
				}
			`, []*core.RelationTuple{
				tuple.MustWithCaveat(tuple.MustParse("document:first#viewer@user:*"), "testcaveat"),
				tuple.MustWithCaveat(tuple.MustParse("document:first#banned@user:bannedguy"), "anothercaveat"),
			}, require)
		})

	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	ctx := context.Background()

	// Call with empty context.
	caveatContext, err := structpb.NewStruct(map[string]any{})
	req.NoError(err)

	request := &v1.LookupSubjectsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		Resource:          obj("document", "first"),
		Permission:        "view",
		SubjectObjectType: "user",
		Context:           caveatContext,
	}

	lookupClient, err := client.LookupSubjects(ctx, request)
	req.NoError(err)

	found := false
	for {
		resp, err := lookupClient.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		found = true
		require.NoError(t, err)
		require.Equal(t, "*", resp.Subject.SubjectObjectId)
		require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION, resp.Subject.Permissionship)
		require.Equal(t, 1, len(resp.ExcludedSubjects))

		require.Equal(t, "bannedguy", resp.ExcludedSubjects[0].SubjectObjectId)
		require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION, resp.ExcludedSubjects[0].Permissionship)
	}
	require.True(t, found)

	// Call with negative context.
	caveatContext, err = structpb.NewStruct(map[string]any{
		"anothercondition": 41,
	})
	req.NoError(err)

	request = &v1.LookupSubjectsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		Resource:          obj("document", "first"),
		Permission:        "view",
		SubjectObjectType: "user",
		Context:           caveatContext,
	}

	lookupClient, err = client.LookupSubjects(ctx, request)
	req.NoError(err)

	found = false
	for {
		resp, err := lookupClient.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		found = true
		require.NoError(t, err)
		require.Equal(t, "*", resp.Subject.SubjectObjectId)
		require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_CONDITIONAL_PERMISSION, resp.Subject.Permissionship)
		require.Equal(t, 0, len(resp.ExcludedSubjects))
	}
	require.True(t, found)
}

type expectedSubject struct {
	subjectID     string
	isConditional bool
}

type sortByID []expectedSubject

func (a sortByID) Len() int           { return len(a) }
func (a sortByID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortByID) Less(i, j int) bool { return strings.Compare(a[i].subjectID, a[j].subjectID) < 0 }

func generateMap(length int) map[string]any {
	output := make(map[string]any, length)
	for i := 0; i < length; i++ {
		random := randString(32)
		output[random] = random
	}
	return output
}

var randInput = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = randInput[rand.Intn(len(randInput))] //nolint:gosec
	}
	return string(b)
}
