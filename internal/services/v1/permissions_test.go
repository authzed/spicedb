package v1_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"github.com/authzed/authzed-go/pkg/responsemeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	itestutil "github.com/authzed/spicedb/internal/testutil"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	pgraph "github.com/authzed/spicedb/pkg/graph"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/testutil"
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
		{
			obj("document", "-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK=="),
			"view",
			sub("user", "unkn-base64YWZzZGZh-ZHNmZHPwn5iK8J+YivC/fmIrwn5iK==owngal", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "foo"),
			"*",
			sub("user", "bar", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			codes.InvalidArgument,
		},
	}

	for _, delta := range testTimedeltas {
		delta := delta
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, debug := range []bool{false, true} {
				debug := debug
				t.Run(fmt.Sprintf("debug%v", debug), func(t *testing.T) {
					for _, tc := range testCases {
						tc := tc
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
									require.Nil(encodedDebugInfo)

									debugInfo := checkResp.DebugTrace
									require.NotNil(debugInfo.Check)
									require.NotNil(debugInfo.Check.Duration)
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

	// debug info is returned empty to make sure clients are not broken with backward incompatible payloads
	require.Nil(encodedDebugInfo)

	debugInfo := checkResp.DebugTrace
	require.GreaterOrEqual(len(debugInfo.Check.GetSubProblems().Traces), 1)
	require.NotEmpty(debugInfo.SchemaUsed)

	// Compile the schema into the namespace definitions.
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: debugInfo.SchemaUsed,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(err, "Invalid schema: %s", debugInfo.SchemaUsed)
	require.Equal(4, len(compiled.OrderedDefinitions))
}

func TestLookupResources(t *testing.T) {
	testCases := []struct {
		objectType           string
		permission           string
		subject              *v1.SubjectReference
		expectedObjectIds    []string
		expectedErrorCode    codes.Code
		minimumDispatchCount int
		maximumDispatchCount int
	}{
		{
			"document", "viewer",
			sub("user", "eng_lead", ""),
			[]string{"masterplan"},
			codes.OK,
			1,
			1,
		},
		{
			"document", "view",
			sub("user", "eng_lead", ""),
			[]string{"masterplan"},
			codes.OK,
			2,
			2,
		},
		{
			"document", "view",
			sub("user", "product_manager", ""),
			[]string{"masterplan"},
			codes.OK,
			3,
			3,
		},
		{
			"document", "view",
			sub("user", "chief_financial_officer", ""),
			[]string{"masterplan", "healthplan"},
			codes.OK,
			3,
			3,
		},
		{
			"document", "view",
			sub("user", "auditor", ""),
			[]string{"masterplan", "companyplan"},
			codes.OK,
			5,
			5,
		},
		{
			"document", "view",
			sub("user", "vp_product", ""),
			[]string{"masterplan"},
			codes.OK,
			4,
			4,
		},
		{
			"document", "view",
			sub("user", "legal", ""),
			[]string{"masterplan", "companyplan"},
			codes.OK,
			4,
			4,
		},
		{
			"document", "view",
			sub("user", "owner", ""),
			[]string{"masterplan", "companyplan", "ownerplan"},
			codes.OK,
			6,
			6,
		},
		{
			"document", "view",
			sub("user", "villain", ""),
			nil,
			codes.OK,
			1,
			1,
		},
		{
			"document", "view",
			sub("user", "unknowngal", ""),
			nil,
			codes.OK,
			1,
			1,
		},
		{
			"document", "view_and_edit",
			sub("user", "eng_lead", ""),
			nil,
			codes.OK,
			1,
			1,
		},
		{
			"document", "view_and_edit",
			sub("user", "multiroleguy", ""),
			[]string{"specialplan"},
			codes.OK,
			6,
			7,
		},
		{
			"document", "view_and_edit",
			sub("user", "missingrolegal", ""),
			nil,
			codes.OK,
			1,
			1,
		},
		{
			"document", "invalidrelation",
			sub("user", "missingrolegal", ""),
			[]string{},
			codes.FailedPrecondition,
			1,
			1,
		},
		{
			"document", "view_and_edit",
			sub("user", "someuser", "invalidrelation"),
			[]string{},
			codes.FailedPrecondition,
			0,
			0,
		},
		{
			"invalidnamespace", "view_and_edit",
			sub("user", "someuser", ""),
			[]string{},
			codes.FailedPrecondition,
			0,
			0,
		},
		{
			"document", "view_and_edit",
			sub("invalidnamespace", "someuser", ""),
			[]string{},
			codes.FailedPrecondition,
			0,
			0,
		},
		{
			"document", "view_and_edit",
			sub("user", "*", ""),
			[]string{},
			codes.InvalidArgument,
			0,
			0,
		},
		{
			"document", "*",
			sub("user", "someuser", ""),
			[]string{},
			codes.InvalidArgument,
			0,
			0,
		},
	}

	for _, delta := range testTimedeltas {
		delta := delta
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				tc := tc
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

						slices.Sort(tc.expectedObjectIds)
						slices.Sort(resolvedObjectIds)

						require.Equal(tc.expectedObjectIds, resolvedObjectIds)

						dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
						require.NoError(err)
						require.GreaterOrEqual(dispatchCount, 0)
						require.LessOrEqual(dispatchCount, tc.maximumDispatchCount)
						require.GreaterOrEqual(dispatchCount, tc.minimumDispatchCount)
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
		{"document", "somedoc", "*", 1, codes.InvalidArgument},
	}

	for _, delta := range testTimedeltas {
		delta := delta
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				tc := tc
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
		tt := tt
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
		{
			obj("document", "specialplan"),
			"*",
			"user",
			"",
			nil,
			codes.InvalidArgument,
		},
	}

	for _, delta := range testTimedeltas {
		delta := delta
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				tc := tc
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

						slices.Sort(tc.expectedSubjectIds)
						slices.Sort(resolvedObjectIds)

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

func TestCheckWithCaveatErrors(t *testing.T) {
	req := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(
		req,
		testTimedeltas[0],
		memdb.DisableGC,
		true,
		func(ds datastore.Datastore, assertions *require.Assertions) (datastore.Datastore, datastore.Revision) {
			return tf.DatastoreFromSchemaAndTestRelationships(
				ds,
				`definition user {}

				 caveat somecaveat(somemap map<any>) {
					  somemap.first == 42 && somemap.second < 56
				 }
				
				 definition document {
					relation viewer: user with somecaveat
					permission view = viewer
				 }
				`,
				[]*core.RelationTuple{tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat]")},
				assertions,
			)
		})

	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	ctx := context.Background()

	tcs := []struct {
		name          string
		context       map[string]any
		expectedError string
		expectedCode  codes.Code
	}{
		{
			"nil map in context",
			map[string]any{
				"somemap": nil,
			},
			"type error for parameters for caveat `somecaveat`: could not convert context parameter `somemap`: for map<any>: map requires a map, found: <nil>",
			codes.InvalidArgument,
		},
		{
			"empty map in context",
			map[string]any{
				"somemap": map[string]any{},
			},
			"evaluation error for caveat somecaveat: no such key: first",
			codes.InvalidArgument,
		},
		{
			"wrong value in map",
			map[string]any{
				"somemap": map[string]any{
					"first":  42,
					"second": "hello",
				},
			},
			"evaluation error for caveat somecaveat: no such overload",
			codes.InvalidArgument,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			request := &v1.CheckPermissionRequest{
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_AtLeastAsFresh{
						AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
					},
				},
				Resource:   obj("document", "firstdoc"),
				Permission: "view",
				Subject:    sub("user", "tom", ""),
			}

			var err error
			request.Context, err = structpb.NewStruct(tc.context)
			req.NoError(err)

			_, err = client.CheckPermission(ctx, request)
			req.Error(err)
			req.Contains(err.Error(), tc.expectedError)
			grpcutil.RequireStatus(t, tc.expectedCode, err)
		})
	}
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

	slices.SortFunc(responses, byIDAndPermission)

	// NOTE: due to the order of the deduplication of dispatching in reachable resources, this can return the conditional
	// result more than once, as per cursored LR. Therefore, filter in that case.
	require.GreaterOrEqual(t, 3, len(responses))
	require.LessOrEqual(t, 2, len(responses))

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

	require.Equal(t, 2, len(responses))
	slices.SortFunc(responses, byIDAndPermission)

	require.Equal(t, "first", responses[0].ResourceObjectId)                                                    // nolint: gosec
	require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, responses[0].Permissionship) // nolint: gosec

	require.Equal(t, "second", responses[1].ResourceObjectId)                                                   // nolint: gosec
	require.Equal(t, v1.LookupPermissionship_LOOKUP_PERMISSIONSHIP_HAS_PERMISSION, responses[1].Permissionship) // nolint: gosec
}

func byIDAndPermission(a, b *v1.LookupResourcesResponse) int {
	return strings.Compare(
		fmt.Sprintf("%s:%v", a.ResourceObjectId, a.Permissionship),
		fmt.Sprintf("%s:%v", b.ResourceObjectId, b.Permissionship),
	)
}

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

	slices.SortFunc(resolvedSubjects, bySubjectID)
	slices.SortFunc(expectedSubjects, bySubjectID)

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

	slices.SortFunc(resolvedSubjects, bySubjectID)
	slices.SortFunc(expectedSubjects, bySubjectID)

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

	slices.SortFunc(resolvedSubjects, bySubjectID)
	slices.SortFunc(expectedSubjects, bySubjectID)

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

func bySubjectID(a, b expectedSubject) int {
	return cmp.Compare(a.subjectID, b.subjectID)
}

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

func TestGetCaveatContext(t *testing.T) {
	strct, err := structpb.NewStruct(map[string]any{"foo": "bar"})
	require.NoError(t, err)

	_, err = v1svc.GetCaveatContext(context.Background(), strct, 1)
	require.ErrorContains(t, err, "request caveat context should have less than 1 bytes")

	caveatMap, err := v1svc.GetCaveatContext(context.Background(), strct, 0)
	require.NoError(t, err)
	require.Contains(t, caveatMap, "foo")

	caveatMap, err = v1svc.GetCaveatContext(context.Background(), strct, -1)
	require.NoError(t, err)
	require.Contains(t, caveatMap, "foo")
}

func TestLookupResourcesWithCursors(t *testing.T) {
	testCases := []struct {
		objectType        string
		permission        string
		subject           *v1.SubjectReference
		expectedObjectIds []string
	}{
		{
			"document", "view",
			sub("user", "eng_lead", ""),
			[]string{"masterplan"},
		},
		{
			"document", "view",
			sub("user", "product_manager", ""),
			[]string{"masterplan"},
		},
		{
			"document", "view",
			sub("user", "chief_financial_officer", ""),
			[]string{"masterplan", "healthplan"},
		},
		{
			"document", "view",
			sub("user", "auditor", ""),
			[]string{"masterplan", "companyplan"},
		},
		{
			"document", "view",
			sub("user", "vp_product", ""),
			[]string{"masterplan"},
		},
		{
			"document", "view",
			sub("user", "legal", ""),
			[]string{"masterplan", "companyplan"},
		},
		{
			"document", "view",
			sub("user", "owner", ""),
			[]string{"masterplan", "companyplan", "ownerplan"},
		},
	}

	for _, delta := range testTimedeltas {
		delta := delta
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, limit := range []int{1, 2, 5, 10, 100} {
				limit := limit
				t.Run(fmt.Sprintf("limit%d", limit), func(t *testing.T) {
					for _, tc := range testCases {
						tc := tc
						t.Run(fmt.Sprintf("%s::%s from %s:%s#%s", tc.objectType, tc.permission, tc.subject.Object.ObjectType, tc.subject.Object.ObjectId, tc.subject.OptionalRelation), func(t *testing.T) {
							require := require.New(t)
							conn, cleanup, _, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
							client := v1.NewPermissionsServiceClient(conn)
							t.Cleanup(func() {
								goleak.VerifyNone(t, goleak.IgnoreCurrent())
							})
							t.Cleanup(cleanup)

							var currentCursor *v1.Cursor
							foundObjectIds := mapz.NewSet[string]()

							for i := 0; i < 5; i++ {
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
									OptionalLimit:  uint32(limit),
									OptionalCursor: currentCursor,
								}, grpc.Trailer(&trailer))

								require.NoError(err)

								var locallyResolvedObjectIds []string
								for {
									resp, err := lookupClient.Recv()
									if errors.Is(err, io.EOF) {
										break
									}

									require.NoError(err)

									locallyResolvedObjectIds = append(locallyResolvedObjectIds, resp.ResourceObjectId)
									foundObjectIds.Add(resp.ResourceObjectId)
									currentCursor = resp.AfterResultCursor
								}

								require.LessOrEqual(len(locallyResolvedObjectIds), limit)
								if len(locallyResolvedObjectIds) < limit {
									break
								}
							}

							resolvedObjectIds := foundObjectIds.AsSlice()
							slices.Sort(tc.expectedObjectIds)
							slices.Sort(resolvedObjectIds)

							require.Equal(tc.expectedObjectIds, resolvedObjectIds)
						})
					}
				})
			}
		})
	}
}

func TestLookupResourcesDeduplication(t *testing.T) {
	req := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(req, testTimedeltas[0], memdb.DisableGC, true,
		func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
			return tf.DatastoreFromSchemaAndTestRelationships(ds, `
				definition user {}

				definition document {
					relation viewer: user
					relation editor: user
					permission view = viewer + editor
				}
			`, []*core.RelationTuple{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:first#editor@user:tom"),
			}, require)
		})

	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	lookupClient, err := client.LookupResources(context.Background(), &v1.LookupResourcesRequest{
		ResourceObjectType: "document",
		Permission:         "view",
		Subject:            sub("user", "tom", ""),
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
	})

	require.NoError(t, err)

	foundObjectIds := mapz.NewSet[string]()
	for {
		resp, err := lookupClient.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		require.True(t, foundObjectIds.Add(resp.ResourceObjectId))
	}

	require.Equal(t, []string{"first"}, foundObjectIds.AsSlice())
}

func TestLookupResourcesBeyondAllowedLimit(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	resp, err := client.LookupResources(context.Background(), &v1.LookupResourcesRequest{
		ResourceObjectType: "document",
		Permission:         "view",
		Subject:            sub("user", "tom", ""),
		OptionalLimit:      1005,
	})
	require.NoError(err)

	_, err = resp.Recv()
	require.Error(err)
	require.Contains(err.Error(), "provided limit 1005 is greater than maximum allowed of 1000")
}

func TestCheckBulkPermissions(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.StandardDatastoreWithCaveatedData)
	client := v1.NewPermissionsServiceClient(conn)
	defer cleanup()

	testCases := []struct {
		name                  string
		requests              []string
		response              []bulkCheckTest
		expectedDispatchCount int
	}{
		{
			name: "same resource and permission, different subjects",
			requests: []string{
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:masterplan#view@user:product_manager[test:{"secret": "1234"}]`,
				`document:masterplan#view@user:villain[test:{"secret": "1234"}]`,
			},
			response: []bulkCheckTest{
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:masterplan#view@user:product_manager[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:masterplan#view@user:villain[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
			},
			expectedDispatchCount: 49,
		},
		{
			name: "different resources, same permission and subject",
			requests: []string{
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:companyplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:healthplan#view@user:eng_lead[test:{"secret": "1234"}]`,
			},
			response: []bulkCheckTest{
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:companyplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
				{
					req:  `document:healthplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
			},
			expectedDispatchCount: 18,
		},
		{
			name: "some items fail",
			requests: []string{
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				"fake:fake#fake@fake:fake",
				"superfake:plan#view@user:eng_lead",
			},
			response: []bulkCheckTest{
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req: "fake:fake#fake@fake:fake",
					err: namespace.NewNamespaceNotFoundErr("fake"),
				},
				{
					req: "superfake:plan#view@user:eng_lead",
					err: namespace.NewNamespaceNotFoundErr("superfake"),
				},
			},
			expectedDispatchCount: 17,
		},
		{
			name: "different caveat context is not clustered",
			requests: []string{
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:companyplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:masterplan#view@user:eng_lead[test:{"secret": "4321"}]`,
				`document:masterplan#view@user:eng_lead`,
			},
			response: []bulkCheckTest{
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:companyplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "4321"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
				{
					req:     `document:masterplan#view@user:eng_lead`,
					resp:    v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
					partial: []string{"secret"},
				},
			},
			expectedDispatchCount: 50,
		},
		{
			name: "namespace validation",
			requests: []string{
				"document:masterplan#view@fake:fake",
				"fake:fake#fake@user:eng_lead",
			},
			response: []bulkCheckTest{
				{
					req: "document:masterplan#view@fake:fake",
					err: namespace.NewNamespaceNotFoundErr("fake"),
				},
				{
					req: "fake:fake#fake@user:eng_lead",
					err: namespace.NewNamespaceNotFoundErr("fake"),
				},
			},
			expectedDispatchCount: 1,
		},
		{
			name: "chunking test",
			requests: (func() []string {
				toReturn := make([]string, 0, datastore.FilterMaximumIDCount+5)
				for i := 0; i < int(datastore.FilterMaximumIDCount+5); i++ {
					toReturn = append(toReturn, fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i))
				}

				return toReturn
			})(),
			response: (func() []bulkCheckTest {
				toReturn := make([]bulkCheckTest, 0, datastore.FilterMaximumIDCount+5)
				for i := 0; i < int(datastore.FilterMaximumIDCount+5); i++ {
					toReturn = append(toReturn, bulkCheckTest{
						req:  fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i),
						resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					})
				}

				return toReturn
			})(),
			expectedDispatchCount: 11,
		},
		{
			name: "chunking test with errors",
			requests: (func() []string {
				toReturn := make([]string, 0, datastore.FilterMaximumIDCount+6)
				toReturn = append(toReturn, `nondoc:masterplan#view@user:eng_lead`)

				for i := 0; i < int(datastore.FilterMaximumIDCount+5); i++ {
					toReturn = append(toReturn, fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i))
				}

				return toReturn
			})(),
			response: (func() []bulkCheckTest {
				toReturn := make([]bulkCheckTest, 0, datastore.FilterMaximumIDCount+6)
				toReturn = append(toReturn, bulkCheckTest{
					req: `nondoc:masterplan#view@user:eng_lead`,
					err: namespace.NewNamespaceNotFoundErr("nondoc"),
				})

				for i := 0; i < int(datastore.FilterMaximumIDCount+5); i++ {
					toReturn = append(toReturn, bulkCheckTest{
						req:  fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i),
						resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					})
				}

				return toReturn
			})(),
			expectedDispatchCount: 11,
		},
		{
			name: "same resource and permission with same subject, repeated",
			requests: []string{
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
				`document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
			},
			response: []bulkCheckTest{
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:masterplan#view@user:eng_lead[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
			},
			expectedDispatchCount: 17,
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			req := v1.CheckBulkPermissionsRequest{
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
				},
				Items: make([]*v1.CheckBulkPermissionsRequestItem, 0, len(tt.requests)),
			}

			for _, r := range tt.requests {
				req.Items = append(req.Items, relToCheckBulkRequestItem(r))
			}

			expected := make([]*v1.CheckBulkPermissionsPair, 0, len(tt.response))
			for _, r := range tt.response {
				reqRel := tuple.ParseRel(r.req)
				resp := &v1.CheckBulkPermissionsPair_Item{
					Item: &v1.CheckBulkPermissionsResponseItem{
						Permissionship: r.resp,
					},
				}
				pair := &v1.CheckBulkPermissionsPair{
					Request: &v1.CheckBulkPermissionsRequestItem{
						Resource:   reqRel.Resource,
						Permission: reqRel.Relation,
						Subject:    reqRel.Subject,
					},
					Response: resp,
				}
				if reqRel.OptionalCaveat != nil {
					pair.Request.Context = reqRel.OptionalCaveat.Context
				}
				if len(r.partial) > 0 {
					resp.Item.PartialCaveatInfo = &v1.PartialCaveatInfo{
						MissingRequiredContext: r.partial,
					}
				}

				if r.err != nil {
					rewritten := shared.RewriteError(context.Background(), r.err, &shared.ConfigForErrors{})
					s, ok := status.FromError(rewritten)
					require.True(t, ok, "expected provided error to be status")
					pair.Response = &v1.CheckBulkPermissionsPair_Error{
						Error: s.Proto(),
					}
				}
				expected = append(expected, pair)
			}

			var trailer metadata.MD
			actual, err := client.CheckBulkPermissions(context.Background(), &req, grpc.Trailer(&trailer))
			require.NoError(t, err)

			dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
			require.NoError(t, err)
			require.Equal(t, tt.expectedDispatchCount, dispatchCount)

			testutil.RequireProtoSlicesEqual(t, expected, actual.Pairs, nil, "response bulk check pairs did not match")
		})
	}
}

func TestLookupSubjectsWithCursors(t *testing.T) {
	testCases := []struct {
		resource        *v1.ObjectReference
		permission      string
		subjectType     string
		subjectRelation string

		expectedSubjectIds []string
	}{
		{
			obj("document", "companyplan"),
			"view",
			"user",
			"",
			[]string{"auditor", "legal", "owner"},
		},
		{
			obj("document", "healthplan"),
			"view",
			"user",
			"",
			[]string{"chief_financial_officer"},
		},
		{
			obj("document", "masterplan"),
			"view",
			"user",
			"",
			[]string{"auditor", "chief_financial_officer", "eng_lead", "legal", "owner", "product_manager", "vp_product"},
		},
		{
			obj("document", "masterplan"),
			"view_and_edit",
			"user",
			"",
			nil,
		},
		{
			obj("document", "specialplan"),
			"view_and_edit",
			"user",
			"",
			[]string{"multiroleguy"},
		},
		{
			obj("document", "unknownobj"),
			"view",
			"user",
			"",
			nil,
		},
	}

	for _, delta := range testTimedeltas {
		delta := delta
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, limit := range []int{1, 2, 5, 10, 100} {
				limit := limit
				t.Run(fmt.Sprintf("limit%d_", limit), func(t *testing.T) {
					for _, tc := range testCases {
						tc := tc
						t.Run(fmt.Sprintf("%s:%s#%s for %s#%s", tc.resource.ObjectType, tc.resource.ObjectId, tc.permission, tc.subjectType, tc.subjectRelation), func(t *testing.T) {
							require := require.New(t)
							conn, cleanup, _, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
							client := v1.NewPermissionsServiceClient(conn)
							t.Cleanup(func() {
								goleak.VerifyNone(t, goleak.IgnoreCurrent())
							})
							t.Cleanup(cleanup)

							var currentCursor *v1.Cursor
							foundObjectIds := mapz.NewSet[string]()

							for i := 0; i < 15; i++ {
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
									OptionalConcreteLimit: uint32(limit),
									OptionalCursor:        currentCursor,
								}, grpc.Trailer(&trailer))

								require.NoError(err)
								var resolvedObjectIds []string
								existingCursor := currentCursor
								for {
									resp, err := lookupClient.Recv()
									if errors.Is(err, io.EOF) {
										break
									}

									require.NoError(err)

									resolvedObjectIds = append(resolvedObjectIds, resp.Subject.SubjectObjectId)
									foundObjectIds.Add(resp.Subject.SubjectObjectId)
									currentCursor = resp.AfterResultCursor
								}

								require.LessOrEqual(len(resolvedObjectIds), limit, "starting at cursor %v, found: %v", existingCursor, resolvedObjectIds)

								dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
								require.NoError(err)
								require.GreaterOrEqual(dispatchCount, 0)

								if len(resolvedObjectIds) == 0 {
									break
								}
							}

							allResolvedObjectIds := foundObjectIds.AsSlice()

							sort.Strings(tc.expectedSubjectIds)
							sort.Strings(allResolvedObjectIds)

							require.Equal(tc.expectedSubjectIds, allResolvedObjectIds)
						})
					}
				})
			}
		})
	}
}

func relToCheckBulkRequestItem(rel string) *v1.CheckBulkPermissionsRequestItem {
	r := tuple.ParseRel(rel)
	item := &v1.CheckBulkPermissionsRequestItem{
		Resource:   r.Resource,
		Permission: r.Relation,
		Subject:    r.Subject,
	}
	if r.OptionalCaveat != nil {
		item.Context = r.OptionalCaveat.Context
	}
	return item
}

func withNeedsCaveatContexts(ids []string, contextKeys ...string) []string {
	for i := range ids {
		sort.Strings(contextKeys)
		ids[i] = fmt.Sprintf("%s needs [%s]", ids[i], strings.Join(contextKeys, ","))
	}
	return ids
}

func TestLookupSubjectsWithCursorsOverSchema(t *testing.T) {
	testCases := []struct {
		name          string
		schema        string
		relationships []*core.RelationTuple

		resource        *v1.ObjectReference
		permission      string
		subjectType     string
		subjectRelation string

		expectedSubjectIds []string
	}{
		{
			"basic lookup",
			`
				definition user {}

				definition document {
					relation viewer: user
					permission view = viewer
				}
			`,
			itestutil.GenResourceTuples("document", "somedoc", "viewer", "user", "...", 1000),

			obj("document", "somedoc"),
			"view",
			"user",
			"",
			itestutil.GenSubjectIds("user", 1000),
		},
		{
			"basic resolved caveated lookup",
			`
				definition user {}

				caveat testcaveat(somecondition int) {
					somecondition == 42
				}

				definition document {
					relation viewer: user with testcaveat
					permission view = viewer
				}
			`,
			itestutil.GenResourceTuplesWithCaveat("document", "somedoc", "viewer", "user", "...", "testcaveat", map[string]any{"somecondition": 42}, 1000),

			obj("document", "somedoc"),
			"view",
			"user",
			"",
			itestutil.GenSubjectIds("user", 1000),
		},
		{
			"basic unresolved caveated lookup",
			`
				definition user {}

				caveat testcaveat(somecondition int) {
					somecondition == 42
				}

				definition document {
					relation viewer: user with testcaveat
					permission view = viewer
				}
			`,
			itestutil.GenResourceTuplesWithCaveat("document", "somedoc", "viewer", "user", "...", "testcaveat", map[string]any{}, 1000),

			obj("document", "somedoc"),
			"view",
			"user",
			"",
			withNeedsCaveatContexts(itestutil.GenSubjectIds("user", 1000), "somecondition"),
		},
		{
			"partially resolved caveat lookup",
			`
			definition user {}

			caveat testcaveat(somecondition int) {
				somecondition == 42
			}

			definition document {
				relation viewer: user | user with testcaveat
				permission view = viewer
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:somedoc#viewer@user:tom"),
				tuple.MustParse("document:somedoc#viewer@user:fred[testcaveat:{\"somecondition\":42}]"),
				tuple.MustParse("document:somedoc#viewer@user:sam[testcaveat:{\"somecondition\":41}]"),
				tuple.MustParse("document:somedoc#viewer@user:sarah[testcaveat]"),
			},

			obj("document", "somedoc"),
			"view",
			"user",
			"",
			[]string{"tom", "fred", "sarah needs [somecondition]"},
		},
		{
			"lookup over wildcard",
			`
				definition user {}

				definition document {
					relation viewer: user | user:*
					permission view = viewer
				}
			`,
			itestutil.JoinTuples(
				itestutil.GenResourceTuples("document", "somedoc", "viewer", "user", "...", 500),
				[]*core.RelationTuple{
					tuple.MustParse("document:somedoc#viewer@user:*"),
				},
			),

			obj("document", "somedoc"),
			"view",
			"user",
			"",

			append(itestutil.GenSubjectIds("user", 500), "*"),
		},
		{
			"lookup over wildcard with exclusions",
			`
				definition user {}

				definition document {
					relation viewer: user | user:*
					relation banned: user
					permission view = viewer - banned
				}
			`,

			itestutil.JoinTuples(
				itestutil.GenResourceTuples("document", "somedoc", "banned", "user", "...", 25),
				[]*core.RelationTuple{
					tuple.MustParse("document:somedoc#viewer@user:*"),
				},
			),

			obj("document", "somedoc"),
			"view",
			"user",

			"",
			[]string{
				"*",
				"!user-0", "!user-1", "!user-2", "!user-3",
				"!user-4", "!user-5", "!user-6", "!user-7",
				"!user-8", "!user-9", "!user-10", "!user-11",
				"!user-12", "!user-13", "!user-14", "!user-15",
				"!user-16", "!user-17", "!user-18", "!user-19",
				"!user-20", "!user-21", "!user-22", "!user-23",
				"!user-24",
			},
		},
		{
			"lookup over wildcard with caveated exclusions",
			`
				definition user {}

				caveat testcaveat(somecondition int) {
					somecondition == 42
				}

				definition document {
					relation viewer: user | user:*
					relation banned: user with testcaveat
					permission view = viewer - banned
				}
			`,

			itestutil.JoinTuples(
				[]*core.RelationTuple{
					tuple.MustParse("document:somedoc#viewer@user:*"),
					tuple.MustParse("document:somedoc#banned@user:tom"),
					tuple.MustParse("document:somedoc#banned@user:fred[testcaveat:{\"somecondition\":42}]"),
					tuple.MustParse("document:somedoc#banned@user:sam[testcaveat:{\"somecondition\":41}]"),
					tuple.MustParse("document:somedoc#banned@user:sarah[testcaveat]"),
				},
			),

			obj("document", "somedoc"),
			"view",
			"user",

			"",
			[]string{"*", "!(sarah needs [somecondition])", "!fred", "!tom"},
		},
		{
			"lookup over union",
			`
				definition user {}

				definition document {
					relation editor: user
					relation viewer: user
					permission view = viewer + editor
				}
			`,
			itestutil.JoinTuples(
				itestutil.GenResourceTuples("document", "somedoc", "viewer", "user", "...", 580),
				itestutil.GenResourceTuplesWithOffset("document", "somedoc", "editor", "user", "...", 500, 500),
			),

			obj("document", "somedoc"),
			"view",
			"user",
			"",
			itestutil.GenSubjectIds("user", 1000),
		},
		{
			"lookup over intersection",
			`
				definition user {}

				definition document {
					relation editor: user
					relation viewer: user
					permission view = viewer & editor
				}
			`,
			itestutil.JoinTuples(
				itestutil.GenResourceTuples("document", "somedoc", "viewer", "user", "...", 580),
				itestutil.GenResourceTuplesWithOffset("document", "somedoc", "editor", "user", "...", 500, 500),
			),

			obj("document", "somedoc"),
			"view",
			"user",
			"",
			itestutil.GenSubjectIdsWithOffset("user", 500, 80),
		},
		{
			"lookup over exclusion",
			`
				definition user {}

				definition document {
					relation banned: user
					relation viewer: user
					permission view = viewer - banned
				}
			`,
			itestutil.JoinTuples(
				itestutil.GenResourceTuples("document", "somedoc", "viewer", "user", "...", 580),
				itestutil.GenResourceTuplesWithOffset("document", "somedoc", "banned", "user", "...", 500, 500),
			),

			obj("document", "somedoc"),
			"view",
			"user",
			"",
			itestutil.GenSubjectIdsWithOffset("user", 0, 500),
		},
		{
			"lookup over union with arrow",
			`
				definition user {}

				definition organization {
					relation admin: user
				}

				definition document {
					relation org: organization
					relation editor: user
					relation viewer: user
					permission view = viewer + editor + org->admin
				}
			`,
			itestutil.JoinTuples(
				itestutil.GenResourceTuples("document", "somedoc", "viewer", "user", "...", 580),
				itestutil.GenResourceTuplesWithOffset("document", "somedoc", "editor", "user", "...", 500, 500),
				itestutil.GenResourceTuplesWithOffset("organization", "someorg", "admin", "user", "...", 700, 500),
				[]*core.RelationTuple{
					tuple.MustParse("document:somedoc#org@organization:someorg"),
				},
			),

			obj("document", "somedoc"),
			"view",
			"user",
			"",
			itestutil.GenSubjectIds("user", 1200),
		},
		{
			"lookup over groups",
			`
				definition user {}

				definition group {
					relation direct_member: user | group#member
					permission member = direct_member
				}

				definition document {
					relation viewer: user | group#member
					permission view = viewer
				}
			`,
			itestutil.JoinTuples(
				itestutil.GenResourceTuples("document", "somedoc", "viewer", "user", "...", 580),
				itestutil.GenResourceTuplesWithOffset("document", "somedoc", "viewer", "user", "...", 1200, 100),
				itestutil.GenResourceTuplesWithOffset("group", "somegroup", "direct_member", "user", "...", 500, 500),
				itestutil.GenResourceTuplesWithOffset("group", "childgroup", "direct_member", "user", "...", 700, 500),
				[]*core.RelationTuple{
					tuple.MustParse("document:somedoc#viewer@group:somegroup#member"),
					tuple.MustParse("group:somegroup#direct_member@group:childgroup#member"),
				},
			),

			obj("document", "somedoc"),
			"view",
			"user",
			"",
			itestutil.GenSubjectIds("user", 1300),
		},
		{
			"complex schema with disjoint user sets",
			`
				definition user {}

				definition group {
					relation owner: user
					relation parent: group
					relation direct_member: user | group#member
					permission member = owner + direct_member + parent->member
				}

				definition supercontainer {
					relation owner: user | group#member
				}

				definition container {
					relation parent: supercontainer
					relation direct_member: user | group#member
					relation owner: user | group#member
			
					permission special_ownership = parent->owner
					permission member = owner + direct_member
				}

				definition resource {
					relation parent: container
					relation viewer: user | group#member
					relation owner: user | group#member

					permission view = owner + parent->member + viewer + parent->special_ownership			
				}
			`,
			itestutil.JoinTuples(
				[]*core.RelationTuple{
					tuple.MustParse("resource:someresource#owner@user:31#..."),
					tuple.MustParse("resource:someresource#parent@container:17#..."),
					tuple.MustParse("container:17#direct_member@group:81#member"),
					tuple.MustParse("container:17#direct_member@user:11#..."),
					tuple.MustParse("container:17#direct_member@user:129#..."),
					tuple.MustParse("container:17#direct_member@user:13#..."),
					tuple.MustParse("container:17#direct_member@user:130#..."),
					tuple.MustParse("container:17#direct_member@user:131#..."),
					tuple.MustParse("container:17#direct_member@user:133#..."),
					tuple.MustParse("container:17#direct_member@user:134#..."),
					tuple.MustParse("container:17#direct_member@user:135#..."),
					tuple.MustParse("container:17#direct_member@user:15#..."),
					tuple.MustParse("container:17#direct_member@user:16#..."),
					tuple.MustParse("container:17#direct_member@user:160#..."),
					tuple.MustParse("container:17#direct_member@user:163#..."),
					tuple.MustParse("container:17#direct_member@user:166#..."),
					tuple.MustParse("container:17#direct_member@user:17#..."),
					tuple.MustParse("container:17#direct_member@user:18#..."),
					tuple.MustParse("container:17#direct_member@user:19#..."),
					tuple.MustParse("container:17#direct_member@user:20#..."),
					tuple.MustParse("container:17#direct_member@user:23#..."),
					tuple.MustParse("container:17#direct_member@user:244#..."),
					tuple.MustParse("container:17#direct_member@user:25#..."),
					tuple.MustParse("container:17#direct_member@user:26#..."),
					tuple.MustParse("container:17#direct_member@user:262#..."),
					tuple.MustParse("container:17#direct_member@user:264#..."),
					tuple.MustParse("container:17#direct_member@user:265#..."),
					tuple.MustParse("container:17#direct_member@user:267#..."),
					tuple.MustParse("container:17#direct_member@user:268#..."),
					tuple.MustParse("container:17#direct_member@user:269#..."),
					tuple.MustParse("container:17#direct_member@user:27#..."),
					tuple.MustParse("container:17#direct_member@user:298#..."),
					tuple.MustParse("container:17#direct_member@user:30#..."),
					tuple.MustParse("container:17#direct_member@user:31#..."),
					tuple.MustParse("container:17#direct_member@user:317#..."),
					tuple.MustParse("container:17#direct_member@user:318#..."),
					tuple.MustParse("container:17#direct_member@user:32#..."),
					tuple.MustParse("container:17#direct_member@user:324#..."),
					tuple.MustParse("container:17#direct_member@user:33#..."),
					tuple.MustParse("container:17#direct_member@user:34#..."),
					tuple.MustParse("container:17#direct_member@user:341#..."),
					tuple.MustParse("container:17#direct_member@user:342#..."),
					tuple.MustParse("container:17#direct_member@user:343#..."),
					tuple.MustParse("container:17#direct_member@user:349#..."),
					tuple.MustParse("container:17#direct_member@user:357#..."),
					tuple.MustParse("container:17#direct_member@user:361#..."),
					tuple.MustParse("container:17#direct_member@user:388#..."),
					tuple.MustParse("container:17#direct_member@user:410#..."),
					tuple.MustParse("container:17#direct_member@user:430#..."),
					tuple.MustParse("container:17#direct_member@user:438#..."),
					tuple.MustParse("container:17#direct_member@user:446#..."),
					tuple.MustParse("container:17#direct_member@user:448#..."),
					tuple.MustParse("container:17#direct_member@user:451#..."),
					tuple.MustParse("container:17#direct_member@user:452#..."),
					tuple.MustParse("container:17#direct_member@user:453#..."),
					tuple.MustParse("container:17#direct_member@user:456#..."),
					tuple.MustParse("container:17#direct_member@user:458#..."),
					tuple.MustParse("container:17#direct_member@user:459#..."),
					tuple.MustParse("container:17#direct_member@user:462#..."),
					tuple.MustParse("container:17#direct_member@user:470#..."),
					tuple.MustParse("container:17#direct_member@user:471#..."),
					tuple.MustParse("container:17#direct_member@user:474#..."),
					tuple.MustParse("container:17#direct_member@user:475#..."),
					tuple.MustParse("container:17#direct_member@user:476#..."),
					tuple.MustParse("container:17#direct_member@user:477#..."),
					tuple.MustParse("container:17#direct_member@user:478#..."),
					tuple.MustParse("container:17#direct_member@user:480#..."),
					tuple.MustParse("container:17#direct_member@user:485#..."),
					tuple.MustParse("container:17#direct_member@user:488#..."),
					tuple.MustParse("container:17#direct_member@user:490#..."),
					tuple.MustParse("container:17#direct_member@user:494#..."),
					tuple.MustParse("container:17#direct_member@user:496#..."),
					tuple.MustParse("container:17#direct_member@user:506#..."),
					tuple.MustParse("container:17#direct_member@user:508#..."),
					tuple.MustParse("container:17#direct_member@user:513#..."),
					tuple.MustParse("container:17#direct_member@user:514#..."),
					tuple.MustParse("container:17#direct_member@user:518#..."),
					tuple.MustParse("container:17#direct_member@user:528#..."),
					tuple.MustParse("container:17#direct_member@user:530#..."),
					tuple.MustParse("container:17#direct_member@user:537#..."),
					tuple.MustParse("container:17#direct_member@user:545#..."),
					tuple.MustParse("container:17#direct_member@user:614#..."),
					tuple.MustParse("container:17#direct_member@user:616#..."),
					tuple.MustParse("container:17#direct_member@user:619#..."),
					tuple.MustParse("container:17#direct_member@user:620#..."),
					tuple.MustParse("container:17#direct_member@user:621#..."),
					tuple.MustParse("container:17#direct_member@user:622#..."),
					tuple.MustParse("container:17#direct_member@user:624#..."),
					tuple.MustParse("container:17#direct_member@user:625#..."),
					tuple.MustParse("container:17#direct_member@user:626#..."),
					tuple.MustParse("container:17#direct_member@user:629#..."),
					tuple.MustParse("container:17#direct_member@user:630#..."),
					tuple.MustParse("container:17#direct_member@user:633#..."),
					tuple.MustParse("container:17#direct_member@user:635#..."),
					tuple.MustParse("container:17#direct_member@user:644#..."),
					tuple.MustParse("container:17#direct_member@user:645#..."),
					tuple.MustParse("container:17#direct_member@user:646#..."),
					tuple.MustParse("container:17#direct_member@user:647#..."),
					tuple.MustParse("container:17#direct_member@user:649#..."),
					tuple.MustParse("container:17#direct_member@user:652#..."),
					tuple.MustParse("container:17#direct_member@user:653#..."),
					tuple.MustParse("container:17#direct_member@user:656#..."),
					tuple.MustParse("container:17#direct_member@user:657#..."),
					tuple.MustParse("container:17#direct_member@user:672#..."),
					tuple.MustParse("container:17#direct_member@user:680#..."),
					tuple.MustParse("container:17#direct_member@user:687#..."),
					tuple.MustParse("container:17#direct_member@user:690#..."),
					tuple.MustParse("container:17#direct_member@user:691#..."),
					tuple.MustParse("container:17#direct_member@user:698#..."),
					tuple.MustParse("container:17#direct_member@user:699#..."),
					tuple.MustParse("container:17#direct_member@user:7#..."),
					tuple.MustParse("container:17#direct_member@user:700#..."),
					tuple.MustParse("container:17#owner@user:3#..."),
					tuple.MustParse("container:17#owner@user:378#..."),
					tuple.MustParse("container:17#owner@user:410#..."),
					tuple.MustParse("container:17#owner@user:651#..."),
					tuple.MustParse("container:17#parent@supercontainer:22#..."),
					tuple.MustParse("group:81#direct_member@user:11#..."),
					tuple.MustParse("group:81#direct_member@user:129#..."),
					tuple.MustParse("group:81#direct_member@user:13#..."),
					tuple.MustParse("group:81#direct_member@user:130#..."),
					tuple.MustParse("group:81#direct_member@user:131#..."),
					tuple.MustParse("group:81#direct_member@user:133#..."),
					tuple.MustParse("group:81#direct_member@user:134#..."),
					tuple.MustParse("group:81#direct_member@user:135#..."),
					tuple.MustParse("group:81#direct_member@user:15#..."),
					tuple.MustParse("group:81#direct_member@user:156#..."),
					tuple.MustParse("group:81#direct_member@user:16#..."),
					tuple.MustParse("group:81#direct_member@user:163#..."),
					tuple.MustParse("group:81#direct_member@user:166#..."),
					tuple.MustParse("group:81#direct_member@user:167#..."),
					tuple.MustParse("group:81#direct_member@user:18#..."),
					tuple.MustParse("group:81#direct_member@user:19#..."),
					tuple.MustParse("group:81#direct_member@user:20#..."),
					tuple.MustParse("group:81#direct_member@user:23#..."),
					tuple.MustParse("group:81#direct_member@user:24#..."),
					tuple.MustParse("group:81#direct_member@user:244#..."),
					tuple.MustParse("group:81#direct_member@user:25#..."),
					tuple.MustParse("group:81#direct_member@user:26#..."),
					tuple.MustParse("group:81#direct_member@user:262#..."),
					tuple.MustParse("group:81#direct_member@user:264#..."),
					tuple.MustParse("group:81#direct_member@user:265#..."),
					tuple.MustParse("group:81#direct_member@user:267#..."),
					tuple.MustParse("group:81#direct_member@user:268#..."),
					tuple.MustParse("group:81#direct_member@user:269#..."),
					tuple.MustParse("group:81#direct_member@user:27#..."),
					tuple.MustParse("group:81#direct_member@user:285#..."),
					tuple.MustParse("group:81#direct_member@user:286#..."),
					tuple.MustParse("group:81#direct_member@user:287#..."),
					tuple.MustParse("group:81#direct_member@user:298#..."),
					tuple.MustParse("group:81#direct_member@user:30#..."),
					tuple.MustParse("group:81#direct_member@user:31#..."),
					tuple.MustParse("group:81#direct_member@user:310#..."),
					tuple.MustParse("group:81#direct_member@user:317#..."),
					tuple.MustParse("group:81#direct_member@user:318#..."),
					tuple.MustParse("group:81#direct_member@user:32#..."),
					tuple.MustParse("group:81#direct_member@user:324#..."),
					tuple.MustParse("group:81#direct_member@user:34#..."),
					tuple.MustParse("group:81#direct_member@user:341#..."),
					tuple.MustParse("group:81#direct_member@user:342#..."),
					tuple.MustParse("group:81#direct_member@user:343#..."),
					tuple.MustParse("group:81#direct_member@user:349#..."),
					tuple.MustParse("group:81#direct_member@user:371#..."),
					tuple.MustParse("group:81#direct_member@user:382#..."),
					tuple.MustParse("group:81#direct_member@user:388#..."),
					tuple.MustParse("group:81#direct_member@user:4#..."),
					tuple.MustParse("group:81#direct_member@user:411#..."),
					tuple.MustParse("group:81#direct_member@user:437#..."),
					tuple.MustParse("group:81#direct_member@user:438#..."),
					tuple.MustParse("group:81#direct_member@user:440#..."),
					tuple.MustParse("group:81#direct_member@user:452#..."),
					tuple.MustParse("group:81#direct_member@user:481#..."),
					tuple.MustParse("group:81#direct_member@user:486#..."),
					tuple.MustParse("group:81#direct_member@user:487#..."),
					tuple.MustParse("group:81#direct_member@user:529#..."),
					tuple.MustParse("group:81#direct_member@user:7#..."),
					tuple.MustParse("group:81#parent@group:1#..."),
					tuple.MustParse("supercontainer:22#direct_member@user:279#..."),
					tuple.MustParse("supercontainer:22#direct_member@user:438#..."),
					tuple.MustParse("supercontainer:22#direct_member@user:472#..."),
					tuple.MustParse("supercontainer:22#direct_member@user:485#..."),
					tuple.MustParse("supercontainer:22#direct_member@user:489#..."),
					tuple.MustParse("supercontainer:22#direct_member@user:526#..."),
					tuple.MustParse("supercontainer:22#direct_member@user:536#..."),
					tuple.MustParse("supercontainer:22#direct_member@user:537#..."),
					tuple.MustParse("supercontainer:22#direct_member@user:623#..."),
					tuple.MustParse("supercontainer:22#direct_member@user:672#..."),
					tuple.MustParse("supercontainer:22#owner@group:3#member"),
					tuple.MustParse("supercontainer:22#owner@user:136#..."),
					tuple.MustParse("supercontainer:22#owner@user:19#..."),
					tuple.MustParse("supercontainer:22#owner@user:21#..."),
					tuple.MustParse("supercontainer:22#owner@user:279#..."),
					tuple.MustParse("supercontainer:22#owner@user:3#..."),
					tuple.MustParse("supercontainer:22#owner@user:31#..."),
					tuple.MustParse("supercontainer:22#owner@user:4#..."),
					tuple.MustParse("supercontainer:22#owner@user:439#..."),
					tuple.MustParse("supercontainer:22#owner@user:500#..."),
					tuple.MustParse("supercontainer:22#owner@user:7#..."),
					tuple.MustParse("supercontainer:22#owner@user:9#..."),
					tuple.MustParse("group:3#direct_member@user:135#..."),
					tuple.MustParse("group:3#direct_member@user:160#..."),
					tuple.MustParse("group:3#direct_member@user:17#..."),
					tuple.MustParse("group:3#direct_member@user:19#..."),
					tuple.MustParse("group:3#direct_member@user:272#..."),
					tuple.MustParse("group:3#direct_member@user:3#..."),
					tuple.MustParse("group:3#direct_member@user:4#..."),
					tuple.MustParse("group:3#direct_member@user:439#..."),
					tuple.MustParse("group:3#direct_member@user:7#..."),
					tuple.MustParse("group:3#direct_member@user:9#..."),
					tuple.MustParse("group:1#direct_member@user:12#..."),
					tuple.MustParse("group:1#direct_member@user:13#..."),
					tuple.MustParse("group:1#direct_member@user:135#..."),
					tuple.MustParse("group:1#direct_member@user:14#..."),
					tuple.MustParse("group:1#direct_member@user:21#..."),
					tuple.MustParse("group:1#direct_member@user:320#..."),
					tuple.MustParse("group:1#direct_member@user:321#..."),
					tuple.MustParse("group:1#direct_member@user:322#..."),
					tuple.MustParse("group:1#direct_member@user:323#..."),
					tuple.MustParse("group:1#direct_member@user:34#..."),
					tuple.MustParse("group:1#direct_member@user:397#..."),
					tuple.MustParse("group:1#direct_member@user:46#..."),
					tuple.MustParse("group:1#direct_member@user:50#..."),
					tuple.MustParse("group:1#direct_member@user:662#..."),
					tuple.MustParse("group:1#owner@user:135#..."),
					tuple.MustParse("group:1#owner@user:148#..."),
					tuple.MustParse("group:1#owner@user:160#..."),
					tuple.MustParse("group:1#owner@user:17#..."),
					tuple.MustParse("group:1#owner@user:25#..."),
					tuple.MustParse("group:1#owner@user:279#..."),
					tuple.MustParse("group:1#owner@user:3#..."),
					tuple.MustParse("group:1#owner@user:31#..."),
					tuple.MustParse("group:1#owner@user:4#..."),
					tuple.MustParse("group:1#owner@user:406#..."),
					tuple.MustParse("group:1#owner@user:439#..."),
					tuple.MustParse("group:1#owner@user:7#..."),
					tuple.MustParse("group:1#owner@user:9#..."),
				},
			),

			obj("resource", "someresource"),
			"view",
			"user",
			"",
			[]string{"11", "12", "129", "13", "130", "131", "133", "134", "135", "136", "14", "148", "15", "156", "16", "160", "163", "166", "167", "17", "18", "19", "20", "21", "23", "24", "244", "25", "26", "262", "264", "265", "267", "268", "269", "27", "272", "279", "285", "286", "287", "298", "3", "30", "31", "310", "317", "318", "32", "320", "321", "322", "323", "324", "33", "34", "341", "342", "343", "349", "357", "361", "371", "378", "382", "388", "397", "4", "406", "410", "411", "430", "437", "438", "439", "440", "446", "448", "451", "452", "453", "456", "458", "459", "46", "462", "470", "471", "474", "475", "476", "477", "478", "480", "481", "485", "486", "487", "488", "490", "494", "496", "50", "500", "506", "508", "513", "514", "518", "528", "529", "530", "537", "545", "614", "616", "619", "620", "621", "622", "624", "625", "626", "629", "630", "633", "635", "644", "645", "646", "647", "649", "651", "652", "653", "656", "657", "662", "672", "680", "687", "690", "691", "698", "699", "7", "700", "9"},
		},
	}

	for _, delta := range testTimedeltas {
		delta := delta
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, limit := range []int{0, 5, 10, 15, 104, 572} {
				limit := limit
				t.Run(fmt.Sprintf("limit%d_", limit), func(t *testing.T) {
					for _, tc := range testCases {
						tc := tc
						t.Run(tc.name, func(t *testing.T) {
							req := require.New(t)
							conn, cleanup, _, revision := testserver.NewTestServer(req, testTimedeltas[0], memdb.DisableGC, true,
								func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
									return tf.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, require)
								})

							client := v1.NewPermissionsServiceClient(conn)
							t.Cleanup(func() {
								goleak.VerifyNone(t, goleak.IgnoreCurrent())
							})
							t.Cleanup(cleanup)

							var currentCursor *v1.Cursor
							foundObjectIds := mapz.NewSet[string]()

							for i := 0; i < 500; i++ {
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
									OptionalConcreteLimit: uint32(limit),
									OptionalCursor:        currentCursor,
								}, grpc.Trailer(&trailer))

								req.NoError(err)
								var resolvedObjectIds []string
								existingCursor := currentCursor
								for {
									resp, err := lookupClient.Recv()
									if errors.Is(err, io.EOF) {
										break
									}

									req.NoError(err)

									subjectID := resp.Subject.SubjectObjectId
									if resp.Subject.PartialCaveatInfo != nil {
										missingContext := slices.Clone(resp.Subject.PartialCaveatInfo.MissingRequiredContext)
										sort.Strings(missingContext)
										subjectID = fmt.Sprintf("%v needs [%s]", subjectID, strings.Join(missingContext, ","))
									}

									resolvedObjectIds = append(resolvedObjectIds, subjectID)
									foundObjectIds.Add(subjectID)
									currentCursor = resp.AfterResultCursor

									if len(resp.ExcludedSubjects) > 0 {
										for _, excludedSubject := range resp.ExcludedSubjects {
											if excludedSubject.PartialCaveatInfo == nil {
												foundObjectIds.Add(fmt.Sprintf("!%v", excludedSubject.SubjectObjectId))
											} else {
												foundObjectIds.Add(fmt.Sprintf("!(%v needs [%s])", excludedSubject.SubjectObjectId, strings.Join(excludedSubject.PartialCaveatInfo.MissingRequiredContext, ",")))
											}
										}
									}
								}

								if limit > 0 {
									allowedLimit := limit
									if slices.Contains(tc.expectedSubjectIds, "*") {
										allowedLimit++
									}

									req.LessOrEqual(len(resolvedObjectIds), allowedLimit, "starting at cursor %v, found: %v", existingCursor, resolvedObjectIds)
								}

								dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
								req.NoError(err)
								req.GreaterOrEqual(dispatchCount, 0)

								if len(resolvedObjectIds) == 0 || limit == 0 {
									break
								}
							}

							allResolvedObjectIds := foundObjectIds.AsSlice()

							sort.Strings(tc.expectedSubjectIds)
							sort.Strings(allResolvedObjectIds)

							req.Equal(tc.expectedSubjectIds, allResolvedObjectIds)
						})
					}
				})
			}
		})
	}
}
