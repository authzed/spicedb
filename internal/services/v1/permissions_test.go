package v1_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"github.com/authzed/authzed-go/pkg/responsemeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/ccoveille/go-safecast"
	"github.com/scylladb/go-set"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/shared"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	pgraph "github.com/authzed/spicedb/pkg/graph"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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
									require.Equal(tuple.V1StringObjectRef(tc.resource), tuple.V1StringObjectRef(debugInfo.Check.Resource))
									require.Equal(tc.permission, debugInfo.Check.Permission)
									require.Equal(tuple.V1StringSubjectRef(tc.subject), tuple.V1StringSubjectRef(debugInfo.Check.Subject))
									require.NotEmpty(debugInfo.Check.Source, "source in debug trace is empty")
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

func TestCheckPermissionWithWildcardSubject(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(require, testTimedeltas[0], memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	ctx := context.Background()
	ctx = requestmeta.AddRequestHeaders(ctx, requestmeta.RequestDebugInformation)

	_, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		Resource:   obj("document", "masterplan"),
		Permission: "view",
		Subject:    sub("user", "*", ""),
	})

	require.Error(err)
	require.ErrorContains(err, "invalid argument: cannot perform check on wildcard subject")
	grpcutil.RequireStatus(t, codes.InvalidArgument, err)
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

func TestCheckPermissionWithDebugInfoInError(t *testing.T) {
	req := require.New(t)
	conn, cleanup, _, revision := testserver.NewTestServer(req, testTimedeltas[0], memdb.DisableGC, true,
		func(ds datastore.Datastore, assertions *require.Assertions) (datastore.Datastore, datastore.Revision) {
			return tf.DatastoreFromSchemaAndTestRelationships(
				ds,
				`definition user {}
				
				 definition document {
					relation viewer: user | document#view
					permission view = viewer
				 }
				`,
				[]tuple.Relationship{
					tuple.MustParse("document:doc1#viewer@user:tom"),
					tuple.MustParse("document:doc1#viewer@document:doc2#view"),
					tuple.MustParse("document:doc2#viewer@document:doc3#view"),
					tuple.MustParse("document:doc3#viewer@document:doc1#view"),
				},
				assertions,
			)
		},
	)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	ctx := context.Background()
	ctx = requestmeta.AddRequestHeaders(ctx, requestmeta.RequestDebugInformation)

	_, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		Resource:   obj("document", "doc1"),
		Permission: "view",
		Subject:    sub("user", "fred", ""),
	})

	req.Error(err)
	grpcutil.RequireStatus(t, codes.ResourceExhausted, err)

	s, ok := status.FromError(err)
	req.True(ok)

	foundDebugInfo := false
	for _, d := range s.Details() {
		if errInfo, ok := d.(*errdetails.ErrorInfo); ok {
			req.NotNil(errInfo.Metadata)
			req.NotNil(errInfo.Metadata[string(spiceerrors.DebugTraceErrorDetailsKey)])
			req.NotEmpty(errInfo.Metadata[string(spiceerrors.DebugTraceErrorDetailsKey)])

			debugInfo := &v1.DebugInformation{}
			err = prototext.Unmarshal([]byte(errInfo.Metadata[string(spiceerrors.DebugTraceErrorDetailsKey)]), debugInfo)
			req.NoError(err)

			req.Equal(1, len(debugInfo.Check.GetSubProblems().Traces))
			req.Equal(1, len(debugInfo.Check.GetSubProblems().Traces[0].GetSubProblems().Traces))

			foundDebugInfo = true
		}
	}

	req.True(foundDebugInfo)
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
			8,
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
					for _, useV2 := range []bool{false, true} {
						useV2 := useV2
						t.Run(fmt.Sprintf("v2:%v", useV2), func(t *testing.T) {
							require := require.New(t)
							conn, cleanup, _, revision := testserver.NewTestServerWithConfig(
								require,
								delta,
								memdb.DisableGC,
								true,
								testserver.ServerConfig{
									MaxUpdatesPerWrite:         1000,
									MaxPreconditionsCount:      1000,
									StreamingAPITimeout:        30 * time.Second,
									MaxRelationshipContextSize: 25000,
								},
								tf.StandardDatastoreWithData,
							)
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

func DS(objectType string, objectID string, objectRelation string) *core.DirectSubject {
	return &core.DirectSubject{
		Subject: tuple.CoreONR(objectType, objectID, objectRelation),
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

func TestLookupSubjectsWithConcreteLimit(t *testing.T) {
	conn, cleanup, _, revision := testserver.NewTestServer(require.New(t), testTimedeltas[0], memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	ctx := context.Background()

	lsClient, err := client.LookupSubjects(ctx, &v1.LookupSubjectsRequest{
		Resource: &v1.ObjectReference{
			ObjectType: "document",
			ObjectId:   "masterplan",
		},
		Permission:        "view",
		SubjectObjectType: "user",
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
			},
		},
		OptionalConcreteLimit: 2,
	})
	require.NoError(t, err)
	for {
		_, err := lsClient.Recv()
		require.Error(t, err)
		grpcutil.RequireStatus(t, codes.Unimplemented, err)
		return
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
		Resource:   obj("document", "caveatedplan"),
		Permission: "caveated_viewer",
		Subject:    sub("user", "caveatedguy", ""),
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
				[]tuple.Relationship{tuple.MustParse("document:firstdoc#viewer@user:tom[somecaveat]")},
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
			`, []tuple.Relationship{
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
			`, []tuple.Relationship{
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
			`, []tuple.Relationship{
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
								uintLimit, err := safecast.ToUint32(limit)
								require.NoError(err)
								lookupClient, err := client.LookupResources(context.Background(), &v1.LookupResourcesRequest{
									ResourceObjectType: tc.objectType,
									Permission:         tc.permission,
									Subject:            tc.subject,
									Consistency: &v1.Consistency{
										Requirement: &v1.Consistency_AtLeastAsFresh{
											AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
										},
									},
									OptionalLimit:  uintLimit,
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
			`, []tuple.Relationship{
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
	defer goleak.VerifyNone(t, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)

	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.StandardDatastoreWithCaveatedData)
	client := v1.NewPermissionsServiceClient(conn)
	defer cleanup()

	testCases := []struct {
		name     string
		requests []string
		response []bulkCheckTest
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
		},
		{
			name: "different caveat context is not clustered",
			requests: []string{
				`document:caveatedplan#caveated_viewer@user:caveatedguy[test:{"secret": "1234"}]`,
				`document:caveatedplan#caveated_viewer@user:caveatedguy[test:{"secret": "4321"}]`,
				`document:caveatedplan#caveated_viewer@user:caveatedguy`,
			},
			response: []bulkCheckTest{
				{
					req:  `document:caveatedplan#caveated_viewer@user:caveatedguy[test:{"secret": "1234"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
				},
				{
					req:  `document:caveatedplan#caveated_viewer@user:caveatedguy[test:{"secret": "4321"}]`,
					resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
				},
				{
					req:     `document:caveatedplan#caveated_viewer@user:caveatedguy`,
					resp:    v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
					partial: []string{"secret"},
				},
			},
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
		},
		{
			name: "chunking test",
			requests: (func() []string {
				toReturn := make([]string, 0, defaultFilterMaximumIDCountForTest+5)
				for i := 0; i < int(defaultFilterMaximumIDCountForTest+5); i++ {
					toReturn = append(toReturn, fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i))
				}

				return toReturn
			})(),
			response: (func() []bulkCheckTest {
				toReturn := make([]bulkCheckTest, 0, defaultFilterMaximumIDCountForTest+5)
				for i := 0; i < int(defaultFilterMaximumIDCountForTest+5); i++ {
					toReturn = append(toReturn, bulkCheckTest{
						req:  fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i),
						resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					})
				}

				return toReturn
			})(),
		},
		{
			name: "chunking test with errors",
			requests: (func() []string {
				toReturn := make([]string, 0, defaultFilterMaximumIDCountForTest+6)
				toReturn = append(toReturn, `nondoc:masterplan#view@user:eng_lead`)

				for i := 0; i < int(defaultFilterMaximumIDCountForTest+5); i++ {
					toReturn = append(toReturn, fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i))
				}

				return toReturn
			})(),
			response: (func() []bulkCheckTest {
				toReturn := make([]bulkCheckTest, 0, defaultFilterMaximumIDCountForTest+6)
				toReturn = append(toReturn, bulkCheckTest{
					req: `nondoc:masterplan#view@user:eng_lead`,
					err: namespace.NewNamespaceNotFoundErr("nondoc"),
				})

				for i := 0; i < int(defaultFilterMaximumIDCountForTest+5); i++ {
					toReturn = append(toReturn, bulkCheckTest{
						req:  fmt.Sprintf(`document:masterplan-%d#view@user:eng_lead`, i),
						resp: v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					})
				}

				return toReturn
			})(),
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
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			for _, withTracing := range []bool{true, false} {
				t.Run(fmt.Sprintf("withTracing=%t", withTracing), func(t *testing.T) {
					req := v1.CheckBulkPermissionsRequest{
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
						},
						Items:       make([]*v1.CheckBulkPermissionsRequestItem, 0, len(tt.requests)),
						WithTracing: withTracing,
					}

					for _, r := range tt.requests {
						req.Items = append(req.Items, mustRelToCheckBulkRequestItem(r))
					}

					expected := make([]*v1.CheckBulkPermissionsPair, 0, len(tt.response))
					for _, r := range tt.response {
						reqRel, err := tuple.ParseV1Rel(r.req)
						require.NoError(t, err)

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

					if withTracing {
						for index, pair := range actual.Pairs {
							if pair.GetItem() != nil {
								parsed := tuple.MustParse(tt.requests[index])
								require.NotNil(t, pair.GetItem().DebugTrace, "missing debug trace in response for item %v", pair.GetItem())
								require.True(t, pair.GetItem().DebugTrace.Check != nil, "missing check trace in response for item %v", pair.GetItem())
								require.Equal(t, parsed.Resource.ObjectID, pair.GetItem().DebugTrace.Check.Resource.ObjectId, "resource in debug trace does not match")
								require.NotEmpty(t, pair.GetItem().DebugTrace.Check.TraceOperationId, "trace operation ID in debug trace is empty")
								require.NotEmpty(t, pair.GetItem().DebugTrace.Check.Source, "source in debug trace is empty")
							}
						}
					} else {
						testutil.RequireProtoSlicesEqual(t, expected, actual.Pairs, nil, "response bulk check pairs did not match")
					}
				})
			}
		})
	}
}

func mustRelToCheckBulkRequestItem(rel string) *v1.CheckBulkPermissionsRequestItem {
	r, err := tuple.ParseV1Rel(rel)
	if err != nil {
		panic(err)
	}

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

func TestImportBulkRelationships(t *testing.T) {
	testCases := []struct {
		name       string
		batchSize  func() uint64
		numBatches int
	}{
		{"one small batch", constBatch(1), 1},
		{"one big batch", constBatch(10_000), 1},
		{"many small batches", constBatch(5), 1_000},
		{"one empty batch", constBatch(0), 1},
		{"small random batches", randomBatch(1, 10), 100},
		{"big random batches", randomBatch(1_000, 3_000), 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, withTrait := range []string{"", "caveated_viewer", "expiring_viewer"} {
				withTrait := withTrait
				t.Run(fmt.Sprintf("withTrait=%s", withTrait), func(t *testing.T) {
					require := require.New(t)

					conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithSchema)
					client := v1.NewPermissionsServiceClient(conn)
					t.Cleanup(cleanup)

					ctx := context.Background()

					writer, err := client.ImportBulkRelationships(ctx)
					require.NoError(err)

					var expectedTotal uint64
					for batchNum := 0; batchNum < tc.numBatches; batchNum++ {
						batchSize := tc.batchSize()
						batch := make([]*v1.Relationship, 0, batchSize)

						for i := uint64(0); i < batchSize; i++ {
							if withTrait == "caveated_viewer" {
								batch = append(batch, mustRelWithCaveatAndContext(
									tf.DocumentNS.Name,
									strconv.Itoa(batchNum)+"_"+strconv.FormatUint(i, 10),
									"caveated_viewer",
									tf.UserNS.Name,
									strconv.FormatUint(i, 10),
									"",
									"test",
									map[string]any{"secret": strconv.FormatUint(i, 10)},
								))
							} else if withTrait == "expiring_viewer" {
								batch = append(batch, relWithExpiration(
									tf.DocumentNS.Name,
									strconv.Itoa(batchNum)+"_"+strconv.FormatUint(i, 10),
									"expiring_viewer",
									tf.UserNS.Name,
									strconv.FormatUint(i, 10),
									"",
									time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC),
								))
							} else {
								batch = append(batch, rel(
									tf.DocumentNS.Name,
									strconv.Itoa(batchNum)+"_"+strconv.FormatUint(i, 10),
									"viewer",
									tf.UserNS.Name,
									strconv.FormatUint(i, 10),
									"",
								))
							}
						}

						err := writer.Send(&v1.ImportBulkRelationshipsRequest{
							Relationships: batch,
						})
						require.NoError(err)

						expectedTotal += batchSize
					}

					resp, err := writer.CloseAndRecv()
					require.NoError(err)
					require.Equal(expectedTotal, resp.NumLoaded)

					readerClient := v1.NewPermissionsServiceClient(conn)
					stream, err := readerClient.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
						RelationshipFilter: &v1.RelationshipFilter{
							ResourceType: tf.DocumentNS.Name,
						},
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
						},
					})
					require.NoError(err)

					var readBack uint64
					var res *v1.ReadRelationshipsResponse
					for _, err = stream.Recv(); err == nil; res, err = stream.Recv() {
						readBack++
						if res == nil {
							continue
						}

						if withTrait == "caveated_viewer" {
							require.NotNil(res.Relationship.OptionalCaveat)
							require.Equal("test", res.Relationship.OptionalCaveat.CaveatName)
						} else if withTrait == "expiring_viewer" {
							require.NotNil(res.Relationship.OptionalExpiresAt)
						} else {
							require.Nil(res.Relationship.OptionalCaveat)
						}
					}
					require.ErrorIs(err, io.EOF)
					require.Equal(expectedTotal, readBack)
				})
			}
		})
	}
}

func TestExportBulkRelationshipsBeyondAllowedLimit(t *testing.T) {
	require := require.New(t)
	conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	resp, err := client.ExportBulkRelationships(context.Background(), &v1.ExportBulkRelationshipsRequest{
		OptionalLimit: 10000005,
	})
	require.NoError(err)

	_, err = resp.Recv()
	require.Error(err)
	require.Contains(err.Error(), "provided limit 10000005 is greater than maximum allowed of 100000")
}

func TestExportBulkRelationships(t *testing.T) {
	conn, cleanup, _, _ := testserver.NewTestServer(require.New(t), 0, memdb.DisableGC, true, tf.StandardDatastoreWithSchema)
	client := v1.NewPermissionsServiceClient(conn)
	t.Cleanup(cleanup)

	nsAndRels := []struct {
		namespace string
		relation  string
	}{
		{tf.DocumentNS.Name, "viewer"},
		{tf.FolderNS.Name, "viewer"},
		{tf.DocumentNS.Name, "owner"},
		{tf.FolderNS.Name, "owner"},
		{tf.DocumentNS.Name, "editor"},
		{tf.FolderNS.Name, "editor"},
		{tf.DocumentNS.Name, "caveated_viewer"},
		{tf.DocumentNS.Name, "expiring_viewer"},
	}

	totalToWrite := 1_000
	expectedRels := set.NewStringSetWithSize(totalToWrite)
	batch := make([]*v1.Relationship, totalToWrite)
	for i := range batch {
		nsAndRel := nsAndRels[i%len(nsAndRels)]
		v1rel := relationshipForBulkTesting(nsAndRel, i)
		batch[i] = v1rel
		expectedRels.Add(tuple.MustV1RelString(v1rel))
	}

	ctx := context.Background()
	writer, err := client.ImportBulkRelationships(ctx)
	require.NoError(t, err)

	require.NoError(t, writer.Send(&v1.ImportBulkRelationshipsRequest{
		Relationships: batch,
	}))

	resp, err := writer.CloseAndRecv()
	require.NoError(t, err)
	numLoaded, err := safecast.ToInt(resp.NumLoaded)
	require.NoError(t, err)
	require.Equal(t, totalToWrite, numLoaded)

	testCases := []struct {
		batchSize      uint32
		paginateEveryN int
	}{
		{1_000, math.MaxInt},
		{10, math.MaxInt},
		{1_000, 1},
		{100, 5},
		{97, 7},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d-%d", tc.batchSize, tc.paginateEveryN), func(t *testing.T) {
			require := require.New(t)

			var totalRead int
			remainingRels := expectedRels.Copy()
			require.Equal(totalToWrite, expectedRels.Size())
			var cursor *v1.Cursor

			var done bool
			for !done {
				streamCtx, cancel := context.WithCancel(ctx)

				stream, err := client.ExportBulkRelationships(streamCtx, &v1.ExportBulkRelationshipsRequest{
					OptionalLimit:  tc.batchSize,
					OptionalCursor: cursor,
				})
				require.NoError(err)

				for i := 0; i < tc.paginateEveryN; i++ {
					batch, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						done = true
						break
					}

					require.NoError(err)
					require.LessOrEqual(uint64(len(batch.Relationships)), uint64(tc.batchSize))
					require.NotNil(batch.AfterResultCursor)
					require.NotEmpty(batch.AfterResultCursor.Token)

					cursor = batch.AfterResultCursor
					totalRead += len(batch.Relationships)

					for _, rel := range batch.Relationships {
						remainingRels.Remove(tuple.MustV1RelString(rel))
					}
				}

				cancel()
			}

			require.Equal(totalToWrite, totalRead)
			require.True(remainingRels.IsEmpty(), "rels were not exported %#v", remainingRels.List())
		})
	}
}

func TestExportBulkRelationshipsWithFilter(t *testing.T) {
	testCases := []struct {
		name          string
		filter        *v1.RelationshipFilter
		expectedCount int
	}{
		{
			"basic filter",
			&v1.RelationshipFilter{
				ResourceType: tf.DocumentNS.Name,
			},
			625,
		},
		{
			"filter by resource ID",
			&v1.RelationshipFilter{
				OptionalResourceId: "12",
			},
			1,
		},
		{
			"filter by resource ID prefix",
			&v1.RelationshipFilter{
				OptionalResourceIdPrefix: "1",
			},
			111,
		},
		{
			"filter by resource ID prefix and resource type",
			&v1.RelationshipFilter{
				ResourceType:             tf.DocumentNS.Name,
				OptionalResourceIdPrefix: "1",
			},
			69,
		},
		{
			"filter by invalid resource type",
			&v1.RelationshipFilter{
				ResourceType: "invalid",
			},
			0,
		},
	}

	batchSize := uint32(14)

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithSchema)
			client := v1.NewPermissionsServiceClient(conn)
			t.Cleanup(cleanup)

			nsAndRels := []struct {
				namespace string
				relation  string
			}{
				{tf.DocumentNS.Name, "viewer"},
				{tf.FolderNS.Name, "viewer"},
				{tf.DocumentNS.Name, "owner"},
				{tf.FolderNS.Name, "owner"},
				{tf.DocumentNS.Name, "editor"},
				{tf.FolderNS.Name, "editor"},
				{tf.DocumentNS.Name, "caveated_viewer"},
				{tf.DocumentNS.Name, "expiring_viewer"},
			}

			expectedRels := set.NewStringSetWithSize(1000)
			batch := make([]*v1.Relationship, 1000)
			for i := range batch {
				nsAndRel := nsAndRels[i%len(nsAndRels)]
				v1rel := relationshipForBulkTesting(nsAndRel, i)
				batch[i] = v1rel

				if tc.filter != nil {
					filter, err := datastore.RelationshipsFilterFromPublicFilter(tc.filter)
					require.NoError(err)
					if !filter.Test(tuple.FromV1Relationship(v1rel)) {
						continue
					}
				}

				expectedRels.Add(tuple.MustV1RelString(v1rel))
			}

			require.Equal(tc.expectedCount, expectedRels.Size())

			ctx := context.Background()
			writer, err := client.ImportBulkRelationships(ctx)
			require.NoError(err)

			require.NoError(writer.Send(&v1.ImportBulkRelationshipsRequest{
				Relationships: batch,
			}))

			_, err = writer.CloseAndRecv()
			require.NoError(err)

			var totalRead uint64
			remainingRels := expectedRels.Copy()
			var cursor *v1.Cursor

			foundRels := mapz.NewSet[string]()
			for {
				streamCtx, cancel := context.WithCancel(ctx)

				stream, err := client.ExportBulkRelationships(streamCtx, &v1.ExportBulkRelationshipsRequest{
					OptionalRelationshipFilter: tc.filter,
					OptionalLimit:              batchSize,
					OptionalCursor:             cursor,
				})
				require.NoError(err)

				batch, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					cancel()
					break
				}

				require.NoError(err)
				relLength, err := safecast.ToUint32(len(batch.Relationships))
				require.NoError(err)
				require.LessOrEqual(relLength, batchSize)
				require.NotNil(batch.AfterResultCursor)
				require.NotEmpty(batch.AfterResultCursor.Token)

				cursor = batch.AfterResultCursor
				totalRead += uint64(len(batch.Relationships))

				for _, rel := range batch.Relationships {
					if tc.filter != nil {
						filter, err := datastore.RelationshipsFilterFromPublicFilter(tc.filter)
						require.NoError(err)
						require.True(filter.Test(tuple.FromV1Relationship(rel)), "relationship did not match filter: %s", rel)
					}

					require.True(remainingRels.Has(tuple.MustV1RelString(rel)), "relationship was not expected or was repeated: %s", rel)
					remainingRels.Remove(tuple.MustV1RelString(rel))
					foundRels.Add(tuple.MustV1RelString(rel))
				}

				cancel()
			}

			// These are statically defined.
			expectedCount, _ := safecast.ToUint64(tc.expectedCount)
			require.Equal(expectedCount, totalRead, "found: %v", foundRels.AsSlice())
			require.True(remainingRels.IsEmpty(), "rels were not exported %#v", remainingRels.List())
		})
	}
}
