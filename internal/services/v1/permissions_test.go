package v1_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/authzed/authzed-go/pkg/responsemeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	pgraph "github.com/authzed/spicedb/pkg/graph"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

var testTimedeltas = []time.Duration{0, 1 * time.Second}

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})

	// Set this to Trace to dump log statements in tests.
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

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
			"viewer",
			sub("user", "eng_lead", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"viewer",
			sub("user", "product_manager", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"viewer",
			sub("user", "chief_financial_officer", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "healthplan"),
			"viewer",
			sub("user", "chief_financial_officer", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"viewer",
			sub("user", "auditor", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "companyplan"),
			"viewer",
			sub("user", "auditor", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"viewer",
			sub("user", "vp_product", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"viewer",
			sub("user", "legal", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "companyplan"),
			"viewer",
			sub("user", "legal", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"viewer",
			sub("user", "owner", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "companyplan"),
			"viewer",
			sub("user", "owner", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"viewer",
			sub("user", "villain", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"viewer",
			sub("user", "unknowngal", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			codes.OK,
		},

		{
			obj("document", "masterplan"),
			"viewer_and_editor",
			sub("user", "eng_lead", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "specialplan"),
			"viewer_and_editor",
			sub("user", "multiroleguy", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"viewer_and_editor",
			sub("user", "missingrolegal", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "specialplan"),
			"viewer_and_editor_derived",
			sub("user", "multiroleguy", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
			codes.OK,
		},
		{
			obj("document", "masterplan"),
			"viewer_and_editor_derived",
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
			"viewer_and_editor_derived",
			sub("user", "someuser", "invalidrelation"),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.FailedPrecondition,
		},
		{
			obj("invalidnamespace", "masterplan"),
			"viewer_and_editor_derived",
			sub("user", "someuser", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.FailedPrecondition,
		},
		{
			obj("document", "masterplan"),
			"viewer_and_editor_derived",
			sub("invalidnamespace", "someuser", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.FailedPrecondition,
		},
		{
			obj("document", "*"),
			"viewer_and_editor_derived",
			sub("invalidnamespace", "someuser", ""),
			v1.CheckPermissionResponse_PERMISSIONSHIP_UNSPECIFIED,
			codes.InvalidArgument,
		},
		{
			obj("document", "something"),
			"viewer",
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
					conn, cleanup, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
					client := v1.NewPermissionsServiceClient(conn)
					t.Cleanup(cleanup)

					var trailer metadata.MD
					checkResp, err := client.CheckPermission(context.Background(), &v1.CheckPermissionRequest{
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_AtLeastAsFresh{
								AtLeastAsFresh: zedtoken.NewFromRevision(revision),
							},
						},
						Resource:   tc.resource,
						Permission: tc.permission,
						Subject:    tc.subject,
					}, grpc.Trailer(&trailer))

					if tc.expectedStatus == codes.OK {
						require.Equal(tc.expected, checkResp.Permissionship)

						dispatchCount, err := responsemeta.GetIntResponseTrailerMetadata(trailer, responsemeta.DispatchedOperationsCount)
						require.NoError(err)
						require.GreaterOrEqual(dispatchCount, 0)
					} else {
						grpcutil.RequireStatus(t, tc.expectedStatus, err)
					}
				})
			}
		})
	}
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
			"document", "viewer",
			sub("user", "eng_lead", ""),
			[]string{"masterplan"},
			codes.OK,
		},
		{
			"document", "viewer",
			sub("user", "product_manager", ""),
			[]string{"masterplan"},
			codes.OK,
		},
		{
			"document", "viewer",
			sub("user", "chief_financial_officer", ""),
			[]string{"masterplan", "healthplan"},
			codes.OK,
		},
		{
			"document", "viewer",
			sub("user", "auditor", ""),
			[]string{"masterplan", "companyplan"},
			codes.OK,
		},
		{
			"document", "viewer",
			sub("user", "vp_product", ""),
			[]string{"masterplan"},
			codes.OK,
		},
		{
			"document", "viewer",
			sub("user", "legal", ""),
			[]string{"masterplan", "companyplan"},
			codes.OK,
		},
		{
			"document", "viewer",
			sub("user", "owner", ""),
			[]string{"masterplan", "companyplan"},
			codes.OK,
		},
		{
			"document", "viewer",
			sub("user", "villain", ""),
			nil,
			codes.OK,
		},
		{
			"document", "viewer",
			sub("user", "unknowngal", ""),
			nil,
			codes.OK,
		},

		{
			"document", "viewer_and_editor",
			sub("user", "eng_lead", ""),
			nil,
			codes.OK,
		},
		{
			"document", "viewer_and_editor",
			sub("user", "multiroleguy", ""),
			[]string{"specialplan"},
			codes.OK,
		},
		{
			"document", "viewer_and_editor",
			sub("user", "missingrolegal", ""),
			nil,
			codes.OK,
		},
		{
			"document", "viewer_and_editor_derived",
			sub("user", "multiroleguy", ""),
			[]string{"specialplan"},
			codes.OK,
		},
		{
			"document", "viewer_and_editor_derived",
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
			"document", "viewer_and_editor_derived",
			sub("user", "someuser", "invalidrelation"),
			[]string{},
			codes.FailedPrecondition,
		},
		{
			"invalidnamespace", "viewer_and_editor_derived",
			sub("user", "someuser", ""),
			[]string{},
			codes.FailedPrecondition,
		},
		{
			"document", "viewer_and_editor_derived",
			sub("invalidnamespace", "someuser", ""),
			[]string{},
			codes.FailedPrecondition,
		},
		{
			"document", "viewer_and_editor_derived",
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
					conn, cleanup, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
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
								AtLeastAsFresh: zedtoken.NewFromRevision(revision),
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
		{"document", "masterplan", "viewer", 7, codes.OK},
		{"document", "masterplan", "fakerelation", 0, codes.FailedPrecondition},
		{"fake", "masterplan", "owner", 0, codes.FailedPrecondition},
		{"document", "", "owner", 1, codes.InvalidArgument},
	}

	for _, delta := range testTimedeltas {
		t.Run(fmt.Sprintf("fuzz%d", delta/time.Millisecond), func(t *testing.T) {
			for _, tc := range testCases {
				t.Run(fmt.Sprintf("%s:%s#%s", tc.startObjectType, tc.startObjectID, tc.startPermission), func(t *testing.T) {
					require := require.New(t)
					conn, cleanup, revision := testserver.NewTestServer(require, delta, memdb.DisableGC, true, tf.StandardDatastoreWithData)
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
								AtLeastAsFresh: zedtoken.NewFromRevision(revision),
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

func TestTranslateExpansionTree(t *testing.T) {
	var (
		ONR  = tuple.ObjectAndRelation
		User = tuple.User
	)

	table := []struct {
		name  string
		input *core.RelationTupleTreeNode
	}{
		{"simple leaf", pgraph.Leaf(nil, User(ONR("user", "user1", "...")))},
		{
			"simple union",
			pgraph.Union(nil,
				pgraph.Leaf(nil, User(ONR("user", "user1", "..."))),
				pgraph.Leaf(nil, User(ONR("user", "user2", "..."))),
				pgraph.Leaf(nil, User(ONR("user", "user3", "..."))),
			),
		},
		{
			"simple intersection",
			pgraph.Intersection(nil,
				pgraph.Leaf(nil,
					User(ONR("user", "user1", "...")),
					User(ONR("user", "user2", "...")),
				),
				pgraph.Leaf(nil,
					User(ONR("user", "user2", "...")),
					User(ONR("user", "user3", "...")),
				),
				pgraph.Leaf(nil,
					User(ONR("user", "user2", "...")),
					User(ONR("user", "user4", "...")),
				),
			),
		},
		{
			"empty intersection",
			pgraph.Intersection(nil,
				pgraph.Leaf(nil,
					User(ONR("user", "user1", "...")),
					User(ONR("user", "user2", "...")),
				),
				pgraph.Leaf(nil,
					User(ONR("user", "user3", "...")),
					User(ONR("user", "user4", "...")),
				),
			),
		},
		{
			"simple exclusion",
			pgraph.Exclusion(nil,
				pgraph.Leaf(nil,
					User(ONR("user", "user1", "...")),
					User(ONR("user", "user2", "...")),
				),
				pgraph.Leaf(nil, User(ONR("user", "user2", "..."))),
				pgraph.Leaf(nil, User(ONR("user", "user3", "..."))),
			),
		},
		{
			"empty exclusion",
			pgraph.Exclusion(nil,
				pgraph.Leaf(nil,
					User(ONR("user", "user1", "...")),
					User(ONR("user", "user2", "...")),
				),
				pgraph.Leaf(nil, User(ONR("user", "user1", "..."))),
				pgraph.Leaf(nil, User(ONR("user", "user2", "..."))),
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
