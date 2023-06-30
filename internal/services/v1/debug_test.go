package v1_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"github.com/authzed/authzed-go/pkg/responsemeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

type debugCheckRequest struct {
	resource      *v1.ObjectReference
	permission    string
	subject       *v1.SubjectReference
	caveatContext map[string]any
}

type rda func(req *require.Assertions, debugInfo *v1.DebugInformation)

type debugCheckInfo struct {
	name                           string
	checkRequest                   debugCheckRequest
	expectedPermission             v1.CheckPermissionResponse_Permissionship
	expectedMinimumSubProblemCount int
	runDebugAssertions             []rda
}

func expectDebugFrames(permissionNames ...string) rda {
	return func(req *require.Assertions, debugInfo *v1.DebugInformation) {
		found := mapz.NewSet[string]()
		for _, sp := range debugInfo.Check.GetSubProblems().Traces {
			for _, permissionName := range permissionNames {
				if sp.Permission == permissionName {
					found.Add(permissionName)
				}
			}
		}

		foundNames := found.AsSlice()
		sort.Strings(permissionNames)
		sort.Strings(foundNames)

		req.Equal(permissionNames, foundNames, "missing expected subproblem(s)")
	}
}

func expectCaveat(caveatExpression string) rda {
	return func(req *require.Assertions, debugInfo *v1.DebugInformation) {
		req.Equal(caveatExpression, debugInfo.Check.CaveatEvaluationInfo.Expression)
	}
}

func expectMissingContext(context ...string) rda {
	sort.Strings(context)
	return func(req *require.Assertions, debugInfo *v1.DebugInformation) {
		missing := debugInfo.Check.CaveatEvaluationInfo.PartialCaveatInfo.MissingRequiredContext
		sort.Strings(missing)
		req.Equal(context, missing)
	}
}

func findFrame(checkTrace *v1.CheckDebugTrace, resourceType string, permissionName string) *v1.CheckDebugTrace {
	if checkTrace.Resource.ObjectType == resourceType && checkTrace.Permission == permissionName {
		return checkTrace
	}

	subProblems := checkTrace.GetSubProblems()
	if subProblems != nil {
		for _, sp := range subProblems.Traces {
			found := findFrame(sp, resourceType, permissionName)
			if found != nil {
				return found
			}
		}
	}
	return nil
}

func TestCheckPermissionWithDebug(t *testing.T) {
	tcs := []struct {
		name          string
		schema        string
		relationships []*core.RelationTuple
		toTest        []debugCheckInfo
	}{
		{
			"basic debug",
			`definition user {}
			
			 definition document {
				relation editor: user
				relation viewer: user
				permission edit = editor
				permission view = viewer + edit
			 }
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:first#editor@user:sarah"),
			},
			[]debugCheckInfo{
				{
					"sarah as editor",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						nil,
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
					1,
					[]rda{expectDebugFrames("editor")},
				},
				{
					"tom as viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "tom", ""),
						nil,
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
					1,
					[]rda{expectDebugFrames("viewer")},
				},
				{
					"benny as nothing",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "benny", ""),
						nil,
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					2,
					[]rda{expectDebugFrames("viewer", "editor")},
				},
			},
		},
		{
			"caveated debug",
			`definition user {}
			
			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			 definition document {
				relation another: user
				relation viewer: user with somecaveat
				permission view = viewer + another
			 }
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:first#viewer@user:sarah[somecaveat]"),
			},
			[]debugCheckInfo{
				{
					"sarah as viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						map[string]any{
							"somecondition": 42,
						},
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
					1,
					[]rda{expectDebugFrames("viewer"), expectCaveat("somecondition == 42")},
				},
				{
					"sarah as not viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						map[string]any{
							"somecondition": 31,
						},
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					1,
					[]rda{expectDebugFrames("viewer"), expectCaveat("somecondition == 42")},
				},
				{
					"sarah as conditional viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						map[string]any{},
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
					1,
					[]rda{
						expectDebugFrames("viewer"),
						expectCaveat("somecondition == 42"),
						expectMissingContext("somecondition"),
					},
				},
			},
		},
		{
			"batched recursive",
			`definition user {}

			definition folder {
				relation parent: folder
				relation fviewer: user
				permission fview = fviewer + parent->fview
			}
			
			 definition document {
				relation folder: folder
				relation viewer: user
				permission view = viewer + folder->fview
			 }
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:first#folder@folder:f1"),
				tuple.MustParse("document:first#folder@folder:f2"),
				tuple.MustParse("document:first#folder@folder:f3"),
				tuple.MustParse("document:first#folder@folder:f4"),
				tuple.MustParse("document:first#folder@folder:f5"),
				tuple.MustParse("document:first#folder@folder:f6"),
				tuple.MustParse("folder:f1#parent@folder:f1p"),
				tuple.MustParse("folder:f2#parent@folder:f2p"),
				tuple.MustParse("folder:f3#parent@folder:f3p"),
				tuple.MustParse("folder:f4#parent@folder:f4p"),
				tuple.MustParse("folder:f5#parent@folder:f5p"),
				tuple.MustParse("folder:f6#parent@folder:f6p"),
				tuple.MustParse("folder:f6p#fviewer@user:sarah"),
			},
			[]debugCheckInfo{
				{
					"tom as viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "tom", ""),
						nil,
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
					1,
					[]rda{expectDebugFrames("viewer")},
				},
				{
					"sarah as recursive viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						nil,
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
					1,
					[]rda{expectDebugFrames("fview")},
				},
				{
					"benny as not viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "benny", ""),
						nil,
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					2,
					[]rda{
						expectDebugFrames("viewer"),
						func(req *require.Assertions, debugInfo *v1.DebugInformation) {
							// Ensure that all the resource IDs are batched into a single frame.
							found := findFrame(debugInfo.Check, "folder", "fview")
							req.NotNil(found)
							req.Equal(6, len(strings.Split(found.Resource.ObjectId, ",")))

							// Ensure there are no more than 2 subframes, to verify we haven't
							// accidentally fanned out.
							req.LessOrEqual(2, len(found.GetSubProblems().Traces))
						},
					},
				},
			},
		},
		{
			"ip address caveat",
			`definition user {}

			caveat has_valid_ip(user_ip ipaddress, allowed_range string) {
				user_ip.in_cidr(allowed_range)
			}
			
			definition resource {
				relation viewer: user | user with has_valid_ip
			}`,
			[]*core.RelationTuple{
				tuple.MustParse(`resource:first#viewer@user:sarah[has_valid_ip:{"allowed_range":"192.168.0.0/16"}]`),
			},
			[]debugCheckInfo{
				{
					"sarah as viewer",
					debugCheckRequest{
						obj("resource", "first"),
						"viewer",
						sub("user", "sarah", ""),
						map[string]any{
							"user_ip": "192.168.1.100",
						},
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
					0,
					nil,
				},
			},
		},
		{
			"multiple caveated debug",
			`definition user {}
			
			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			caveat anothercaveat(anothercondition string) {
				anothercondition == "hello world"
			}

			definition org {
				relation member: user with somecaveat
			}

			 definition document {
				relation parent: org with anothercaveat
				permission view = parent->member
			 }
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:first#parent@org:someorg[anothercaveat]"),
				tuple.MustParse("org:someorg#member@user:sarah[somecaveat]"),
			},
			[]debugCheckInfo{
				{
					"sarah as viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						map[string]any{
							"anothercondition": "hello world",
							"somecondition":    "42",
						},
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
					1,
					[]rda{expectDebugFrames("member"), expectCaveat(`anothercondition == "hello world" && somecondition == 42`)},
				},
				{
					"sarah as not viewer due to org",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						map[string]any{
							"anothercondition": "hi there",
							"somecondition":    "42",
						},
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					1,
					[]rda{expectDebugFrames("member"), expectCaveat(`anothercondition == "hello world"`)},
				},
				{
					"sarah as not viewer due to viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						map[string]any{
							"anothercondition": "hello world",
							"somecondition":    "41",
						},
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_NO_PERMISSION,
					1,
					[]rda{expectDebugFrames("member"), expectCaveat(`anothercondition == "hello world" && somecondition == 42`)},
				},
				{
					"sarah as partially conditional viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						map[string]any{
							"anothercondition": "hello world",
						},
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
					1,
					[]rda{
						expectDebugFrames("member"),
						expectMissingContext("somecondition"),
					},
				},
				{
					"sarah as fully conditional viewer",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						map[string]any{},
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
					1,
					[]rda{
						expectDebugFrames("member"),
						expectMissingContext("anothercondition"),
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)
			conn, cleanup, _, revision := testserver.NewTestServer(req, testTimedeltas[0], memdb.DisableGC, true,
				func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
					return tf.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, req)
				})

			client := v1.NewPermissionsServiceClient(conn)
			t.Cleanup(cleanup)

			ctx := context.Background()
			ctx = requestmeta.AddRequestHeaders(ctx, requestmeta.RequestDebugInformation)

			for _, stc := range tc.toTest {
				stc := stc
				t.Run(stc.name, func(t *testing.T) {
					req := require.New(t)

					var trailer metadata.MD
					caveatContext, err := structpb.NewStruct(stc.checkRequest.caveatContext)
					req.NoError(err)

					checkResp, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_AtLeastAsFresh{
								AtLeastAsFresh: zedtoken.MustNewFromRevision(revision),
							},
						},
						Resource:   stc.checkRequest.resource,
						Permission: stc.checkRequest.permission,
						Subject:    stc.checkRequest.subject,
						Context:    caveatContext,
					}, grpc.Trailer(&trailer))

					req.NoError(err)
					req.Equal(stc.expectedPermission, checkResp.Permissionship)

					encodedDebugInfo, err := responsemeta.GetResponseTrailerMetadataOrNil(trailer, responsemeta.DebugInformation)
					req.NoError(err)

					req.NotNil(encodedDebugInfo)

					debugInfo := &v1.DebugInformation{}
					err = protojson.Unmarshal([]byte(*encodedDebugInfo), debugInfo)
					req.NoError(err)
					req.NotEmpty(debugInfo.SchemaUsed)

					req.Equal(stc.checkRequest.resource.ObjectType, debugInfo.Check.Resource.ObjectType)
					req.Equal(stc.checkRequest.resource.ObjectId, debugInfo.Check.Resource.ObjectId)
					req.Equal(stc.checkRequest.permission, debugInfo.Check.Permission)

					if debugInfo.Check.GetSubProblems() != nil {
						req.GreaterOrEqual(len(debugInfo.Check.GetSubProblems().Traces), stc.expectedMinimumSubProblemCount, "found traces: %s", prototext.Format(debugInfo.Check))
					} else {
						req.Equal(0, stc.expectedMinimumSubProblemCount)
					}

					for _, rda := range stc.runDebugAssertions {
						rda(req, debugInfo)
					}
				})
			}
		})
	}
}
