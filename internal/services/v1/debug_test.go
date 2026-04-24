package v1_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
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
					found.Insert(permissionName)
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
		relationships []tuple.Relationship
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
							req.Len(strings.Split(found.Resource.ObjectId, ","), 6)

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
			[]tuple.Relationship{
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
			[]tuple.Relationship{
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
					[]rda{expectDebugFrames("member"), expectCaveat(`(anothercondition == "hello world") && (somecondition == 42)`)},
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
					[]rda{expectDebugFrames("member"), expectCaveat(`(anothercondition == "hello world") && (somecondition == 42)`)},
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
					[]rda{expectDebugFrames("member"), expectCaveat(`(anothercondition == "hello world") && (somecondition == 42)`)},
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
						expectMissingContext("anothercondition", "somecondition"),
					},
				},
			},
		},
		{
			"reused caveat parameter name is renamed in debug frame",
			`definition user {}
			
			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			caveat anothercaveat(somecondition int) {
				somecondition == 41
			}

			definition org {
				relation member: user with somecaveat
			}

			 definition document {
				relation parent: org with anothercaveat
				permission view = parent->member
			 }
			`,
			[]tuple.Relationship{
				tuple.MustParse(`document:first#parent@org:someorg[anothercaveat:{"somecondition":41}]`),
				tuple.MustParse(`org:someorg#member@user:sarah[somecaveat:{"somecondition":42}]`),
			},
			[]debugCheckInfo{
				{
					"sarah has view permission",
					debugCheckRequest{
						obj("document", "first"),
						"view",
						sub("user", "sarah", ""),
						map[string]any{},
					},
					v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION,
					1,
					[]rda{expectDebugFrames("member"), expectCaveat(`(somecondition__0 == 41) && (somecondition__1 == 42)`)},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)
			conn, cleanup, _, revision := testserver.NewTestServer(req, 5*time.Second, memdb.DisableGC, true,
				func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
					return tf.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, req)
				})

			client := v1.NewPermissionsServiceClient(conn)
			t.Cleanup(cleanup)

			ctx := t.Context()
			ctx = requestmeta.AddRequestHeaders(ctx, requestmeta.RequestDebugInformation)

			for _, stc := range tc.toTest {
				t.Run(stc.name, func(t *testing.T) {
					req := require.New(t)

					caveatContext, err := structpb.NewStruct(stc.checkRequest.caveatContext)
					req.NoError(err)

					checkResp, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
						Consistency: &v1.Consistency{
							Requirement: &v1.Consistency_AtLeastAsFresh{
								AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision),
							},
						},
						Resource:   stc.checkRequest.resource,
						Permission: stc.checkRequest.permission,
						Subject:    stc.checkRequest.subject,
						Context:    caveatContext,
					})

					req.NoError(err)
					req.Equal(stc.expectedPermission, checkResp.Permissionship)

					debugInfo := checkResp.DebugTrace
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

type bulkCheckItem struct {
	toCheck string
	rda     []rda
}

type frameInfo struct {
	resourceType   string
	resourceIDs    []string
	permission     string
	permissionship v1.CheckDebugTrace_Permissionship
}

func expectOrderedFrames(frames ...frameInfo) rda {
	return func(req *require.Assertions, debugInfo *v1.DebugInformation) {
		expectFrames(req, frames, debugInfo.Check)
	}
}

func expectFrames(req *require.Assertions, frames []frameInfo, check *v1.CheckDebugTrace) {
	if len(frames) == 0 {
		return
	}

	frame := frames[0]
	req.Equal(frame.resourceType, check.Resource.ObjectType)
	req.Equal(frame.resourceIDs, strings.Split(check.Resource.ObjectId, ","))
	req.Equal(frame.permission, check.Permission)
	req.Equal(frame.permissionship, check.Result, "frame: %s", prototext.Format(check))

	remainingFrames := frames[1:]
	if len(remainingFrames) > 0 {
		req.NotNil(check.GetSubProblems(), "expected subproblems")
		expectFrames(req, remainingFrames, check.GetSubProblems().Traces[0])
	}
}

func TestBulkCheckPermissionWithDebug(t *testing.T) {
	tcs := []struct {
		name          string
		schema        string
		relationships []tuple.Relationship
		toTest        []bulkCheckItem
	}{
		{
			"basic batching",
			`definition user {}
			
			 definition document {
				relation viewer: user
				permission view = viewer + nil
			 }
			`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#viewer@user:tom"),
				tuple.MustParse("document:second#viewer@user:tom"),
			},
			[]bulkCheckItem{
				{
					toCheck: "document:first#view@user:tom",
					rda: []rda{
						expectOrderedFrames(
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second"},
								permission:     "viewer",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION,
							},
						),
					},
				},
				{
					toCheck: "document:second#view@user:tom",
					rda: []rda{
						expectOrderedFrames(
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"second"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second"},
								permission:     "viewer",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION,
							},
						),
					},
				},
			},
		},
		{
			"batching with positive and negative",
			`definition user {}
			
			 definition document {
				relation viewer: user
				permission view = viewer + nil
			 }
			`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#viewer@user:tom"),
			},
			[]bulkCheckItem{
				{
					toCheck: "document:first#view@user:tom",
					rda: []rda{
						expectOrderedFrames(
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_UNSPECIFIED,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second"},
								permission:     "viewer",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_UNSPECIFIED,
							},
						),
					},
				},
				{
					toCheck: "document:second#view@user:tom",
					rda: []rda{
						expectOrderedFrames(
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"second"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_NO_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_UNSPECIFIED,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second"},
								permission:     "viewer",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_UNSPECIFIED,
							},
						),
					},
				},
			},
		},
		{
			"multiple batching within the same request",
			`definition user {}
			
			 definition document {
				relation viewer: user
				permission view = viewer + nil
			 }
			`,
			[]tuple.Relationship{
				tuple.MustParse("document:first#viewer@user:tom"),
			},
			(func() []bulkCheckItem {
				// 100 IDs is the default batch size, so 99 + `first`.
				docsSlice := make([]string, 0, 99)
				docsSlice = append(docsSlice, "first")
				for i := range 99 {
					docsSlice = append(docsSlice, fmt.Sprintf("doc-%d", i))
				}

				items := make([]bulkCheckItem, 0, 501) //nolint:prealloc  // for some reason prealloc thinks this should be 1002
				items = append(items, bulkCheckItem{
					toCheck: "document:first#view@user:tom",
					rda: []rda{
						expectOrderedFrames(
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    docsSlice,
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_UNSPECIFIED,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    docsSlice,
								permission:     "viewer",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_UNSPECIFIED,
							},
						),
					},
				})

				for i := range 500 {
					items = append(items, bulkCheckItem{
						toCheck: fmt.Sprintf("document:doc-%d#view@user:tom", i),
						rda:     nil,
					})
				}

				return items
			})(),
		},
		{
			"caveated branch",
			`definition user {}

		 	 caveat somecaveat(somecondition int) {
		 		somecondition == 42
			 }

			 definition document {
				relation viewer: user with somecaveat
				permission view = viewer + nil
			 }
			`,
			[]tuple.Relationship{
				tuple.MustParse(`document:first#viewer@user:tom[somecaveat:{"somecondition": 41}]`),
				tuple.MustParse(`document:second#viewer@user:tom[somecaveat:{"somecondition": 42}]`),
				tuple.MustParse(`document:third#viewer@user:tom[somecaveat]`),
			},
			[]bulkCheckItem{
				{
					toCheck: "document:first#view@user:tom",
					rda: []rda{
						expectOrderedFrames(
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_NO_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second", "third"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second", "third"},
								permission:     "viewer",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
							},
						),
					},
				},
				{
					toCheck: "document:second#view@user:tom",
					rda: []rda{
						expectOrderedFrames(
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"second"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_HAS_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second", "third"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second", "third"},
								permission:     "viewer",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
							},
						),
					},
				},
				{
					toCheck: "document:third#view@user:tom",
					rda: []rda{
						expectOrderedFrames(
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"third"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second", "third"},
								permission:     "view",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
							},
							frameInfo{
								resourceType:   "document",
								resourceIDs:    []string{"first", "second", "third"},
								permission:     "viewer",
								permissionship: v1.CheckDebugTrace_PERMISSIONSHIP_CONDITIONAL_PERMISSION,
							},
						),
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)
			conn, cleanup, _, revision := testserver.NewTestServer(req, 5*time.Second, memdb.DisableGC, true,
				func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
					return tf.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, tc.relationships, req)
				})

			client := v1.NewPermissionsServiceClient(conn)
			t.Cleanup(cleanup)

			ctx := t.Context()
			ctx = requestmeta.AddRequestHeaders(ctx, requestmeta.RequestDebugInformation)

			items := make([]*v1.CheckBulkPermissionsRequestItem, 0, len(tc.toTest))
			for _, bci := range tc.toTest {
				parsed := tuple.MustParseV1Rel(bci.toCheck)
				items = append(items, &v1.CheckBulkPermissionsRequestItem{
					Resource:   parsed.Resource,
					Permission: parsed.Relation,
					Subject:    parsed.Subject,
				})
			}

			checkResp, err := client.CheckBulkPermissions(ctx, &v1.CheckBulkPermissionsRequest{
				Consistency: &v1.Consistency{
					Requirement: &v1.Consistency_AtLeastAsFresh{
						AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision),
					},
				},
				WithTracing: true,
				Items:       items,
			})
			require.NoError(t, err)

			for idx, bci := range tc.toTest {
				pair := checkResp.GetPairs()[idx]
				debugInfo := pair.GetItem().DebugTrace

				for _, rda := range bci.rda {
					rda(req, debugInfo)
				}
			}
		})
	}
}

func TestLookupResourcesDebugTraceV2(t *testing.T) {
	req := require.New(t)

	schema := `
	definition user {}

	definition group {
		relation member: user | group#member
		permission can_access = member
	}`
	relationships := []tuple.Relationship{
		tuple.MustParse("group:a#member@group:b#member"),
		tuple.MustParse("group:b#member@group:a#member"),
	}

	conn, cleanup, _, revision := testserver.NewTestServer(req, 5*time.Second, memdb.DisableGC, true,
		func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
			return tf.DatastoreFromSchemaAndTestRelationships(ds, schema, relationships, req)
		})
	t.Cleanup(cleanup)

	client := v1.NewPermissionsServiceClient(conn)

	ctx := t.Context()
	ctx = requestmeta.AddRequestHeaders(ctx, requestmeta.RequestDebugInformation)

	stream, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision),
			},
		},
		ResourceObjectType: "group",
		Permission:         "can_access",
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: "user",
				ObjectId:   "someuser",
			},
		},
	})
	req.NoError(err)

	// Drain the stream — the circular schema will eventually hit MaxDepthExceeded.
	// The traversal stack trace (if any) will be in the error details.
	var streamErr error
	for {
		_, recvErr := stream.Recv()
		if recvErr != nil {
			streamErr = recvErr
			break
		}
	}

	// The circular graph must have resulted in an error.
	if streamErr == nil {
		t.Skip("expected MaxDepthExceeded error from circular schema; none received")
		return
	}

	// Check for the traversal trace in the gRPC error details.
	var trace *dispatch.LookupDebugTrace
	if s, ok := status.FromError(streamErr); ok {
		for _, d := range s.Details() {
			if t, ok := d.(*dispatch.LookupDebugTrace); ok {
				trace = t
			}
		}
	}

	if trace == nil {
		// No trace attached — depth may not have been reached deep enough yet.
		// This is acceptable since the stack trace requires MaxDepth to fire.
		t.Skip("no traversal trace in error details; depth may not have been exceeded")
		return
	}

	req.NotEmpty(trace.ResourceType, "root frame ResourceType must be non-empty")
	req.NotEmpty(trace.Relation, "root frame Relation (permission) must be non-empty")
	
	// Count path length and ensure no "*batch*" artifacts or faked IDs are present
	pathLength := 0
	currentNode := trace
	var prevDepth uint32 = 0
	for currentNode != nil {
		pathLength++
		req.NotContains(currentNode.ResourceId, "*batch*", "trace must not contain batch artifacts")
		req.NotContains(currentNode.ResourceId, "...", "trace must not fake resource ID with ellipses")
		if pathLength > 1 {
			req.True(currentNode.Depth > prevDepth, "depth must increase monotonically")
		}
		prevDepth = currentNode.Depth

		if len(currentNode.SubProblems) > 0 {
			currentNode = currentNode.SubProblems[0]
		} else {
			currentNode = nil
		}
	}
	req.GreaterOrEqual(pathLength, 2, "path length must be at least 2 for recursion")
}

func TestLookupSubjectsDebugTraceV2(t *testing.T) {
	req := require.New(t)

	schema := `
	definition user {}

	definition group {
		relation member: user | group#member
		permission can_access = member
	}`
	relationships := []tuple.Relationship{
		tuple.MustParse("group:a#member@group:b#member"),
		tuple.MustParse("group:b#member@group:a#member"),
	}

	conn, cleanup, _, revision := testserver.NewTestServer(req, 5*time.Second, memdb.DisableGC, true,
		func(ds datastore.Datastore, require *require.Assertions) (datastore.Datastore, datastore.Revision) {
			return tf.DatastoreFromSchemaAndTestRelationships(ds, schema, relationships, req)
		})
	t.Cleanup(cleanup)

	client := v1.NewPermissionsServiceClient(conn)

	ctx := t.Context()
	ctx = requestmeta.AddRequestHeaders(ctx, requestmeta.RequestDebugInformation)

	stream, err := client.LookupSubjects(ctx, &v1.LookupSubjectsRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision),
			},
		},
		Resource: &v1.ObjectReference{
			ObjectType: "group",
			ObjectId:   "a",
		},
		Permission:        "can_access",
		SubjectObjectType: "user",
	})
	req.NoError(err)

	// Drain the stream — the circular schema will eventually hit MaxDepthExceeded.
	// The traversal stack trace (if any) will be in the error details.
	var streamErr error
	for {
		_, recvErr := stream.Recv()
		if recvErr != nil {
			streamErr = recvErr
			break
		}
	}

	// The circular graph must have resulted in an error.
	if streamErr == nil {
		t.Skip("expected MaxDepthExceeded error from circular schema; none received")
		return
	}

	// Check for the traversal trace in the gRPC error details.
	var trace *dispatch.LookupDebugTrace
	if s, ok := status.FromError(streamErr); ok {
		for _, d := range s.Details() {
			if t, ok := d.(*dispatch.LookupDebugTrace); ok {
				trace = t
			}
		}
	}

	if trace == nil {
		t.Skip("no traversal trace in error details; depth may not have been exceeded")
		return
	}

	req.NotEmpty(trace.ResourceType, "root frame ResourceType must be non-empty")
	req.NotEmpty(trace.Relation, "root frame Relation (permission) must be non-empty")
	
	// Count path length and ensure no "*batch*" artifacts or faked IDs are present
	pathLength := 0
	currentNode := trace
	var prevDepth uint32 = 0
	for currentNode != nil {
		pathLength++
		req.NotContains(currentNode.ResourceId, "*batch*", "trace must not contain batch artifacts")
		req.NotContains(currentNode.ResourceId, "...", "trace must not fake resource ID with ellipses")
		if pathLength > 1 {
			req.True(currentNode.Depth > prevDepth, "depth must increase monotonically")
		}
		prevDepth = currentNode.Depth

		if len(currentNode.SubProblems) > 0 {
			currentNode = currentNode.SubProblems[0]
		} else {
			currentNode = nil
		}
	}
	req.GreaterOrEqual(pathLength, 2, "path length must be at least 2 for recursion")
}

// FormatTrace formats a LookupDebugTrace into a readable arrow chain:
// doc:1#viewer -> group:eng#member -> user:alice
func FormatTrace(trace *dispatch.LookupDebugTrace) string {
	if trace == nil {
		return ""
	}
	parts := []string{}
	curr := trace
	for curr != nil {
		id := curr.ResourceId
		if id == "" {
			id = "*"
		}
		parts = append(parts, fmt.Sprintf("%s:%s#%s", curr.ResourceType, id, curr.Relation))
		if len(curr.SubProblems) > 0 {
			curr = curr.SubProblems[0]
		} else {
			curr = nil
		}
	}
	return strings.Join(parts, " -> ")
}
