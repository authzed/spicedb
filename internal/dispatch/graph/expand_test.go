package graph

import (
	"context"
	"fmt"
	"go/ast"
	"go/printer"
	"go/token"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	expand "github.com/authzed/spicedb/internal/graph"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/graph"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
)

func DS(objectType string, objectID string, objectRelation string) *core.DirectSubject {
	return &core.DirectSubject{
		Subject: ONR(objectType, objectID, objectRelation),
	}
}

var (
	companyOwner = graph.Leaf(ONR("folder", "company", "owner"),
		(DS("user", "owner", expand.Ellipsis)),
	)
	companyEditor = graph.Leaf(ONR("folder", "company", "editor"))

	companyEdit = graph.Union(ONR("folder", "company", "edit"),
		companyEditor,
		companyOwner,
	)

	companyViewer = graph.Leaf(ONR("folder", "company", "viewer"),
		(DS("user", "legal", "...")),
		(DS("folder", "auditors", "viewer")),
	)

	companyView = graph.Union(ONR("folder", "company", "view"),
		companyViewer,
		companyEdit,
		graph.Union(ONR("folder", "company", "view")),
	)

	auditorsOwner = graph.Leaf(ONR("folder", "auditors", "owner"))

	auditorsEditor = graph.Leaf(ONR("folder", "auditors", "editor"))

	auditorsEdit = graph.Union(ONR("folder", "auditors", "edit"),
		auditorsEditor,
		auditorsOwner,
	)

	auditorsViewer = graph.Leaf(ONR("folder", "auditors", "viewer"),
		(DS("user", "auditor", "...")),
	)

	auditorsViewRecursive = graph.Union(ONR("folder", "auditors", "view"),
		auditorsViewer,
		auditorsEdit,
		graph.Union(ONR("folder", "auditors", "view")),
	)

	companyViewRecursive = graph.Union(ONR("folder", "company", "view"),
		graph.Union(ONR("folder", "company", "viewer"),
			graph.Leaf(ONR("folder", "auditors", "viewer"),
				(DS("user", "auditor", "..."))),
			graph.Leaf(ONR("folder", "company", "viewer"),
				(DS("user", "legal", "...")),
				(DS("folder", "auditors", "viewer")))),
		graph.Union(ONR("folder", "company", "edit"),
			graph.Leaf(ONR("folder", "company", "editor")),
			graph.Leaf(ONR("folder", "company", "owner"),
				(DS("user", "owner", "...")))),
		graph.Union(ONR("folder", "company", "view")))

	docOwner = graph.Leaf(ONR("document", "masterplan", "owner"),
		(DS("user", "product_manager", "...")),
	)
	docEditor = graph.Leaf(ONR("document", "masterplan", "editor"))

	docEdit = graph.Union(ONR("document", "masterplan", "edit"),
		docOwner,
		docEditor,
	)

	docViewer = graph.Leaf(ONR("document", "masterplan", "viewer"),
		(DS("user", "eng_lead", "...")),
	)

	docView = graph.Union(ONR("document", "masterplan", "view"),
		docViewer,
		docEdit,
		graph.Union(ONR("document", "masterplan", "view"),
			graph.Union(ONR("folder", "plans", "view"),
				graph.Leaf(ONR("folder", "plans", "viewer"),
					(DS("user", "chief_financial_officer", "...")),
				),
				graph.Union(ONR("folder", "plans", "edit"),
					graph.Leaf(ONR("folder", "plans", "editor")),
					graph.Leaf(ONR("folder", "plans", "owner"))),
				graph.Union(ONR("folder", "plans", "view"))),
			graph.Union(ONR("folder", "strategy", "view"),
				graph.Leaf(ONR("folder", "strategy", "viewer")),
				graph.Union(ONR("folder", "strategy", "edit"),
					graph.Leaf(ONR("folder", "strategy", "editor")),
					graph.Leaf(ONR("folder", "strategy", "owner"),
						(DS("user", "vp_product", "...")))),
				graph.Union(ONR("folder", "strategy", "view"),
					graph.Union(ONR("folder", "company", "view"),
						graph.Leaf(ONR("folder", "company", "viewer"),
							(DS("user", "legal", "...")),
							(DS("folder", "auditors", "viewer"))),
						graph.Union(ONR("folder", "company", "edit"),
							graph.Leaf(ONR("folder", "company", "editor")),
							graph.Leaf(ONR("folder", "company", "owner"),
								(DS("user", "owner", "...")))),
						graph.Union(ONR("folder", "company", "view")),
					),
				),
			),
		),
	)
)

func TestExpand(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	testCases := []struct {
		start                 *core.ObjectAndRelation
		expansionMode         v1.DispatchExpandRequest_ExpansionMode
		expected              *core.RelationTupleTreeNode
		expectedDispatchCount int
		expectedDepthRequired int
	}{
		{start: ONR("folder", "company", "owner"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: companyOwner, expectedDispatchCount: 1, expectedDepthRequired: 1},
		{start: ONR("folder", "company", "edit"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: companyEdit, expectedDispatchCount: 3, expectedDepthRequired: 2},
		{start: ONR("folder", "company", "view"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: companyView, expectedDispatchCount: 5, expectedDepthRequired: 3},
		{start: ONR("document", "masterplan", "owner"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: docOwner, expectedDispatchCount: 1, expectedDepthRequired: 1},
		{start: ONR("document", "masterplan", "edit"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: docEdit, expectedDispatchCount: 3, expectedDepthRequired: 2},
		{start: ONR("document", "masterplan", "view"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: docView, expectedDispatchCount: 20, expectedDepthRequired: 5},

		{start: ONR("folder", "auditors", "owner"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: auditorsOwner, expectedDispatchCount: 1, expectedDepthRequired: 1},
		{start: ONR("folder", "auditors", "edit"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: auditorsEdit, expectedDispatchCount: 3, expectedDepthRequired: 2},
		{start: ONR("folder", "auditors", "view"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: auditorsViewRecursive, expectedDispatchCount: 5, expectedDepthRequired: 3},

		{start: ONR("folder", "company", "owner"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: companyOwner, expectedDispatchCount: 1, expectedDepthRequired: 1},
		{start: ONR("folder", "company", "edit"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: companyEdit, expectedDispatchCount: 3, expectedDepthRequired: 2},
		{start: ONR("folder", "company", "view"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: companyViewRecursive, expectedDispatchCount: 6, expectedDepthRequired: 3},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%s", tuple.StringONR(tc.start), tc.expansionMode), func(t *testing.T) {
			require := require.New(t)

			ctx, dispatch, revision := newLocalDispatcher(t)

			expandResult, err := dispatch.DispatchExpand(ctx, &v1.DispatchExpandRequest{
				ResourceAndRelation: tc.start,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				ExpansionMode: tc.expansionMode,
			})

			require.NoError(err)
			require.NotNil(expandResult.TreeNode)
			require.GreaterOrEqual(expandResult.Metadata.DepthRequired, uint32(1))
			require.Equal(tc.expectedDispatchCount, int(expandResult.Metadata.DispatchCount), "mismatch in dispatch count")
			require.Equal(tc.expectedDepthRequired, int(expandResult.Metadata.DepthRequired), "mismatch in depth required")

			if diff := cmp.Diff(tc.expected, expandResult.TreeNode, protocmp.Transform()); diff != "" {
				fset := token.NewFileSet()
				err := printer.Fprint(os.Stdout, fset, serializeToFile(expandResult.TreeNode))
				require.NoError(err)
				t.Errorf("unexpected difference:\n%v", diff)
			}
		})
	}
}

func serializeToFile(node *core.RelationTupleTreeNode) *ast.File {
	return &ast.File{
		Package: 1,
		Name: &ast.Ident{
			Name: "main",
		},
		Decls: []ast.Decl{
			&ast.FuncDecl{
				Name: &ast.Ident{
					Name: "main",
				},
				Type: &ast.FuncType{
					Params: &ast.FieldList{},
				},
				Body: &ast.BlockStmt{
					List: []ast.Stmt{
						&ast.ExprStmt{
							X: serialize(node),
						},
					},
				},
			},
		},
	}
}

func serialize(node *core.RelationTupleTreeNode) *ast.CallExpr {
	var expanded ast.Expr = ast.NewIdent("_this")
	if node.Expanded != nil {
		expanded = onrExpr(node.Expanded)
	}

	children := []ast.Expr{expanded}

	var fName string
	switch node.NodeType.(type) {
	case *core.RelationTupleTreeNode_IntermediateNode:
		switch node.GetIntermediateNode().Operation {
		case core.SetOperationUserset_EXCLUSION:
			fName = "graph.Exclusion"
		case core.SetOperationUserset_INTERSECTION:
			fName = "graph.Intersection"
		case core.SetOperationUserset_UNION:
			fName = "graph.Union"
		default:
			panic("Unknown set operation")
		}

		for _, child := range node.GetIntermediateNode().ChildNodes {
			children = append(children, serialize(child))
		}

	case *core.RelationTupleTreeNode_LeafNode:
		fName = "graph.Leaf"
		for _, subject := range node.GetLeafNode().Subjects {
			onrExpr := onrExpr(subject.Subject)
			children = append(children, &ast.CallExpr{
				Fun:  ast.NewIdent(""),
				Args: []ast.Expr{onrExpr},
			})
		}
	}

	return &ast.CallExpr{
		Fun:  ast.NewIdent(fName),
		Args: children,
	}
}

func onrExpr(onr *core.ObjectAndRelation) ast.Expr {
	return &ast.CallExpr{
		Fun: ast.NewIdent("ONR"),
		Args: []ast.Expr{
			ast.NewIdent("\"" + onr.Namespace + "\""),
			ast.NewIdent("\"" + onr.ObjectId + "\""),
			ast.NewIdent("\"" + onr.Relation + "\""),
		},
	}
}

func TestMaxDepthExpand(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	tpl := tuple.Parse("folder:oops#parent@folder:oops")
	ctx := datastoremw.ContextWithHandle(context.Background())

	revision, err := common.WriteTuples(ctx, ds, core.RelationTupleUpdate_CREATE, tpl)
	require.NoError(err)
	require.True(revision.GreaterThan(datastore.NoRevision))
	require.NoError(datastoremw.SetInContext(ctx, ds))

	dispatch := NewLocalOnlyDispatcher(10)

	_, err = dispatch.DispatchExpand(ctx, &v1.DispatchExpandRequest{
		ResourceAndRelation: ONR("folder", "oops", "view"),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		ExpansionMode: v1.DispatchExpandRequest_SHALLOW,
	})

	require.Error(err)
}

func TestCaveatedExpand(t *testing.T) {
	defer goleak.VerifyNone(t, goleakIgnores...)

	testCases := []struct {
		name          string
		schema        string
		relationships []*core.RelationTuple

		start *core.ObjectAndRelation

		expansionMode    v1.DispatchExpandRequest_ExpansionMode
		expectedTreeText string
	}{
		{
			"basic caveated subject",
			`
			definition user {}

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition document {
				relation viewer: user with somecaveat | user
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:testdoc#viewer@user:sarah[somecaveat]"),
				tuple.MustParse("document:testdoc#viewer@user:mary"),
			},
			tuple.ParseONR("document:testdoc#viewer"),
			v1.DispatchExpandRequest_SHALLOW,
			`
			leaf_node: {
				subjects: {
					subject: {
						namespace: "user"
						object_id: "mary"
						relation: "..."
					}
				}
				subjects: {
					subject: {
						namespace: "user"
						object_id: "sarah"
						relation: "..."
					}
					caveat_expression: {
						caveat: {
							caveat_name: "somecaveat"
							context: {}
						}
					}
				}
			}
			expanded: {
				namespace: "document"
				object_id: "testdoc"
				relation: "viewer"
			}
			`,
		},
		{
			"caveated shallow indirect",
			`
			definition user {}

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition group {
				relation member: user
			}

			definition document {
				relation viewer: group#member with somecaveat
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:testdoc#viewer@group:test#member[somecaveat]"),
				tuple.MustParse("group:test#member@user:mary"),
			},
			tuple.ParseONR("document:testdoc#viewer"),
			v1.DispatchExpandRequest_SHALLOW,
			`
			leaf_node: {
				subjects: {
					subject: {
						namespace: "group"
						object_id: "test"
						relation: "member"
					}
					caveat_expression: {
						caveat: {
							caveat_name: "somecaveat"
							context: {}
						}
					}
				}
			}
			expanded: {
				namespace: "document"
				object_id: "testdoc"
				relation: "viewer"
			}
			`,
		},
		{
			"caveated recursive indirect",
			`
			definition user {}

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			caveat anothercaveat(somecondition int) {
				somecondition != 42
			}

			definition group {
				relation member: user
			}

			definition document {
				relation viewer: group#member with somecaveat
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:testdoc#viewer@group:test#member[somecaveat]"),
				tuple.MustParse("group:test#member@user:mary"),
				tuple.MustParse("group:test#member@user:sarah[anothercaveat]"),
			},
			tuple.ParseONR("document:testdoc#viewer"),
			v1.DispatchExpandRequest_RECURSIVE,
			`
			intermediate_node: {
				operation: UNION
				child_nodes: {
					leaf_node: {
						subjects: {
							subject: {
								namespace: "user"
								object_id: "mary"
								relation: "..."
							}
						}
						subjects: {
							subject: {
								namespace: "user"
								object_id: "sarah"
								relation: "..."
							}
							caveat_expression: {
								caveat: {
									caveat_name: "anothercaveat"
									context: {}
								}
							}
						}
					}
					expanded: {
						namespace: "group"
						object_id: "test"
						relation: "member"
					}
					caveat_expression: {
						caveat: {
							caveat_name: "somecaveat"
							context: {}
						}
					}
				}
				child_nodes: {
					leaf_node: {
						subjects: {
							subject: {
								namespace: "group"
								object_id: "test"
								relation: "member"
							}
							caveat_expression: {
								caveat: {
									caveat_name: "somecaveat"
									context: {}
								}
							}
						}
					}
					expanded: {
						namespace: "document"
						object_id: "testdoc"
						relation: "viewer"
					}
				}
			}
			expanded: {
				namespace: "document"
				object_id: "testdoc"
				relation: "viewer"
			}
			`,
		},
		{
			"shallow caveated arrow",
			`
			definition user {}

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			caveat anothercaveat(somecondition int) {
				somecondition != 42
			}

			caveat orgcaveat(somecondition int) {
				somecondition < 42
			}

			definition organization {
				relation admin: user | user with orgcaveat
			}

			definition document {
				relation org: organization with somecaveat
				relation viewer: user
				permission view = viewer + org->admin
			}
			`,
			[]*core.RelationTuple{
				tuple.MustParse("document:testdoc#viewer@user:tom"),
				tuple.MustParse("document:testdoc#viewer@user:fred[anothercaveat]"),
				tuple.MustParse("document:testdoc#org@organization:someorg[somecaveat]"),
				tuple.MustParse("organization:someorg#admin@user:sarah"),
				tuple.MustParse("organization:someorg#admin@user:mary[orgcaveat]"),
			},
			tuple.ParseONR("document:testdoc#view"),
			v1.DispatchExpandRequest_SHALLOW,
			`
			intermediate_node:  {
				operation:  UNION
				child_nodes:  {
					leaf_node:  {
						subjects:  {
							subject:  {
								namespace:  "user"
								object_id:  "fred"
								relation:  "..."
							}
							caveat_expression:  {
								caveat:  {
									caveat_name:  "anothercaveat"
									context:  {}
								}
							}
						}
						subjects:  {
							subject:  {
								namespace:  "user"
								object_id:  "tom"
								relation:  "..."
							}
						}
					}
					expanded:  {
						namespace:  "document"
						object_id:  "testdoc"
						relation:  "viewer"
					}
				}
				child_nodes:  {
					intermediate_node:  {
						operation:  UNION
						child_nodes:  {
							leaf_node:  {
								subjects:  {
									subject:  {
										namespace:  "user"
										object_id:  "mary"
										relation:  "..."
									}
									caveat_expression:  {
										caveat:  {
											caveat_name:  "orgcaveat"
											context:  {}
										}
									}
								}
								subjects:  {
									subject:  {
										namespace:  "user"
										object_id:  "sarah"
										relation:  "..."
									}
								}
							}
							expanded:  {
								namespace:  "organization"
								object_id:  "someorg"
								relation:  "admin"
							}
							caveat_expression:  {
								caveat:  {
									caveat_name:  "somecaveat"
									context:  {}
								}
							}
						}
					}
					expanded:  {
						namespace:  "document"
						object_id:  "testdoc"
						relation:  "view"
					}
				}
			}
			expanded:  {
				namespace:  "document"
				object_id:  "testdoc"
				relation:  "view"
			}
			`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ctx, dispatch, revision := newLocalDispatcherWithSchemaAndRels(t, tc.schema, tc.relationships)

			expandResult, err := dispatch.DispatchExpand(ctx, &v1.DispatchExpandRequest{
				ResourceAndRelation: tc.start,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				ExpansionMode: tc.expansionMode,
			})
			require.NoError(err)

			expectedTree := &core.RelationTupleTreeNode{}
			err = prototext.Unmarshal([]byte(tc.expectedTreeText), expectedTree)
			require.NoError(err)

			require.NoError(err)
			require.NotNil(expandResult.TreeNode)
			testutil.RequireProtoEqual(t, expectedTree, expandResult.TreeNode, "Got different expansion trees")
		})
	}
}
