package graph

import (
	"context"
	"fmt"
	"go/ast"
	"go/printer"
	"go/token"
	"os"
	"testing"

	v1_api "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	expand "github.com/authzed/spicedb/internal/graph"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/graph"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	_this *core.ObjectAndRelation

	companyOwner = graph.Leaf(ONR("folder", "company", "owner"),
		tuple.User(ONR("user", "owner", expand.Ellipsis)),
	)
	companyEditor = graph.Union(ONR("folder", "company", "editor"),
		graph.Leaf(_this),
		companyOwner,
	)

	companyViewer = graph.Union(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "legal", "...")),
			tuple.User(ONR("folder", "auditors", "viewer")),
		),
		companyEditor,
		graph.Union(ONR("folder", "company", "viewer")),
	)

	auditorsOwner = graph.Leaf(ONR("folder", "auditors", "owner"))

	auditorsEditor = graph.Union(ONR("folder", "auditors", "editor"),
		graph.Leaf(_this),
		auditorsOwner,
	)

	auditorsViewerRecursive = graph.Union(ONR("folder", "auditors", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "auditor", "...")),
		),
		auditorsEditor,
		graph.Union(ONR("folder", "auditors", "viewer")),
	)

	companyViewerRecursive = graph.Union(ONR("folder", "company", "viewer"),
		graph.Union(ONR("folder", "company", "viewer"),
			auditorsViewerRecursive,
			graph.Leaf(_this,
				tuple.User(ONR("user", "legal", "...")),
				tuple.User(ONR("folder", "auditors", "viewer")),
			),
		),
		companyEditor,
		graph.Union(ONR("folder", "company", "viewer")),
	)

	docOwner = graph.Leaf(ONR("document", "masterplan", "owner"),
		tuple.User(ONR("user", "product_manager", "...")),
	)
	docEditor = graph.Union(ONR("document", "masterplan", "editor"),
		graph.Leaf(_this),
		docOwner,
	)
	docViewer = graph.Union(ONR("document", "masterplan", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("user", "eng_lead", "...")),
		),
		docEditor,
		graph.Union(ONR("document", "masterplan", "viewer"),
			graph.Union(ONR("folder", "plans", "viewer"),
				graph.Leaf(_this,
					tuple.User(ONR("user", "chief_financial_officer", "...")),
				),
				graph.Union(ONR("folder", "plans", "editor"),
					graph.Leaf(_this),
					graph.Leaf(ONR("folder", "plans", "owner")),
				),
				graph.Union(ONR("folder", "plans", "viewer")),
			),
			graph.Union(ONR("folder", "strategy", "viewer"),
				graph.Leaf(_this),
				graph.Union(ONR("folder", "strategy", "editor"),
					graph.Leaf(_this),
					graph.Leaf(ONR("folder", "strategy", "owner"),
						tuple.User(ONR("user", "vp_product", "...")),
					),
				),
				graph.Union(ONR("folder", "strategy", "viewer"),
					companyViewer,
				),
			),
		),
	)
)

func TestExpand(t *testing.T) {
	testCases := []struct {
		start                 *core.ObjectAndRelation
		expansionMode         v1.DispatchExpandRequest_ExpansionMode
		expected              *core.RelationTupleTreeNode
		expectedDispatchCount int
		expectedDepthRequired int
	}{
		{start: ONR("folder", "company", "owner"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: companyOwner, expectedDispatchCount: 1, expectedDepthRequired: 1},
		{start: ONR("folder", "company", "editor"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: companyEditor, expectedDispatchCount: 2, expectedDepthRequired: 2},
		{start: ONR("folder", "company", "viewer"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: companyViewer, expectedDispatchCount: 3, expectedDepthRequired: 3},
		{start: ONR("document", "masterplan", "owner"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: docOwner, expectedDispatchCount: 1, expectedDepthRequired: 1},
		{start: ONR("document", "masterplan", "editor"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: docEditor, expectedDispatchCount: 2, expectedDepthRequired: 2},
		{start: ONR("document", "masterplan", "viewer"), expansionMode: v1.DispatchExpandRequest_SHALLOW, expected: docViewer, expectedDispatchCount: 12, expectedDepthRequired: 5},

		{start: ONR("folder", "auditors", "owner"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: auditorsOwner, expectedDispatchCount: 1, expectedDepthRequired: 1},
		{start: ONR("folder", "auditors", "editor"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: auditorsEditor, expectedDispatchCount: 2, expectedDepthRequired: 2},
		{start: ONR("folder", "auditors", "viewer"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: auditorsViewerRecursive, expectedDispatchCount: 3, expectedDepthRequired: 3},

		{start: ONR("folder", "company", "owner"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: companyOwner, expectedDispatchCount: 1, expectedDepthRequired: 1},
		{start: ONR("folder", "company", "editor"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: companyEditor, expectedDispatchCount: 2, expectedDepthRequired: 2},
		{start: ONR("folder", "company", "viewer"), expansionMode: v1.DispatchExpandRequest_RECURSIVE, expected: companyViewerRecursive, expectedDispatchCount: 6, expectedDepthRequired: 4},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%s", tuple.StringONR(tc.start), tc.expansionMode), func(t *testing.T) {
			require := require.New(t)

			ctx, dispatch, revision := newLocalDispatcher(require)

			expandResult, err := dispatch.DispatchExpand(ctx, &v1.DispatchExpandRequest{
				ObjectAndRelation: tc.start,
				Metadata: &v1.ResolverMeta{
					AtRevision:     revision.String(),
					DepthRemaining: 50,
				},
				ExpansionMode: tc.expansionMode,
			})

			require.NoError(err)
			require.NotNil(expandResult.TreeNode)
			require.GreaterOrEqual(expandResult.Metadata.DepthRequired, uint32(1))
			require.Equal(tc.expectedDispatchCount, int(expandResult.Metadata.DispatchCount))
			require.Equal(tc.expectedDepthRequired, int(expandResult.Metadata.DepthRequired))

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
			fName = "tf.E"
		case core.SetOperationUserset_INTERSECTION:
			fName = "tf.I"
		case core.SetOperationUserset_UNION:
			fName = "tf.U"
		default:
			panic("Unknown set operation")
		}

		for _, child := range node.GetIntermediateNode().ChildNodes {
			children = append(children, serialize(child))
		}

	case *core.RelationTupleTreeNode_LeafNode:
		fName = "tf.Leaf"
		for _, user := range node.GetLeafNode().Users {
			onrExpr := onrExpr(user.GetUserset())
			children = append(children, &ast.CallExpr{
				Fun:  ast.NewIdent("User"),
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
		Fun: ast.NewIdent("tf.ONR"),
		Args: []ast.Expr{
			ast.NewIdent("\"" + onr.Namespace + "\""),
			ast.NewIdent("\"" + onr.ObjectId + "\""),
			ast.NewIdent("\"" + onr.Relation + "\""),
		},
	}
}

func TestMaxDepthExpand(t *testing.T) {
	require := require.New(t)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	mutations := []*v1_api.RelationshipUpdate{{
		Operation: v1_api.RelationshipUpdate_OPERATION_CREATE,
		Relationship: &v1_api.Relationship{
			Resource: &v1_api.ObjectReference{
				ObjectType: "folder",
				ObjectId:   "oops",
			},
			Relation: "parent",
			Subject: &v1_api.SubjectReference{
				Object: &v1_api.ObjectReference{
					ObjectType: "folder",
					ObjectId:   "oops",
				},
			},
		},
	}}

	ctx := datastoremw.ContextWithHandle(context.Background())

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(mutations)
	})
	require.NoError(err)
	require.True(revision.GreaterThan(decimal.Zero))
	require.NoError(datastoremw.SetInContext(ctx, ds))

	dispatch := NewLocalOnlyDispatcher()

	_, err = dispatch.DispatchExpand(ctx, &v1.DispatchExpandRequest{
		ObjectAndRelation: ONR("folder", "oops", "viewer"),
		Metadata: &v1.ResolverMeta{
			AtRevision:     revision.String(),
			DepthRemaining: 50,
		},
		ExpansionMode: v1.DispatchExpandRequest_SHALLOW,
	})

	require.Error(err)
}
