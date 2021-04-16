package graph

import (
	"context"
	"go/ast"
	"go/printer"
	"go/token"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/testfixtures"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/graph"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	_this *pb.ObjectAndRelation

	companyOwner = graph.Leaf(ONR("folder", "company", "owner"),
		tuple.User(ONR("user", "owner", Ellipsis)),
	)
	companyEditor = graph.Union(ONR("folder", "company", "editor"),
		graph.Leaf(_this),
		companyOwner,
	)
	companyViewer = graph.Union(ONR("folder", "company", "viewer"),
		graph.Leaf(_this,
			tuple.User(ONR("folder", "auditors", "viewer")),
			tuple.User(ONR("user", "legal", "...")),
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
		start    *pb.ObjectAndRelation
		expected *pb.RelationTupleTreeNode
	}{
		{start: ONR("folder", "company", "owner"), expected: companyOwner},
		{start: ONR("folder", "company", "editor"), expected: companyEditor},
		{start: ONR("folder", "company", "viewer"), expected: companyViewer},
		{start: ONR("document", "masterplan", "owner"), expected: docOwner},
		{start: ONR("document", "masterplan", "editor"), expected: docEditor},
		{start: ONR("document", "masterplan", "viewer"), expected: docViewer},
	}

	for _, tc := range testCases {
		t.Run(tuple.StringONR(tc.start), func(t *testing.T) {
			require := require.New(t)

			dispatch, revision := newLocalDispatcher(require)

			checkResult := dispatch.Expand(context.Background(), ExpandRequest{
				Start:          tc.start,
				AtRevision:     revision,
				DepthRemaining: 50,
			})

			if diff := cmp.Diff(tc.expected, checkResult.Tree, protocmp.Transform()); diff != "" {
				fset := token.NewFileSet()
				err := printer.Fprint(os.Stdout, fset, serializeToFile(checkResult.Tree))
				require.NoError(err)
				t.Errorf("unexpected difference:\n%v", diff)
			}
		})
	}
}

func serializeToFile(node *pb.RelationTupleTreeNode) *ast.File {
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

func serialize(node *pb.RelationTupleTreeNode) *ast.CallExpr {
	expanded := onrExpr(node.Expanded)

	children := []ast.Expr{expanded}

	var fName string
	switch node.NodeType.(type) {
	case *pb.RelationTupleTreeNode_IntermediateNode:
		switch node.GetIntermediateNode().Operation {
		case pb.SetOperationUserset_EXCLUSION:
			fName = "tf.E"
		case pb.SetOperationUserset_INTERSECTION:
			fName = "tf.I"
		case pb.SetOperationUserset_UNION:
			fName = "tf.U"
		default:
			panic("Unknown set operation")
		}

		for _, child := range node.GetIntermediateNode().ChildNodes {
			children = append(children, serialize(child))
		}

	case *pb.RelationTupleTreeNode_LeafNode:
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

func onrExpr(onr *pb.ObjectAndRelation) ast.Expr {
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

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)

	mutations := []*pb.RelationTupleUpdate{
		tuple.Create(&pb.RelationTuple{
			ObjectAndRelation: ONR("folder", "oops", "parent"),
			User:              tuple.User(ONR("folder", "oops", Ellipsis)),
		}),
	}

	ctx := context.Background()

	revision, err := ds.WriteTuples(ctx, nil, mutations)
	require.NoError(err)
	require.Greater(revision, uint64(0))

	nsm, err := namespace.NewCachingNamespaceManager(ds, 1*time.Second, testCacheConfig)
	require.NoError(err)

	dispatch, err := NewLocalDispatcher(nsm, ds)
	require.NoError(err)

	checkResult := dispatch.Expand(ctx, ExpandRequest{
		Start:          ONR("folder", "oops", "viewer"),
		AtRevision:     revision,
		DepthRemaining: 50,
	})

	require.Error(checkResult.Err)
}
