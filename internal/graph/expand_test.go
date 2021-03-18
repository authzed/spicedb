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
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/authzed/spicedb/internal/testfixtures"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

var (
	companyOwner = tf.U(tf.ONR("folder", "company", "owner"),
		tf.Leaf(tf.ONR("folder", "company", "owner"),
			tf.User(tf.ONR("user", "owner", Ellipsis)),
		),
	)
	companyEditor = tf.U(tf.ONR("folder", "company", "editor"),
		tf.U(tf.ONR("folder", "company", "editor"),
			tf.Leaf(tf.ONR("folder", "company", "editor")),
			companyOwner,
		),
	)
	companyViewer = tf.U(tf.ONR("folder", "company", "viewer"),
		tf.U(tf.ONR("folder", "company", "viewer"),
			tf.Leaf(tf.ONR("folder", "company", "viewer"),
				tf.User(tf.ONR("folder", "auditors", "viewer")),
				tf.User(tf.ONR("user", "legal", "...")),
			),
			companyEditor,
			tf.U(tf.ONR("folder", "company", "viewer")),
		),
	)
	docOwner = tf.U(tf.ONR("document", "masterplan", "owner"),
		tf.Leaf(tf.ONR("document", "masterplan", "owner"),
			tf.User(tf.ONR("user", "pm", "...")),
		),
	)
	docEditor = tf.U(tf.ONR("document", "masterplan", "editor"),
		tf.U(tf.ONR("document", "masterplan", "editor"),
			tf.Leaf(tf.ONR("document", "masterplan", "editor")),
			docOwner,
		),
	)
	docViewer = tf.U(tf.ONR("document", "masterplan", "viewer"),
		tf.U(tf.ONR("document", "masterplan", "viewer"),
			tf.Leaf(tf.ONR("document", "masterplan", "viewer"),
				tf.User(tf.ONR("user", "eng_lead", "...")),
			),
			docEditor,
			tf.U(tf.ONR("document", "masterplan", "viewer"),
				tf.U(tf.ONR("folder", "plans", "viewer"),
					tf.U(tf.ONR("folder", "plans", "viewer"),
						tf.Leaf(tf.ONR("folder", "plans", "viewer"),
							tf.User(tf.ONR("user", "cfo", "...")),
						),
						tf.U(tf.ONR("folder", "plans", "editor"),
							tf.U(tf.ONR("folder", "plans", "editor"),
								tf.Leaf(tf.ONR("folder", "plans", "editor")),
								tf.U(tf.ONR("folder", "plans", "owner"),
									tf.Leaf(tf.ONR("folder", "plans", "owner")),
								),
							),
						),
						tf.U(tf.ONR("folder", "plans", "viewer")),
					),
				),
				tf.U(tf.ONR("folder", "strategy", "viewer"),
					tf.U(tf.ONR("folder", "strategy", "viewer"),
						tf.Leaf(tf.ONR("folder", "strategy", "viewer")),
						tf.U(tf.ONR("folder", "strategy", "editor"),
							tf.U(tf.ONR("folder", "strategy", "editor"),
								tf.Leaf(tf.ONR("folder", "strategy", "editor")),
								tf.U(tf.ONR("folder", "strategy", "owner"),
									tf.Leaf(tf.ONR("folder", "strategy", "owner"),
										tf.User(tf.ONR("user", "vp_product", "...")),
									),
								),
							),
						),
						tf.U(tf.ONR("folder", "strategy", "viewer"),
							companyViewer,
						),
					),
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
		{start: tf.ONR("folder", "company", "owner"), expected: companyOwner},
		{start: tf.ONR("folder", "company", "editor"), expected: companyEditor},
		{start: tf.ONR("folder", "company", "viewer"), expected: companyViewer},
		{start: tf.ONR("document", "masterplan", "owner"), expected: docOwner},
		{start: tf.ONR("document", "masterplan", "editor"), expected: docEditor},
		{start: tf.ONR("document", "masterplan", "viewer"), expected: docViewer},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("%s:%s#%s", tc.start.Namespace, tc.start.ObjectId, tc.start.Relation)
		t.Run(name, func(t *testing.T) {
			require := require.New(t)

			ds, revision := tf.StandardDatastoreWithData(require)

			dispatch, err := NewLocalDispatcher(ds)
			require.NoError(err)

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
	ds, _ := testfixtures.StandardDatastoreWithSchema(require)

	mutations := []*pb.RelationTupleUpdate{
		testfixtures.C(&pb.RelationTuple{
			ObjectAndRelation: testfixtures.ONR("folder", "oops", "parent"),
			User:              testfixtures.User(testfixtures.ONR("folder", "oops", Ellipsis)),
		}),
	}

	revision, err := ds.WriteTuples(nil, mutations)
	require.NoError(err)
	require.Greater(revision, uint64(0))

	dispatch, err := NewLocalDispatcher(ds)
	require.NoError(err)

	checkResult := dispatch.Expand(context.Background(), ExpandRequest{
		Start:          testfixtures.ONR("folder", "oops", "viewer"),
		AtRevision:     revision,
		DepthRemaining: 50,
	})

	require.Error(checkResult.Err)
}
