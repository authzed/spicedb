package validation

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	ns "github.com/authzed/spicedb/pkg/namespace"
)

func TestNamespaceValidation(t *testing.T) {
	testCases := []struct {
		name        string
		expectError bool
		config      *pb.NamespaceDefinition
	}{
		{"empty", true, &pb.NamespaceDefinition{}},
		{"simple", false, ns.Namespace("user")},
		{"full", false, ns.Namespace(
			"document",
			ns.Relation("owner", nil),
			ns.Relation("editor", ns.Union(
				ns.This(),
				ns.ComputedUserset("owner"),
			)),
			ns.Relation("parent", nil),
			ns.Relation("lock", nil),
			ns.Relation("viewer", ns.Union(
				ns.This(),
				ns.ComputedUserset("editor"),
				ns.TupleToUserset("parent", "viewer"),
			)),
		)},
		{"working intersection", false, ns.Namespace(
			"document",
			ns.Relation("editor", ns.Intersection(
				ns.This(),
				ns.ComputedUserset("owner"),
			)),
		)},
		{"working exclusion", false, ns.Namespace(
			"document",
			ns.Relation("editor", ns.Exclusion(
				ns.This(),
				ns.ComputedUserset("owner"),
			)),
		)},
		{"bad relation name", true, ns.Namespace(
			"document",
			ns.Relation("a", nil),
		)},
		{"bad rewrite", true, ns.Namespace(
			"document",
			ns.Relation("editor", &pb.UsersetRewrite{}),
		)},
		{"nil union", true, ns.Namespace(
			"document",
			ns.Relation("editor", &pb.UsersetRewrite{
				RewriteOperation: &pb.UsersetRewrite_Union{},
			}),
		)},
		{"no children", true, ns.Namespace(
			"document",
			ns.Relation("editor", &pb.UsersetRewrite{
				RewriteOperation: &pb.UsersetRewrite_Union{
					Union: &pb.SetOperation{},
				},
			}),
		)},
		{"empty child", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_TupleToUserset{},
				},
			)),
		)},
		{"nil child pointer", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				nil,
			)),
		)},
		{"nil child", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{},
			)),
		)},
		{"bad ttu", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_TupleToUserset{
						TupleToUserset: &pb.TupleToUserset{},
					},
				},
			)),
		)},
		{"ttu missing tupleset", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_TupleToUserset{
						TupleToUserset: &pb.TupleToUserset{
							ComputedUserset: &pb.ComputedUserset{
								Object:   pb.ComputedUserset_TUPLE_USERSET_OBJECT,
								Relation: "admin",
							},
						},
					},
				},
			)),
		)},
		{"ttu missing rewrite", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_TupleToUserset{
						TupleToUserset: &pb.TupleToUserset{
							Tupleset: &pb.TupleToUserset_Tupleset{
								Relation: "parent",
							},
						},
					},
				},
			)),
		)},
		{"ttu bad relation", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_TupleToUserset{
						TupleToUserset: &pb.TupleToUserset{
							Tupleset: &pb.TupleToUserset_Tupleset{
								Relation: "",
							},
							ComputedUserset: &pb.ComputedUserset{
								Object:   pb.ComputedUserset_TUPLE_USERSET_OBJECT,
								Relation: "admin",
							},
						},
					},
				},
			)),
		)},
		{"ttu bad computed relation", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_TupleToUserset{
						TupleToUserset: &pb.TupleToUserset{
							Tupleset: &pb.TupleToUserset_Tupleset{
								Relation: "parent",
							},
							ComputedUserset: &pb.ComputedUserset{
								Object:   pb.ComputedUserset_TUPLE_USERSET_OBJECT,
								Relation: "",
							},
						},
					},
				},
			)),
		)},
		{"ttu nil computed relation", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_TupleToUserset{
						TupleToUserset: &pb.TupleToUserset{
							Tupleset: &pb.TupleToUserset_Tupleset{
								Relation: "parent",
							},
							ComputedUserset: &pb.ComputedUserset{
								Object: pb.ComputedUserset_TUPLE_USERSET_OBJECT,
							},
						},
					},
				},
			)),
		)},
		{"empty cu", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_ComputedUserset{},
				},
			)),
		)},
		{"cu empty relation", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_ComputedUserset{
						ComputedUserset: &pb.ComputedUserset{},
					},
				},
			)),
		)},
		{"cu bad relation name", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_ComputedUserset{
						ComputedUserset: &pb.ComputedUserset{
							Relation: "ab",
						},
					},
				},
			)),
		)},
		{"cu bad object type", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_ComputedUserset{
						ComputedUserset: &pb.ComputedUserset{
							Relation: "admin",
							Object:   3,
						},
					},
				},
			)),
		)},
		{"child nil rewrite", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_UsersetRewrite{},
				},
			)),
		)},
		{"child bad rewrite", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_UsersetRewrite{
						UsersetRewrite: &pb.UsersetRewrite{},
					},
				},
			)),
		)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
