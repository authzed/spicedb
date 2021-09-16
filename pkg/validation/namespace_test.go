package validation

import (
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/stretchr/testify/require"

	ns "github.com/authzed/spicedb/pkg/namespace"
)

func TestNamespaceValidation(t *testing.T) {
	testCases := []struct {
		name        string
		expectError bool
		config      *v0.NamespaceDefinition
	}{
		{"empty", true, &v0.NamespaceDefinition{}},
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
			ns.Relation("editor", &v0.UsersetRewrite{}),
		)},
		{"nil union", true, ns.Namespace(
			"document",
			ns.Relation("editor", &v0.UsersetRewrite{
				RewriteOperation: &v0.UsersetRewrite_Union{},
			}),
		)},
		{"no children", true, ns.Namespace(
			"document",
			ns.Relation("editor", &v0.UsersetRewrite{
				RewriteOperation: &v0.UsersetRewrite_Union{
					Union: &v0.SetOperation{},
				},
			}),
		)},
		{"empty child", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_TupleToUserset{},
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
				&v0.SetOperation_Child{},
			)),
		)},
		{"bad ttu", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_TupleToUserset{
						TupleToUserset: &v0.TupleToUserset{},
					},
				},
			)),
		)},
		{"ttu missing tupleset", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_TupleToUserset{
						TupleToUserset: &v0.TupleToUserset{
							ComputedUserset: &v0.ComputedUserset{
								Object:   v0.ComputedUserset_TUPLE_USERSET_OBJECT,
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
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_TupleToUserset{
						TupleToUserset: &v0.TupleToUserset{
							Tupleset: &v0.TupleToUserset_Tupleset{
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
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_TupleToUserset{
						TupleToUserset: &v0.TupleToUserset{
							Tupleset: &v0.TupleToUserset_Tupleset{
								Relation: "",
							},
							ComputedUserset: &v0.ComputedUserset{
								Object:   v0.ComputedUserset_TUPLE_USERSET_OBJECT,
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
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_TupleToUserset{
						TupleToUserset: &v0.TupleToUserset{
							Tupleset: &v0.TupleToUserset_Tupleset{
								Relation: "parent",
							},
							ComputedUserset: &v0.ComputedUserset{
								Object:   v0.ComputedUserset_TUPLE_USERSET_OBJECT,
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
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_TupleToUserset{
						TupleToUserset: &v0.TupleToUserset{
							Tupleset: &v0.TupleToUserset_Tupleset{
								Relation: "parent",
							},
							ComputedUserset: &v0.ComputedUserset{
								Object: v0.ComputedUserset_TUPLE_USERSET_OBJECT,
							},
						},
					},
				},
			)),
		)},
		{"empty cu", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_ComputedUserset{},
				},
			)),
		)},
		{"cu empty relation", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_ComputedUserset{
						ComputedUserset: &v0.ComputedUserset{},
					},
				},
			)),
		)},
		{"cu bad relation name", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_ComputedUserset{
						ComputedUserset: &v0.ComputedUserset{
							Relation: "ab",
						},
					},
				},
			)),
		)},
		{"cu bad object type", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_ComputedUserset{
						ComputedUserset: &v0.ComputedUserset{
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
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_UsersetRewrite{},
				},
			)),
		)},
		{"child bad rewrite", true, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&v0.SetOperation_Child{
					ChildType: &v0.SetOperation_Child_UsersetRewrite{
						UsersetRewrite: &v0.UsersetRewrite{},
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
