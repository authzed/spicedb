package validation

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	ns "github.com/authzed/spicedb/pkg/namespace"
)

func TestNamespaceValidation(t *testing.T) {
	testCases := []struct {
		name   string
		cause  error
		config *pb.NamespaceDefinition
	}{
		{"empty", ErrInvalidNamespaceName, &pb.NamespaceDefinition{}},
		{"simple", nil, ns.Namespace("user")},
		{"full", nil, ns.Namespace(
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
		{"working intersection", nil, ns.Namespace(
			"document",
			ns.Relation("editor", ns.Intersection(
				ns.This(),
				ns.ComputedUserset("owner"),
			)),
		)},
		{"working exclusion", nil, ns.Namespace(
			"document",
			ns.Relation("editor", ns.Exclusion(
				ns.This(),
				ns.ComputedUserset("owner"),
			)),
		)},
		{"bad relation name", ErrInvalidRelationName, ns.Namespace(
			"document",
			ns.Relation("a", nil),
		)},
		{"bad rewrite", ErrUnknownRewriteOperation, ns.Namespace(
			"document",
			ns.Relation("editor", &pb.UsersetRewrite{}),
		)},
		{"nil union", ErrNilDefinition, ns.Namespace(
			"document",
			ns.Relation("editor", &pb.UsersetRewrite{
				RewriteOperation: &pb.UsersetRewrite_Union{},
			}),
		)},
		{"no children", ErrMissingChildren, ns.Namespace(
			"document",
			ns.Relation("editor", &pb.UsersetRewrite{
				RewriteOperation: &pb.UsersetRewrite_Union{
					Union: &pb.SetOperation{},
				},
			}),
		)},
		{"empty child", ErrNilDefinition, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_TupleToUserset{},
				},
			)),
		)},
		{"nil child pointer", ErrNilDefinition, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				nil,
			)),
		)},
		{"nil child", ErrUnknownSetChildType, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{},
			)),
		)},
		{"bad ttu", ErrTupleToUsersetBadRelation, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_TupleToUserset{
						TupleToUserset: &pb.TupleToUserset{},
					},
				},
			)),
		)},
		{"ttu missing tupleset", ErrTupleToUsersetBadRelation, ns.Namespace(
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
		{"ttu missing rewrite", ErrTupleToUsersetMissingUserset, ns.Namespace(
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
		{"ttu bad relation", ErrTupleToUsersetBadRelation, ns.Namespace(
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
		{"ttu bad computed relation", ErrInvalidRelationName, ns.Namespace(
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
		{"ttu nil computed relation", ErrInvalidRelationName, ns.Namespace(
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
		{"empty cu", ErrNilDefinition, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_ComputedUserset{},
				},
			)),
		)},
		{"cu empty relation", ErrInvalidRelationName, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_ComputedUserset{
						ComputedUserset: &pb.ComputedUserset{},
					},
				},
			)),
		)},
		{"cu bad relation name", ErrInvalidRelationName, ns.Namespace(
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
		{"cu bad object type", ErrComputedUsersetObject, ns.Namespace(
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
		{"child nil rewrite", ErrNilDefinition, ns.Namespace(
			"document",
			ns.Relation("viewer", ns.Union(
				&pb.SetOperation_Child{
					ChildType: &pb.SetOperation_Child_UsersetRewrite{},
				},
			)),
		)},
		{"child bad rewrite", ErrUnknownRewriteOperation, ns.Namespace(
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
			require := require.New(t)
			err := NamespaceConfig(tc.config)
			require.True(errors.Is(err, tc.cause))
		})
	}
}
