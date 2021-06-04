package namespace

import (
	"testing"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDiff(t *testing.T) {
	testCases := []struct {
		name           string
		existing       *pb.NamespaceDefinition
		updated        *pb.NamespaceDefinition
		expectedDeltas []Delta
	}{
		{
			"added namespace",
			nil,
			ns.Namespace(
				"document",
			),
			[]Delta{
				{Type: NamespaceAdded},
			},
		},
		{
			"removed namespace",
			ns.Namespace(
				"document",
			),
			nil,
			[]Delta{
				{Type: NamespaceRemoved},
			},
		},
		{
			"added relation",
			ns.Namespace(
				"document",
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", nil),
			),
			[]Delta{
				{Type: AddedRelation, RelationName: "somerel"},
			},
		},
		{
			"remove relation",
			ns.Namespace(
				"document",
				ns.Relation("somerel", nil),
			),
			ns.Namespace(
				"document",
			),
			[]Delta{
				{Type: RemovedRelation, RelationName: "somerel"},
			},
		},
		{
			"renamed relation",
			ns.Namespace(
				"document",
				ns.Relation("somerel", nil),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel2", nil),
			),
			[]Delta{
				{Type: RemovedRelation, RelationName: "somerel"},
				{Type: AddedRelation, RelationName: "somerel2"},
			},
		},
		{
			"changed relation impl",
			ns.Namespace(
				"document",
				ns.Relation("somerel", nil),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				)),
			),
			[]Delta{
				{Type: ChangedRelationImpl, RelationName: "somerel"},
			},
		},
		{
			"no changes",
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				)),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				)),
			),
			[]Delta{},
		},
		{
			"added direct type",
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				)),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				), ns.RelationReference("foo", "bar")),
			),
			[]Delta{
				{Type: RelationDirectTypeAdded, RelationName: "somerel", DirectType: &pb.RelationReference{
					Namespace: "foo",
					Relation:  "bar",
				}},
			},
		},
		{
			"removed direct type",
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				), ns.RelationReference("foo", "bar")),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				)),
			),
			[]Delta{
				{Type: RelationDirectTypeRemoved, RelationName: "somerel", DirectType: &pb.RelationReference{
					Namespace: "foo",
					Relation:  "bar",
				}},
			},
		},
		{
			"no changes with types",
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				), ns.RelationReference("foo", "bar")),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				), ns.RelationReference("foo", "bar")),
			),
			[]Delta{},
		},
		{
			"type added and removed",
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				), ns.RelationReference("foo", "bar")),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.This(),
					ns.ComputedUserset("owner"),
				), ns.RelationReference("foo2", "bar")),
			),
			[]Delta{
				{Type: RelationDirectTypeRemoved, RelationName: "somerel", DirectType: &pb.RelationReference{
					Namespace: "foo",
					Relation:  "bar",
				}},
				{Type: RelationDirectTypeAdded, RelationName: "somerel", DirectType: &pb.RelationReference{
					Namespace: "foo2",
					Relation:  "bar",
				}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			diff, err := DiffNamespaces(tc.existing, tc.updated)
			require.Nil(err)
			require.Equal(tc.expectedDeltas, diff.Deltas())
		})
	}
}
