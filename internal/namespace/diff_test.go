package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	ns "github.com/authzed/spicedb/pkg/namespace"
)

func TestNamespaceDiff(t *testing.T) {
	testCases := []struct {
		name           string
		existing       *core.NamespaceDefinition
		updated        *core.NamespaceDefinition
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
					ns.ComputedUserset("owner"),
				)),
			),
			[]Delta{
				{Type: ChangedRelationImpl, RelationName: "somerel"},
			},
		},
		{
			"changed relation impl 2",
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.ComputedUserset("editor"),
				)),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
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
					ns.ComputedUserset("owner"),
				)),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
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
					ns.ComputedUserset("owner"),
				)),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "bar")),
			),
			[]Delta{
				{Type: RelationDirectTypeAdded, RelationName: "somerel", DirectType: &core.RelationReference{
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
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "bar")),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				)),
			),
			[]Delta{
				{Type: RelationDirectTypeRemoved, RelationName: "somerel", DirectType: &core.RelationReference{
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
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "bar")),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "bar")),
			),
			[]Delta{},
		},
		{
			"type added and removed",
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "bar")),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo2", "bar")),
			),
			[]Delta{
				{Type: RelationDirectTypeRemoved, RelationName: "somerel", DirectType: &core.RelationReference{
					Namespace: "foo",
					Relation:  "bar",
				}},
				{Type: RelationDirectTypeAdded, RelationName: "somerel", DirectType: &core.RelationReference{
					Namespace: "foo2",
					Relation:  "bar",
				}},
			},
		},
		{
			"wildcard type added and removed",
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedPublicNamespace("foo")),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedPublicNamespace("foo2")),
			),
			[]Delta{
				{Type: RelationDirectWildcardTypeRemoved, RelationName: "somerel", WildcardType: "foo"},
				{Type: RelationDirectWildcardTypeAdded, RelationName: "somerel", WildcardType: "foo2"},
			},
		},
		{
			"wildcard type changed",
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedPublicNamespace("foo")),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "something")),
			),
			[]Delta{
				{Type: RelationDirectWildcardTypeRemoved, RelationName: "somerel", WildcardType: "foo"},
				{Type: RelationDirectTypeAdded, RelationName: "somerel", DirectType: &core.RelationReference{
					Namespace: "foo",
					Relation:  "something",
				}},
			},
		},
		{
			"wildcard type changed no rewrite",
			ns.Namespace(
				"document",
				ns.Relation("somerel", nil, ns.AllowedPublicNamespace("user")),
			),
			ns.Namespace(
				"document",
				ns.Relation("somerel", nil, ns.AllowedRelation("organization", "user")),
			),
			[]Delta{
				{Type: RelationDirectWildcardTypeRemoved, RelationName: "somerel", WildcardType: "user"},
				{Type: RelationDirectTypeAdded, RelationName: "somerel", DirectType: &core.RelationReference{
					Namespace: "organization",
					Relation:  "user",
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
