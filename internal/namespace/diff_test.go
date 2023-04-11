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
				ns.MustRelation("somerel", nil),
			),
			[]Delta{
				{Type: AddedRelation, RelationName: "somerel"},
			},
		},
		{
			"remove relation",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil),
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
				ns.MustRelation("somerel", nil),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel2", nil),
			),
			[]Delta{
				{Type: RemovedRelation, RelationName: "somerel"},
				{Type: AddedRelation, RelationName: "somerel2"},
			},
		},
		{
			"added permission",
			ns.Namespace(
				"document",
			),
			ns.Namespace(
				"document",
				ns.MustRelation("someperm", ns.Union(ns.ComputedUserset("hiya"))),
			),
			[]Delta{
				{Type: AddedPermission, RelationName: "someperm"},
			},
		},
		{
			"remove permission",
			ns.Namespace(
				"document",
				ns.MustRelation("someperm", ns.Union(ns.ComputedUserset("hiya"))),
			),
			ns.Namespace(
				"document",
			),
			[]Delta{
				{Type: RemovedPermission, RelationName: "someperm"},
			},
		},
		{
			"renamed permission",
			ns.Namespace(
				"document",
				ns.MustRelation("someperm", ns.Union(ns.ComputedUserset("hiya"))),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("someperm2", ns.Union(ns.ComputedUserset("hiya"))),
			),
			[]Delta{
				{Type: RemovedPermission, RelationName: "someperm"},
				{Type: AddedPermission, RelationName: "someperm2"},
			},
		},
		{
			"legacy changed relation impl",
			ns.Namespace(
				"document",
				ns.MustRelation(
					"somerel",
					nil,
					ns.AllowedRelation("someothernamespace", "somerel"),
				),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel",
					ns.Union(
						ns.ComputedUserset("owner"),
					),
					ns.AllowedRelation("someothernamespace", "somerel"),
				),
			),
			[]Delta{
				{Type: LegacyChangedRelationImpl, RelationName: "somerel"},
			},
		},
		{
			"changed permission impl",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("editor"),
				)),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				)),
			),
			[]Delta{
				{Type: ChangedPermissionImpl, RelationName: "somerel"},
			},
		},
		{
			"no changes",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				)),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				)),
			),
			[]Delta{},
		},
		{
			"added direct type",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelation("foo", "bar")),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeAdded,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelation("foo", "bar"),
				},
			},
		},
		{
			"removed direct type",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelation("foo", "bar")),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeRemoved,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelation("foo", "bar"),
				},
			},
		},
		{
			"no changes with types",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "bar")),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "bar")),
			),
			[]Delta{},
		},
		{
			"type added and removed",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "bar")),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo2", "bar")),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeRemoved,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelation("foo", "bar"),
				},
				{
					Type:         RelationAllowedTypeAdded,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelation("foo2", "bar"),
				},
			},
		},
		{
			"wildcard type added and removed",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedPublicNamespace("foo")),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedPublicNamespace("foo2")),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeRemoved,
					RelationName: "somerel",
					AllowedType:  ns.AllowedPublicNamespace("foo"),
				},
				{
					Type:         RelationAllowedTypeAdded,
					RelationName: "somerel",
					AllowedType:  ns.AllowedPublicNamespace("foo2"),
				},
			},
		},
		{
			"wildcard type changed",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedPublicNamespace("foo")),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "something")),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeRemoved,
					RelationName: "somerel",
					AllowedType:  ns.AllowedPublicNamespace("foo"),
				},
				{
					Type:         RelationAllowedTypeAdded,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelation("foo", "something"),
				},
			},
		},
		{
			"wildcard type changed no rewrite",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedPublicNamespace("user")),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelation("organization", "user")),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeRemoved,
					RelationName: "somerel",
					AllowedType:  ns.AllowedPublicNamespace("user"),
				},
				{
					Type:         RelationAllowedTypeAdded,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelation("organization", "user"),
				},
			},
		},
		{
			"added relation and removed permission with same name",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(ns.ComputedUserset("someotherrel"))),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil),
			),
			[]Delta{
				{Type: AddedRelation, RelationName: "somerel"},
				{Type: RemovedPermission, RelationName: "somerel"},
			},
		},
		{
			"added permission and removed relation with same name",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(ns.ComputedUserset("someotherrel"))),
			),
			[]Delta{
				{Type: RemovedRelation, RelationName: "somerel"},
				{Type: AddedPermission, RelationName: "somerel"},
			},
		},
		{
			"added required caveat type",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelation("user", "...")),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat"))),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeRemoved,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelation("user", "..."),
				},
				{
					Type:         RelationAllowedTypeAdded,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")),
				},
			},
		},
		{
			"added optional caveat type",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelation("user", "...")),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelation("user", "..."), ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat"))),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeAdded,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")),
				},
			},
		},
		{
			"changed required caveat type",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat"))),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("anothercaveat"))),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeRemoved,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")),
				},
				{
					Type:         RelationAllowedTypeAdded,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("anothercaveat")),
				},
			},
		},
		{
			"removed required caveat type",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat"))),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeRemoved,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat")),
				},
			},
		},
		{
			"change required caveat type to optional",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat"))),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", nil, ns.AllowedRelation("user", "..."), ns.AllowedRelationWithCaveat("user", "...", ns.AllowedCaveat("somecaveat"))),
			),
			[]Delta{
				{
					Type:         RelationAllowedTypeAdded,
					RelationName: "somerel",
					AllowedType:  ns.AllowedRelation("user", "..."),
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			diff, err := DiffNamespaces(tc.existing, tc.updated)
			require.Nil(err)
			require.Equal(tc.expectedDeltas, diff.Deltas())
		})
	}
}
