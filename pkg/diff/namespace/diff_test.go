package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"

	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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
			"added namespace comments",
			ns.Namespace(
				"document",
			),
			ns.WithComment(
				"document",
				"some cool comment",
			),
			[]Delta{
				{Type: NamespaceCommentsChanged},
			},
		},
		{
			"unchanged namespace comments",
			ns.WithComment(
				"document",
				"some cool comment",
			),
			ns.WithComment(
				"document",
				"some cool comment",
			),
			[]Delta{},
		},
		{
			"changed namespace comments",
			ns.WithComment(
				"document",
				"some cool comment!",
			),
			ns.WithComment(
				"document",
				"some cool comment",
			),
			[]Delta{
				{Type: NamespaceCommentsChanged},
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
			"changed permission comment",
			ns.Namespace(
				"document",
				ns.MustRelationWithComment("somerel", "some comment", ns.Union(
					ns.ComputedUserset("editor"),
				)),
			),
			ns.Namespace(
				"document",
				ns.MustRelationWithComment("somerel", "some other comment", ns.Union(
					ns.ComputedUserset("editor"),
				)),
			),
			[]Delta{
				{Type: ChangedPermissionComment, RelationName: "somerel"},
			},
		},
		{
			"changed permission impl and comment",
			ns.Namespace(
				"document",
				ns.MustRelationWithComment("somerel", "some comment", ns.Union(
					ns.ComputedUserset("editor"),
				)),
			),
			ns.Namespace(
				"document",
				ns.MustRelationWithComment("somerel", "some other comment", ns.Union(
					ns.ComputedUserset("editor2"),
				)),
			),
			[]Delta{
				{Type: ChangedPermissionImpl, RelationName: "somerel"},
				{Type: ChangedPermissionComment, RelationName: "somerel"},
			},
		},
		{
			"no changes",
			ns.Namespace(
				"document",
				ns.MustRelationWithComment("somerel", "some comment", ns.Union(
					ns.ComputedUserset("editor"),
				)),
			),
			ns.Namespace(
				"document",
				ns.MustRelationWithComment("somerel", "some comment", ns.Union(
					ns.ComputedUserset("editor"),
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
			"changed relation comment",
			ns.Namespace(
				"document",
				ns.MustRelationWithComment("somerel", "some comment", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "bar")),
			),
			ns.Namespace(
				"document",
				ns.MustRelationWithComment("somerel", "changed comment", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("foo", "bar")),
			),
			[]Delta{
				{Type: ChangedRelationComment, RelationName: "somerel"},
			},
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
		{
			"location change does not cause expression change",
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.MustComputesUsersetWithSourcePosition("editor", 1),
				)),
			),
			ns.Namespace(
				"document",
				ns.MustRelation("somerel", ns.Union(
					ns.MustComputesUsersetWithSourcePosition("editor", 2),
				)),
			),
			[]Delta{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			diff, err := DiffNamespaces(tc.existing, tc.updated)
			require.NoError(err)
			require.Equal(tc.expectedDeltas, diff.Deltas())
		})
	}
}

func TestDiffDecorators(t *testing.T) {
	t.Parallel()

	withDecorator := func(name string) *core.Decorator {
		return &core.Decorator{Name: name, RequiredFlag: "testdecorators"}
	}

	t.Run("definition decorator added", func(t *testing.T) {
		t.Parallel()
		diff, err := DiffNamespaces(
			&core.NamespaceDefinition{Name: "document"},
			&core.NamespaceDefinition{Name: "document", Decorators: []*core.Decorator{withDecorator("testdef")}},
		)
		require.NoError(t, err)
		require.Equal(t, []Delta{{Type: NamespaceDecoratorsChanged}}, diff.Deltas())
	})

	t.Run("relation decorator changed", func(t *testing.T) {
		t.Parallel()
		existing := &core.NamespaceDefinition{
			Name:     "document",
			Relation: []*core.Relation{{Name: "viewer"}},
		}
		updated := &core.NamespaceDefinition{
			Name:     "document",
			Relation: []*core.Relation{{Name: "viewer", Decorators: []*core.Decorator{withDecorator("testrel")}}},
		}
		diff, err := DiffNamespaces(existing, updated)
		require.NoError(t, err)
		require.Contains(t, diff.Deltas(), Delta{Type: RelationDecoratorsChanged, RelationName: "viewer"})
	})

	t.Run("permission decorator changed", func(t *testing.T) {
		t.Parallel()
		newPerm := func(decorators []*core.Decorator) *core.Relation {
			perm := ns.MustRelation("view", ns.Union(ns.ComputedUserset("viewer")))
			perm.Decorators = decorators
			return perm
		}

		existing := &core.NamespaceDefinition{
			Name:     "document",
			Relation: []*core.Relation{newPerm(nil)},
		}
		updated := &core.NamespaceDefinition{
			Name:     "document",
			Relation: []*core.Relation{newPerm([]*core.Decorator{withDecorator("testrel")})},
		}
		diff, err := DiffNamespaces(existing, updated)
		require.NoError(t, err)
		require.Contains(t, diff.Deltas(), Delta{Type: RelationDecoratorsChanged, RelationName: "view"})
	})

	t.Run("subject type decorator produces add and remove", func(t *testing.T) {
		t.Parallel()
		allowed := func(ds ...*core.Decorator) *core.Relation {
			return &core.Relation{
				Name: "viewer",
				TypeInformation: &core.TypeInformation{
					AllowedDirectRelations: []*core.AllowedRelation{{
						Namespace:          "user",
						RelationOrWildcard: &core.AllowedRelation_Relation{Relation: "..."},
						Decorators:         ds,
					}},
				},
			}
		}

		diff, err := DiffNamespaces(
			&core.NamespaceDefinition{Name: "document", Relation: []*core.Relation{allowed()}},
			&core.NamespaceDefinition{Name: "document", Relation: []*core.Relation{allowed(withDecorator("testsub"))}},
		)
		require.NoError(t, err)
		require.Len(t, diff.Deltas(), 2)
	})

	t.Run("no decorator change produces no delta", func(t *testing.T) {
		t.Parallel()
		ns := func() *core.NamespaceDefinition {
			return &core.NamespaceDefinition{Name: "document", Decorators: []*core.Decorator{withDecorator("testdef")}}
		}
		diff, err := DiffNamespaces(ns(), ns())
		require.NoError(t, err)
		require.Empty(t, diff.Deltas())
	})
}
