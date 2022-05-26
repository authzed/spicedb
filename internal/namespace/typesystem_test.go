package namespace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
)

func TestTypeSystem(t *testing.T) {
	testCases := []struct {
		name            string
		toCheck         *core.NamespaceDefinition
		otherNamespaces []*core.NamespaceDefinition
		expectedError   string
	}{
		{
			"invalid relation in computed_userset",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("parent", nil),
				ns.Relation("lock", nil),
				ns.Relation("viewer", ns.Union(
					ns.ComputedUserset("editors"),
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{},
			"under permission `viewer`: relation/permission `editors` was not found",
		},
		{
			"invalid relation in tuple_to_userset",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("parent", nil),
				ns.Relation("lock", nil),
				ns.Relation("viewer", ns.Union(
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parents", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{},
			"under permission `viewer`: relation `parents` was not found",
		},
		{
			"use of permission in tuple_to_userset",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("parent", nil),
				ns.Relation("lock", nil),
				ns.Relation("viewer", ns.Union(
					ns.TupleToUserset("editor", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{},
			"under permission `viewer`: permissions cannot be used on the left hand side of an arrow (found `editor`)",
		},
		{
			"rewrite without this and types",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				), ns.AllowedRelation("document", "owner")),
			),
			[]*core.NamespaceDefinition{},
			"direct relations are not allowed under relation `editor`",
		},
		{
			"relation in relation types has invalid namespace",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.AllowedRelation("someinvalidns", "...")),
				ns.Relation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("parent", nil),
				ns.Relation("lock", nil),
				ns.Relation("viewer", ns.Union(
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{},
			"could not lookup definition `someinvalidns` for relation `owner`: object definition `someinvalidns` not found",
		},
		{
			"relation in relation types has invalid relation",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.AllowedRelation("anotherns", "foobar")),
				ns.Relation("editor", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("parent", nil),
				ns.Relation("lock", nil),
				ns.Relation("viewer", ns.Union(
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace(
					"anotherns",
				),
			},
			"for relation `owner`: relation/permission `foobar` was not found under definition `anotherns`",
		},
		{
			"full type check",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.AllowedRelation("user", "...")),
				ns.Relation("can_comment",
					nil,
					ns.AllowedRelation("user", "..."),
					ns.AllowedRelation("folder", "can_comment"),
				),
				ns.Relation("editor",
					ns.Union(
						ns.ComputedUserset("owner"),
					),
				),
				ns.Relation("parent", nil, ns.AllowedRelation("folder", "...")),
				ns.Relation("viewer", nil, ns.AllowedRelation("user", "..."), ns.AllowedPublicNamespace("user")),
				ns.Relation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "view"),
				)),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"folder",
					ns.Relation("can_comment", nil, ns.AllowedRelation("user", "...")),
					ns.Relation("parent", nil, ns.AllowedRelation("folder", "...")),
				),
			},
			"",
		},
		{
			"transitive wildcard type check",
			ns.Namespace(
				"document",
				ns.Relation("viewer", nil, ns.AllowedRelation("user", "..."), ns.AllowedRelation("group", "member")),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"group",
					ns.Relation("member", nil, ns.AllowedRelation("user", "..."), ns.AllowedPublicNamespace("user")),
				),
			},
			"for relation `viewer`: relation/permission `group#member` includes wildcard type `user` via relation `group#member`: wildcard relations cannot be transitively included",
		},
		{
			"ttu wildcard type check",
			ns.Namespace(
				"folder",
				ns.Relation("parent", nil, ns.AllowedRelation("folder", "..."), ns.AllowedPublicNamespace("folder")),
				ns.Relation("viewer", ns.Union(
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
			},
			"for arrow under relation `viewer`: relation `folder#parent` includes wildcard type `folder` via relation `folder#parent`: wildcard relations cannot be used on the left side of arrows",
		},
		{
			"recursive transitive wildcard type check",
			ns.Namespace(
				"document",
				ns.Relation("viewer", nil, ns.AllowedRelation("user", "..."), ns.AllowedRelation("group", "member")),
			),
			[]*core.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"group",
					ns.Relation("member", nil, ns.AllowedRelation("group", "manager"), ns.AllowedRelation("user", "...")),
					ns.Relation("manager", nil, ns.AllowedRelation("group", "member"), ns.AllowedPublicNamespace("user")),
				),
			},
			"for relation `viewer`: relation/permission `group#member` includes wildcard type `user` via relation `group#manager`: wildcard relations cannot be transitively included",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(err)

			ctx := context.Background()

			lastRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				for _, otherNS := range tc.otherNamespaces {
					if err := rwt.WriteNamespaces(otherNS); err != nil {
						return err
					}
				}
				return nil
			})
			require.NoError(err)

			ts, err := BuildNamespaceTypeSystemForDatastore(tc.toCheck, ds.SnapshotReader(lastRevision))
			require.NoError(err)

			_, terr := ts.Validate(ctx)
			if tc.expectedError == "" {
				require.NoError(terr)
			} else {
				require.Error(terr)
				require.Equal(tc.expectedError, terr.Error())
			}
		})
	}
}
