package namespace

import (
	"context"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	ns "github.com/authzed/spicedb/pkg/namespace"
)

func TestTypeSystem(t *testing.T) {
	testCases := []struct {
		name            string
		toCheck         *v0.NamespaceDefinition
		otherNamespaces []*v0.NamespaceDefinition
		expectedError   string
	}{
		{
			"invalid relation in computed_userset",
			ns.Namespace(
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
					ns.ComputedUserset("editors"),
					ns.TupleToUserset("parent", "viewer"),
				)),
			),
			[]*v0.NamespaceDefinition{},
			"under permission `viewer`: relation/permission `editors` was not found",
		},
		{
			"invalid relation in tuple_to_userset",
			ns.Namespace(
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
					ns.TupleToUserset("parents", "viewer"),
				)),
			),
			[]*v0.NamespaceDefinition{},
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
			[]*v0.NamespaceDefinition{},
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
			[]*v0.NamespaceDefinition{},
			"direct relations are not allowed under relation `editor`",
		},
		{
			"relation in relation types has invalid namespace",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.AllowedRelation("someinvalidns", "...")),
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
			),
			[]*v0.NamespaceDefinition{},
			"could not lookup definition `someinvalidns` for relation `owner`: object definition `someinvalidns` not found",
		},
		{
			"relation in relation types has invalid relation",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.AllowedRelation("anotherns", "foobar")),
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
			),
			[]*v0.NamespaceDefinition{
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
						ns.This(),
					),
					ns.AllowedRelation("user", "..."),
				),
				ns.Relation("parent", nil, ns.AllowedRelation("folder", "...")),
				ns.Relation("viewer", ns.Union(
					ns.This(),
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "viewer"),
				), ns.AllowedRelation("user", "..."), ns.AllowedPublicNamespace("user")),
			),
			[]*v0.NamespaceDefinition{
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
			[]*v0.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"group",
					ns.Relation("member", nil, ns.AllowedRelation("user", "..."), ns.AllowedPublicNamespace("user")),
				),
			},
			"for relation `viewer`: relation/permission `group#member` includes wildcard (public) type `user`: wildcard relations cannot be transitively included",
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
			[]*v0.NamespaceDefinition{
				ns.Namespace("user"),
			},
			"for arrow under relation `viewer`: relation `folder#parent` includes wildcard (public) type `folder`: wildcard relations cannot be used on the left side of arrows",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
			require.NoError(err)

			nsm, err := NewCachingNamespaceManager(ds, 0*time.Second, nil)
			require.NoError(err)

			var lastRevision decimal.Decimal
			for _, otherNS := range tc.otherNamespaces {
				var err error
				lastRevision, err = ds.WriteNamespace(context.Background(), otherNS)
				require.NoError(err)
			}

			ts, err := BuildNamespaceTypeSystemForManager(tc.toCheck, nsm, lastRevision)
			require.NoError(err)

			terr := ts.Validate(context.Background())
			if tc.expectedError == "" {
				require.NoError(terr)
			} else {
				require.Error(terr)
				require.Equal(tc.expectedError, terr.Error())
			}
		})
	}
}
