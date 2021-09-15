package namespace

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	v0 "github.com/authzed/spicedb/internal/proto/authzed/api/v0"
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
				), ns.RelationReference("document", "owner")),
			),
			[]*v0.NamespaceDefinition{},
			"direct relations are not allowed under relation `editor`",
		},
		{
			"relation in relation types has invalid namespace",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil, ns.RelationReference("someinvalidns", "...")),
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
				ns.Relation("owner", nil, ns.RelationReference("anotherns", "foobar")),
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
				ns.Relation("owner", nil, ns.RelationReference("user", "...")),
				ns.Relation("can_comment",
					nil,
					ns.RelationReference("user", "..."),
					ns.RelationReference("folder", "can_comment"),
				),
				ns.Relation("editor",
					ns.Union(
						ns.ComputedUserset("owner"),
						ns.This(),
					),
					ns.RelationReference("user", "..."),
				),
				ns.Relation("parent", nil, ns.RelationReference("folder", "...")),
				ns.Relation("viewer", ns.Union(
					ns.This(),
					ns.ComputedUserset("editor"),
					ns.TupleToUserset("parent", "viewer"),
				), ns.RelationReference("user", "...")),
			),
			[]*v0.NamespaceDefinition{
				ns.Namespace("user"),
				ns.Namespace(
					"folder",
					ns.Relation("can_comment", nil, ns.RelationReference("user", "...")),
					ns.Relation("parent", nil, ns.RelationReference("folder", "...")),
				),
			},
			"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
			require.NoError(err)

			nsm, err := NewCachingNamespaceManager(ds, 0*time.Second, nil)
			require.NoError(err)

			for _, otherNS := range tc.otherNamespaces {
				_, err := ds.WriteNamespace(context.Background(), otherNS)
				require.NoError(err)
			}

			ts, err := BuildNamespaceTypeSystem(tc.toCheck, nsm)
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
