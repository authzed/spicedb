package namespace_test

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/testfixtures"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestListReferencedNamespaces(t *testing.T) {
	testCases := []struct {
		name          string
		toCheck       []*core.NamespaceDefinition
		expectedNames []string
	}{
		{
			"basic namespace",
			[]*core.NamespaceDefinition{
				ns.Namespace(
					"document",
					ns.MustRelation("owner", nil),
				),
			},
			[]string{"document"},
		},
		{
			"basic namespaces",
			[]*core.NamespaceDefinition{
				ns.Namespace(
					"document",
					ns.MustRelation("viewer", nil, ns.AllowedRelation("user", "...")),
				),
				ns.Namespace("user"),
			},
			[]string{"document", "user"},
		},
		{
			"basic namespaces with references",
			[]*core.NamespaceDefinition{
				ns.Namespace(
					"document",
					ns.MustRelation("viewer", nil,
						ns.AllowedRelation("group", "member"),
						ns.AllowedRelation("group", "manager"),
						ns.AllowedRelation("team", "member")),
				),
				ns.Namespace("user"),
			},
			[]string{"document", "user", "group", "team"},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			found := namespace.ListReferencedNamespaces(tc.toCheck)
			sort.Strings(found)
			sort.Strings(tc.expectedNames)

			require.Equal(tc.expectedNames, found)
		})
	}
}

func TestCheckNamespaceAndRelations(t *testing.T) {
	tcs := []struct {
		name          string
		schema        string
		checks        []namespace.TypeAndRelationToCheck
		expectedError string
	}{
		{
			"missing namespace",
			`definition user {}`,
			[]namespace.TypeAndRelationToCheck{
				{
					NamespaceName: "user",
					RelationName:  "...",
					AllowEllipsis: true,
				},
				{
					NamespaceName: "resource",
					RelationName:  "viewer",
					AllowEllipsis: false,
				},
			},
			"object definition `resource` not found",
		},
		{
			"missing relation",
			`definition resource {}`,
			[]namespace.TypeAndRelationToCheck{
				{
					NamespaceName: "resource",
					RelationName:  "viewer",
					AllowEllipsis: false,
				},
			},
			"relation/permission `viewer` not found",
		},
		{
			"valid",
			`definition user {}
			
			definition resource {
				relation viewer: user
			}
			`,
			[]namespace.TypeAndRelationToCheck{
				{
					NamespaceName: "user",
					RelationName:  "...",
					AllowEllipsis: true,
				},
				{
					NamespaceName: "resource",
					RelationName:  "viewer",
					AllowEllipsis: false,
				},
			},
			"",
		},
		{
			"ellipsis not allowed",
			`definition user {}`,
			[]namespace.TypeAndRelationToCheck{
				{
					NamespaceName: "user",
					RelationName:  "...",
					AllowEllipsis: false,
				},
			},
			"relation/permission `...` not found",
		},
		{
			"another missing namespace",
			`definition user {}`,
			[]namespace.TypeAndRelationToCheck{
				{
					NamespaceName: "resource",
					RelationName:  "viewer",
					AllowEllipsis: false,
				},
				{
					NamespaceName: "user",
					RelationName:  "...",
					AllowEllipsis: true,
				},
			},
			"object definition `resource` not found",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)
			rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			req.NoError(err)

			ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, tc.schema, nil, req)

			rev, err := ds.HeadRevision(context.Background())
			require.NoError(t, err)

			reader := ds.SnapshotReader(rev)

			err = namespace.CheckNamespaceAndRelations(context.Background(), tc.checks, reader)
			if tc.expectedError == "" {
				require.Nil(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectedError)
			}
		})
	}
}
