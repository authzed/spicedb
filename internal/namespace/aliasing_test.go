package namespace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	ns "github.com/authzed/spicedb/pkg/namespace"
)

func TestAliasing(t *testing.T) {
	testCases := []struct {
		name             string
		toCheck          *core.NamespaceDefinition
		expectedError    string
		expectedAliasMap map[string]string
	}{
		{
			"basic aliasing",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("edit"),
				)),
			),
			"",
			map[string]string{
				"edit": "owner",
			},
		},
		{
			"multiple aliasing",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("edit"),
				)),
				ns.MustRelation("another_viewer", ns.Union(
					ns.ComputedUserset("viewer"),
				)),
			),
			"",
			map[string]string{
				"edit":           "owner",
				"another_viewer": "viewer",
			},
		},
		{
			"alias cycle",
			ns.Namespace(
				"document",
				ns.MustRelation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("owner", ns.Union(
					ns.ComputedUserset("edit"),
				)),
			),
			"under definition `document`, there exists a cycle in permissions: edit, owner",
			map[string]string{},
		},
		{
			"alias multi-level cycle",
			ns.Namespace(
				"document",
				ns.MustRelation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("owner", ns.Union(
					ns.ComputedUserset("foo"),
				)),
				ns.MustRelation("foo", ns.Union(
					ns.ComputedUserset("edit"),
				)),
			),
			"under definition `document`, there exists a cycle in permissions: edit, foo, owner",
			map[string]string{},
		},
		{
			"multi-level aliasing",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("cool_viewer", ns.Union(
					ns.ComputedUserset("viewer"),
				)),
				ns.MustRelation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("admin", ns.Union(
					ns.ComputedUserset("edit"),
				)),
				ns.MustRelation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("edit"),
				)),
				ns.MustRelation("reallyadmin", ns.Union(
					ns.ComputedUserset("admin"),
				)),
				ns.MustRelation("reallynotadmin", ns.Union(
					ns.ComputedUserset("admin"),
					ns.ComputedUserset("viewer"),
				)),
			),
			"",
			map[string]string{
				"edit":        "owner",
				"admin":       "owner",
				"reallyadmin": "owner",
				"cool_viewer": "viewer",
			},
		},
		{
			"permission-only alias",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("can_view", ns.Union(
					ns.ComputedUserset("view"),
				)),
			),
			"",
			map[string]string{
				"can_view": "view",
			},
		},
		{
			"non-aliasing",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("edit", ns.Intersection(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("edit"),
				)),
				ns.MustRelation("somethingelse", ns.Exclusion(
					ns.ComputedUserset("owner"),
					ns.ComputedUserset("viewer"),
				)),
				ns.MustRelation("witharrow", ns.Union(
					ns.TupleToUserset("owner", "something"),
				)),
			),
			"",
			map[string]string{},
		},
		{
			"non-aliasing with nil",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.Nil(),
				)),
				ns.MustRelation("another", ns.Union(
					ns.Nil(),
				)),
			),
			"",
			map[string]string{},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			lastRevision, err := ds.HeadRevision(context.Background())
			require.NoError(err)

			ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(ds.SnapshotReader(lastRevision)))

			def, err := schema.NewDefinition(ts, tc.toCheck)
			require.NoError(err)

			ctx := context.Background()
			vdef, terr := def.Validate(ctx)
			require.NoError(terr)

			computed, aerr := computePermissionAliases(vdef)
			if tc.expectedError != "" {
				require.Equal(tc.expectedError, aerr.Error())
			} else {
				require.NoError(aerr)
				require.Equal(tc.expectedAliasMap, computed)
			}
		})
	}
}
