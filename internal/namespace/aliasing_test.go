package namespace

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

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
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("view", ns.Union(
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
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("edit"),
				)),
				ns.Relation("another_viewer", ns.Union(
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
				ns.Relation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("owner", ns.Union(
					ns.ComputedUserset("edit"),
				)),
			),
			"there exists a cycle in permissions: [edit owner]",
			map[string]string{},
		},
		{
			"alias multi-level cycle",
			ns.Namespace(
				"document",
				ns.Relation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("owner", ns.Union(
					ns.ComputedUserset("foo"),
				)),
				ns.Relation("foo", ns.Union(
					ns.ComputedUserset("edit"),
				)),
			),
			"there exists a cycle in permissions: [edit foo owner]",
			map[string]string{},
		},
		{
			"multi-level aliasing",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("cool_viewer", ns.Union(
					ns.ComputedUserset("viewer"),
				)),
				ns.Relation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("admin", ns.Union(
					ns.ComputedUserset("edit"),
				)),
				ns.Relation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("edit"),
				)),
				ns.Relation("reallyadmin", ns.Union(
					ns.ComputedUserset("admin"),
				)),
				ns.Relation("reallynotadmin", ns.Union(
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
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("can_view", ns.Union(
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
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("edit", ns.Intersection(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("edit"),
				)),
				ns.Relation("somethingelse", ns.Exclusion(
					ns.ComputedUserset("owner"),
					ns.ComputedUserset("viewer"),
				)),
				ns.Relation("witharrow", ns.Union(
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
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("view", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.Nil(),
				)),
				ns.Relation("another", ns.Union(
					ns.Nil(),
				)),
			),
			"",
			map[string]string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			require.NoError(err)

			var lastRevision decimal.Decimal
			ts, err := BuildNamespaceTypeSystemForDatastore(tc.toCheck, ds.SnapshotReader(lastRevision))
			require.NoError(err)

			ctx := context.Background()
			vts, terr := ts.Validate(ctx)
			require.NoError(terr)

			computed, aerr := computePermissionAliases(vts)
			if tc.expectedError != "" {
				require.Equal(tc.expectedError, aerr.Error())
			} else {
				require.NoError(aerr)
				require.Equal(tc.expectedAliasMap, computed)
			}
		})
	}
}
