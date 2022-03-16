package namespace

import (
	"context"
	"fmt"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	ns "github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestCanonicalization(t *testing.T) {
	testCases := []struct {
		name             string
		toCheck          *core.NamespaceDefinition
		expectedError    string
		expectedCacheMap map[string]string
	}{
		{
			"basic canonicalization",
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
				"edit": "596a8660f9a0c085", "view": "865dfa6397c4ea1c",
			},
		},
		{
			"canonicalization with aliases",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("other_edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
			),
			"",
			map[string]string{
				"edit": "596a8660f9a0c085", "other_edit": "596a8660f9a0c085",
			},
		},
		{
			"canonicalization with nested aliases",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("other_edit", ns.Union(
					ns.ComputedUserset("edit"),
				)),
			),
			"",
			map[string]string{
				"edit": "596a8660f9a0c085", "other_edit": "596a8660f9a0c085",
			},
		},
		{
			"canonicalization with same union expressions",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("first", ns.Union(
					ns.ComputedUserset("owner"),
					ns.ComputedUserset("viewer"),
				)),
				ns.Relation("second", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("owner"),
				)),
			),
			"",
			map[string]string{"first": "cb51da20fc9f20f", "second": "cb51da20fc9f20f"},
		},
		{
			"canonicalization with same union expressions due to aliasing",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.Relation("first", ns.Union(
					ns.ComputedUserset("edit"),
					ns.ComputedUserset("viewer"),
				)),
				ns.Relation("second", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("edit"),
				)),
			),
			"",
			map[string]string{"edit": "596a8660f9a0c085", "first": "cb51da20fc9f20f", "second": "cb51da20fc9f20f"},
		},
		{
			"canonicalization with same intersection expressions",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("first", ns.Intersection(
					ns.ComputedUserset("owner"),
					ns.ComputedUserset("viewer"),
				)),
				ns.Relation("second", ns.Intersection(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("owner"),
				)),
			),
			"",
			map[string]string{"first": "715c00bf909cba72", "second": "715c00bf909cba72"},
		},
		{
			"canonicalization with different expressions",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("first", ns.Exclusion(
					ns.ComputedUserset("owner"),
					ns.ComputedUserset("viewer"),
				)),
				ns.Relation("second", ns.Exclusion(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("owner"),
				)),
			),
			"",
			map[string]string{"first": "60d847d0d7349127", "second": "c4be69de0d88b140"},
		},
		{
			"canonicalization with arrow expressions",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("viewer", nil),
				ns.Relation("first", ns.Union(
					ns.TupleToUserset("owner", "something"),
				)),
				ns.Relation("second", ns.Union(
					ns.TupleToUserset("owner", "something"),
				)),
				ns.Relation("difftuple", ns.Union(
					ns.TupleToUserset("viewer", "something"),
				)),
				ns.Relation("diffrel", ns.Union(
					ns.TupleToUserset("owner", "somethingelse"),
				)),
			),
			"",
			map[string]string{"first": "9fd2b03cabeb2e42", "second": "9fd2b03cabeb2e42", "diffrel": "ab86f3a255f31908", "difftuple": "dddc650e89a7bf1a"},
		},
		{
			"canonicalization with same nested union expressions",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("editor", nil),
				ns.Relation("viewer", nil),
				ns.Relation("first", ns.Union(
					ns.ComputedUserset("owner"),
					ns.Rewrite(
						ns.Union(
							ns.ComputedUserset("editor"),
							ns.ComputedUserset("viewer"),
						),
					),
				)),
				ns.Relation("second", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.Rewrite(
						ns.Union(
							ns.ComputedUserset("editor"),
							ns.ComputedUserset("owner"),
						),
					),
				)),
			),
			"",
			map[string]string{"first": "cbbd2c2ca8cc5d06", "second": "cbbd2c2ca8cc5d06"},
		},
		{
			"canonicalization with same nested intersection expressions",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("editor", nil),
				ns.Relation("viewer", nil),
				ns.Relation("first", ns.Intersection(
					ns.ComputedUserset("owner"),
					ns.Rewrite(
						ns.Intersection(
							ns.ComputedUserset("editor"),
							ns.ComputedUserset("viewer"),
						),
					),
				)),
				ns.Relation("second", ns.Intersection(
					ns.ComputedUserset("viewer"),
					ns.Rewrite(
						ns.Intersection(
							ns.ComputedUserset("editor"),
							ns.ComputedUserset("owner"),
						),
					),
				)),
			),
			"",
			map[string]string{"first": "ecae2c4f917f1f54", "second": "ecae2c4f917f1f54"},
		},
		{
			"canonicalization with diffgerent nested exclusion expressions",
			ns.Namespace(
				"document",
				ns.Relation("owner", nil),
				ns.Relation("editor", nil),
				ns.Relation("viewer", nil),
				ns.Relation("first", ns.Exclusion(
					ns.ComputedUserset("owner"),
					ns.Rewrite(
						ns.Exclusion(
							ns.ComputedUserset("editor"),
							ns.ComputedUserset("viewer"),
						),
					),
				)),
				ns.Relation("second", ns.Exclusion(
					ns.ComputedUserset("viewer"),
					ns.Rewrite(
						ns.Exclusion(
							ns.ComputedUserset("editor"),
							ns.ComputedUserset("owner"),
						),
					),
				)),
			),
			"",
			map[string]string{"first": "eec1e7c6b1beb0d6", "second": "cf8b9b0d14b1fd05"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
			require.NoError(err)

			ctx := datastoremw.ContextWithDatastore(context.Background(), ds)
			nsm, err := NewCachingNamespaceManager(nil)
			require.NoError(err)

			var lastRevision decimal.Decimal
			ts, err := BuildNamespaceTypeSystemForManager(tc.toCheck, nsm, lastRevision)
			require.NoError(err)

			terr := ts.Validate(ctx)
			require.NoError(terr)

			aliases, aerr := computePermissionAliases(ts)
			require.NoError(aerr)

			cacheKeys, cerr := computeCanonicalCacheKeys(ts, aliases)
			require.NoError(cerr)
			require.Equal(tc.expectedCacheMap, cacheKeys)
		})
	}
}

const comparisonSchemaTemplate = `
definition document {
	relation viewer: document
	relation editor: document
	relation owner: document

	permission first = %s
	permission second = %s
}
`

func TestCanonicalizationComparison(t *testing.T) {
	testCases := []struct {
		name         string
		first        string
		second       string
		expectedSame bool
	}{
		{
			"same relation",
			"viewer",
			"viewer",
			true,
		},
		{
			"different relation",
			"viewer",
			"owner",
			false,
		},
		{
			"union associativity",
			"viewer + owner",
			"owner + viewer",
			true,
		},
		{
			"intersection associativity",
			"viewer & owner",
			"owner & viewer",
			true,
		},
		{
			"exclusion non-associativity",
			"viewer - owner",
			"owner - viewer",
			false,
		},
		{
			"nested union associativity",
			"viewer + (owner + editor)",
			"owner + (viewer + editor)",
			true,
		},
		{
			"nested intersection associativity",
			"viewer & (owner & editor)",
			"owner & (viewer & editor)",
			true,
		},
		{
			"nested union associativity 2",
			"(viewer + owner) + editor",
			"(owner + viewer) + editor",
			true,
		},
		{
			"nested intersection associativity 2",
			"(viewer & owner) & editor",
			"(owner & viewer) & editor",
			true,
		},
		{
			"nested exclusion non-associativity",
			"viewer - (owner - editor)",
			"viewer - owner - editor",
			false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
			require.NoError(err)

			ctx := datastoremw.ContextWithDatastore(context.Background(), ds)
			nsm, err := NewCachingNamespaceManager(nil)
			require.NoError(err)

			empty := ""
			schemaText := fmt.Sprintf(comparisonSchemaTemplate, tc.first, tc.second)
			defs, err := compiler.Compile([]compiler.InputSchema{
				{Source: input.Source("schema"), SchemaString: schemaText},
			}, &empty)

			var lastRevision decimal.Decimal
			ts, err := BuildNamespaceTypeSystemForManager(defs[0], nsm, lastRevision)
			require.NoError(err)

			terr := ts.Validate(ctx)
			require.NoError(terr)

			aliases, aerr := computePermissionAliases(ts)
			require.NoError(aerr)

			cacheKeys, cerr := computeCanonicalCacheKeys(ts, aliases)
			require.NoError(cerr)
			require.True((cacheKeys["first"] == cacheKeys["second"]) == tc.expectedSame)
		})
	}
}
