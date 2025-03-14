package namespace

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
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
			"empty canonicalization",
			ns.Namespace(
				"document",
			),
			"",
			map[string]string{},
		},
		{
			"basic canonicalization",
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
				"owner":  "owner",
				"viewer": "viewer",
				"edit":   computedKeyPrefix + "596a8660f9a0c085",
				"view":   computedKeyPrefix + "0cb51da20fc9f20f",
			},
		},
		{
			"canonicalization with aliases",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("other_edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
			),
			"",
			map[string]string{
				"owner":      "owner",
				"viewer":     "viewer",
				"edit":       computedKeyPrefix + "596a8660f9a0c085",
				"other_edit": computedKeyPrefix + "596a8660f9a0c085",
			},
		},
		{
			"canonicalization with nested aliases",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("other_edit", ns.Union(
					ns.ComputedUserset("edit"),
				)),
			),
			"",
			map[string]string{
				"owner":      "owner",
				"viewer":     "viewer",
				"edit":       computedKeyPrefix + "596a8660f9a0c085",
				"other_edit": computedKeyPrefix + "596a8660f9a0c085",
			},
		},
		{
			"canonicalization with same union expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Union(
					ns.ComputedUserset("owner"),
					ns.ComputedUserset("viewer"),
				)),
				ns.MustRelation("second", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("owner"),
				)),
			),
			"",
			map[string]string{
				"owner":  "owner",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "62152badef526205",
				"second": computedKeyPrefix + "62152badef526205",
			},
		},
		{
			"canonicalization with same union expressions due to aliasing",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("edit", ns.Union(
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("first", ns.Union(
					ns.ComputedUserset("edit"),
					ns.ComputedUserset("viewer"),
				)),
				ns.MustRelation("second", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("edit"),
				)),
			),
			"",
			map[string]string{
				"owner":  "owner",
				"viewer": "viewer",
				"edit":   computedKeyPrefix + "596a8660f9a0c085",
				"first":  computedKeyPrefix + "62152badef526205",
				"second": computedKeyPrefix + "62152badef526205",
			},
		},
		{
			"canonicalization with same intersection expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Intersection(
					ns.ComputedUserset("owner"),
					ns.ComputedUserset("viewer"),
				)),
				ns.MustRelation("second", ns.Intersection(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("owner"),
				)),
			),
			"",
			map[string]string{
				"owner":  "owner",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "18cf8af8ff02bad0",
				"second": computedKeyPrefix + "18cf8af8ff02bad0",
			},
		},
		{
			"canonicalization with different expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Exclusion(
					ns.ComputedUserset("owner"),
					ns.ComputedUserset("viewer"),
				)),
				ns.MustRelation("second", ns.Exclusion(
					ns.ComputedUserset("viewer"),
					ns.ComputedUserset("owner"),
				)),
			),
			"",
			map[string]string{
				"owner":  "owner",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "2cd554a00f7f2d94",
				"second": computedKeyPrefix + "69d4722141f74043",
			},
		},
		{
			"canonicalization with arrow expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Union(
					ns.TupleToUserset("owner", "something"),
				)),
				ns.MustRelation("second", ns.Union(
					ns.TupleToUserset("owner", "something"),
				)),
				ns.MustRelation("difftuple", ns.Union(
					ns.TupleToUserset("viewer", "something"),
				)),
				ns.MustRelation("diffrel", ns.Union(
					ns.TupleToUserset("owner", "somethingelse"),
				)),
			),
			"",
			map[string]string{
				"owner":     "owner",
				"viewer":    "viewer",
				"first":     computedKeyPrefix + "9fd2b03cabeb2e42",
				"second":    computedKeyPrefix + "9fd2b03cabeb2e42",
				"diffrel":   computedKeyPrefix + "ab86f3a255f31908",
				"difftuple": computedKeyPrefix + "dddc650e89a7bf1a",
			},
		},
		{
			"canonicalization with same nested union expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Union(
					ns.ComputedUserset("owner"),
					ns.Rewrite(
						ns.Union(
							ns.ComputedUserset("editor"),
							ns.ComputedUserset("viewer"),
						),
					),
				)),
				ns.MustRelation("second", ns.Union(
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
			map[string]string{
				"owner":  "owner",
				"editor": "editor",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "4c49627fbdbaf248",
				"second": computedKeyPrefix + "4c49627fbdbaf248",
			},
		},
		{
			"canonicalization with same nested intersection expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Intersection(
					ns.ComputedUserset("owner"),
					ns.Rewrite(
						ns.Intersection(
							ns.ComputedUserset("editor"),
							ns.ComputedUserset("viewer"),
						),
					),
				)),
				ns.MustRelation("second", ns.Intersection(
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
			map[string]string{
				"owner":  "owner",
				"editor": "editor",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "7c52666bb7593f0a",
				"second": computedKeyPrefix + "7c52666bb7593f0a",
			},
		},
		{
			"canonicalization with different nested exclusion expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Exclusion(
					ns.ComputedUserset("owner"),
					ns.Rewrite(
						ns.Exclusion(
							ns.ComputedUserset("editor"),
							ns.ComputedUserset("viewer"),
						),
					),
				)),
				ns.MustRelation("second", ns.Exclusion(
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
			map[string]string{
				"owner":  "owner",
				"editor": "editor",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "bb955307170373ae",
				"second": computedKeyPrefix + "6ccf7bece2e540a1",
			},
		},
		{
			"canonicalization with nil expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Union(
					ns.ComputedUserset("owner"),
					ns.Nil(),
				)),
				ns.MustRelation("second", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.Nil(),
				)),
			),
			"",
			map[string]string{
				"owner":  "owner",
				"editor": "editor",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "95f5633117d42867",
				"second": computedKeyPrefix + "f786018d066f37b4",
			},
		},
		{
			"canonicalization with same expressions with nil expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.Nil(),
				)),
				ns.MustRelation("second", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.Nil(),
				)),
			),
			"",
			map[string]string{
				"owner":  "owner",
				"editor": "editor",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "bfc8d945d7030961",
				"second": computedKeyPrefix + "bfc8d945d7030961",
			},
		},
		{
			"canonicalization with functioned arrow expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Union(
					ns.TupleToUserset("owner", "something"),
				)),
				ns.MustRelation("second", ns.Union(
					ns.TupleToUserset("owner", "something"),
				)),
				ns.MustRelation("difftuple", ns.Union(
					ns.TupleToUserset("viewer", "something"),
				)),
				ns.MustRelation("third", ns.Union(
					ns.MustFunctionedTupleToUserset("owner", "any", "something"),
				)),
				ns.MustRelation("thirdwithall", ns.Union(
					ns.MustFunctionedTupleToUserset("owner", "all", "something"),
				)),
				ns.MustRelation("allplusanother", ns.Union(
					ns.MustFunctionedTupleToUserset("owner", "all", "something"),
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("anotherplusall", ns.Union(
					ns.ComputedUserset("owner"),
					ns.MustFunctionedTupleToUserset("owner", "all", "something"),
				)),
			),
			"",
			map[string]string{
				"owner":          "owner",
				"viewer":         "viewer",
				"first":          computedKeyPrefix + "9fd2b03cabeb2e42",
				"second":         computedKeyPrefix + "9fd2b03cabeb2e42",
				"third":          computedKeyPrefix + "9fd2b03cabeb2e42",
				"thirdwithall":   computedKeyPrefix + "eafa2f3f2d970680",
				"difftuple":      computedKeyPrefix + "dddc650e89a7bf1a",
				"allplusanother": computedKeyPrefix + "8b68ba1711b32ca4",
				"anotherplusall": computedKeyPrefix + "8b68ba1711b32ca4",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			ctx := context.Background()

			lastRevision, err := ds.HeadRevision(context.Background())
			require.NoError(err)

			ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(ds.SnapshotReader(lastRevision)))

			def, err := schema.NewDefinition(ts, tc.toCheck)
			require.NoError(err)

			vdef, derr := def.Validate(ctx)
			require.NoError(derr)

			aliases, aerr := computePermissionAliases(vdef)
			require.NoError(aerr)

			cacheKeys, cerr := computeCanonicalCacheKeys(vdef, aliases)
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
		{
			"nested exclusion non-associativity with nil",
			"viewer - (owner - nil)",
			"viewer - owner - nil",
			false,
		},
		{
			"nested intersection associativity with nil",
			"(viewer & owner) & nil",
			"(owner & viewer) & nil",
			true,
		},
		{
			"nested intersection associativity with nil 2",
			"(nil & owner) & editor",
			"(owner & nil) & editor",
			true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			require.NoError(err)

			ctx := context.Background()

			schemaText := fmt.Sprintf(comparisonSchemaTemplate, tc.first, tc.second)
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: schemaText,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(err)

			lastRevision, err := ds.HeadRevision(context.Background())
			require.NoError(err)

			ts := schema.NewTypeSystem(schema.ResolverForDatastoreReader(ds.SnapshotReader(lastRevision)))
			def, err := schema.NewDefinition(ts, compiled.ObjectDefinitions[0])
			require.NoError(err)

			vts, terr := def.Validate(ctx)
			require.NoError(terr)

			aliases, aerr := computePermissionAliases(vts)
			require.NoError(aerr)

			cacheKeys, cerr := computeCanonicalCacheKeys(vts, aliases)
			require.NoError(cerr)
			require.True((cacheKeys["first"] == cacheKeys["second"]) == tc.expectedSame)
		})
	}
}
