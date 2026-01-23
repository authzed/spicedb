package namespace

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	ns "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"
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
				ns.MustRelation("edit2", ns.Union(
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
				"edit2":  computedKeyPrefix + "596a8660f9a0c085",
				"view":   computedKeyPrefix + "62152badef526205",
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
				"first":  computedKeyPrefix + "591f62ba533d9c33",
				"second": computedKeyPrefix + "591f62ba533d9c33",
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
				"first":  computedKeyPrefix + "591f62ba533d9c33",
				"second": computedKeyPrefix + "591f62ba533d9c33",
			},
		},
		{
			"canonicalization with repeated relations",
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
					ns.ComputedUserset("viewer"),
				)),
			),
			"",
			map[string]string{
				"owner":  "owner",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "591f62ba533d9c33",
				"second": computedKeyPrefix + "591f62ba533d9c33",
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
				"first":  computedKeyPrefix + "cb6d0639b2405e56",
				"second": computedKeyPrefix + "cb6d0639b2405e56",
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
				"first":  computedKeyPrefix + "75093669c3281326",
				"second": computedKeyPrefix + "48f53d0dfbb85f59",
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
				"first":  computedKeyPrefix + "d421a51d48db3872",
				"second": computedKeyPrefix + "d421a51d48db3872",
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
				"first":  computedKeyPrefix + "b5aff5c18919bef0",
				"second": computedKeyPrefix + "b5aff5c18919bef0",
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
				"first":  computedKeyPrefix + "5355617f5b8ea218",
				"second": computedKeyPrefix + "ed41136b7aeb2264",
			},
		},
		{
			"canonicalization with aliased nil expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Union(
					ns.ComputedUserset("owner"),
					ns.Nil(),
				)),
				ns.MustRelation("aliased", ns.Union(
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
				"owner":   "owner",
				"editor":  "editor",
				"viewer":  "viewer",
				"first":   computedKeyPrefix + "a8662dfb4e430c9a",
				"aliased": computedKeyPrefix + "a8662dfb4e430c9a",
				"second":  computedKeyPrefix + "6e53cbcc9c210391",
			},
		},
		{
			"canonicalization with self expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Union(
					ns.ComputedUserset("owner"),
					ns.Self(),
				)),
				ns.MustRelation("second", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.Self(),
				)),
				ns.MustRelation("third", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.Nil(),
				)),
			),
			"",
			map[string]string{
				"owner":  "owner",
				"editor": "editor",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "cce7dece39bc4375",
				"second": computedKeyPrefix + "6b726ef17aeeba0e",
				"third":  computedKeyPrefix + "3e8d296baf7849e5",
			},
		},
		{
			"canonicalization with aliased self expressions",
			ns.Namespace(
				"document",
				ns.MustRelation("owner", nil),
				ns.MustRelation("editor", nil),
				ns.MustRelation("viewer", nil),
				ns.MustRelation("first", ns.Union(
					ns.ComputedUserset("owner"),
					ns.Self(),
				)),
				ns.MustRelation("alias", ns.Union(
					ns.Self(),
					ns.ComputedUserset("owner"),
				)),
				ns.MustRelation("second", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.Self(),
				)),
				ns.MustRelation("third", ns.Union(
					ns.ComputedUserset("viewer"),
					ns.Nil(),
				)),
			),
			"",
			map[string]string{
				"owner":  "owner",
				"editor": "editor",
				"viewer": "viewer",
				"first":  computedKeyPrefix + "eb99b65deae87e79",
				"alias":  computedKeyPrefix + "eb99b65deae87e79",
				"second": computedKeyPrefix + "48af971c7276d4d2",
				"third":  computedKeyPrefix + "1c828e67f6ce7848",
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
				"first":  computedKeyPrefix + "3692f3e8ea8d4a4b",
				"second": computedKeyPrefix + "3692f3e8ea8d4a4b",
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
				"allplusanother": computedKeyPrefix + "1588e0e67f417c5f",
				"anotherplusall": computedKeyPrefix + "1588e0e67f417c5f",
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
			require.NoError(err)

			ctx := t.Context()

			lastRevision, err := ds.HeadRevision(t.Context())
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

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
			require.NoError(err)

			ctx := t.Context()

			schemaText := fmt.Sprintf(comparisonSchemaTemplate, tc.first, tc.second)
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: schemaText,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(err)

			lastRevision, err := ds.HeadRevision(t.Context())
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
			require.Equal(tc.expectedSame, cacheKeys["first"] == cacheKeys["second"])
		})
	}
}
