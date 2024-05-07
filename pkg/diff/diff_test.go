package diff

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/diff/caveats"
	"github.com/authzed/spicedb/pkg/diff/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestDiffSchemas(t *testing.T) {
	tcs := []struct {
		name             string
		existingSchema   string
		comparisonSchema string
		expectedDiff     SchemaDiff
	}{
		{
			name:             "no changes",
			existingSchema:   `definition user {}`,
			comparisonSchema: `   definition user {}   `,
			expectedDiff:     SchemaDiff{},
		},
		{
			name:             "added namespace",
			existingSchema:   ``,
			comparisonSchema: `definition user {}`,
			expectedDiff: SchemaDiff{
				AddedNamespaces: []string{"user"},
			},
		},
		{
			name:             "removed namespace",
			existingSchema:   `definition user {}`,
			comparisonSchema: ``,
			expectedDiff: SchemaDiff{
				RemovedNamespaces: []string{"user"},
			},
		},
		{
			name:             "added caveat",
			existingSchema:   ``,
			comparisonSchema: `caveat someCaveat(someparam int) { someparam < 42 }`,
			expectedDiff: SchemaDiff{
				AddedCaveats: []string{"someCaveat"},
			},
		},
		{
			name:             "removed caveat",
			existingSchema:   `caveat someCaveat(someparam int) { someparam < 42 }`,
			comparisonSchema: ``,
			expectedDiff: SchemaDiff{
				RemovedCaveats: []string{"someCaveat"},
			},
		},
		{
			name:             "add and remove namespace and caveat",
			existingSchema:   `definition user {}`,
			comparisonSchema: `caveat someCaveat(someparam int) { someparam < 42 }`,
			expectedDiff: SchemaDiff{
				AddedCaveats:      []string{"someCaveat"},
				RemovedNamespaces: []string{"user"},
			},
		},
		{
			name:             "add and remove namespaces",
			existingSchema:   `definition user {}`,
			comparisonSchema: `definition user2 {}`,
			expectedDiff: SchemaDiff{
				AddedNamespaces:   []string{"user2"},
				RemovedNamespaces: []string{"user"},
			},
		},
		{
			name:             "add and remove caveats",
			existingSchema:   `caveat someCaveat(someparam int) { someparam < 42 }`,
			comparisonSchema: `caveat someCaveat2(someparam int) { someparam < 42 }`,
			expectedDiff: SchemaDiff{
				AddedCaveats:   []string{"someCaveat2"},
				RemovedCaveats: []string{"someCaveat"},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			existingSchema, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.existingSchema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			comparisonSchema, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.comparisonSchema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			diff, err := DiffSchemas(NewDiffableSchemaFromCompiledSchema(existingSchema), NewDiffableSchemaFromCompiledSchema(comparisonSchema))
			require.NoError(t, err)
			require.Equal(t, tc.expectedDiff, *diff)
		})
	}
}

func TestDiffSchemasWithChangedNamespace(t *testing.T) {
	existingSchema, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: `definition user {}`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	comparisonSchema, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: `definition user { relation somerel: user; }`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	diff, err := DiffSchemas(NewDiffableSchemaFromCompiledSchema(existingSchema), NewDiffableSchemaFromCompiledSchema(comparisonSchema))
	require.NoError(t, err)

	require.Len(t, diff.ChangedNamespaces, 1)
	require.Contains(t, diff.ChangedNamespaces, "user")
	require.Len(t, diff.ChangedNamespaces["user"].Deltas(), 1)
	require.Equal(t, namespace.AddedRelation, diff.ChangedNamespaces["user"].Deltas()[0].Type)
	require.Equal(t, "somerel", diff.ChangedNamespaces["user"].Deltas()[0].RelationName)
}

func TestDiffSchemasWithChangedCaveat(t *testing.T) {
	existingSchema, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: `caveat someCaveat(someparam int) { someparam < 42 }`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	comparisonSchema, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: `caveat someCaveat(someparam int) { someparam <= 42 }`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	diff, err := DiffSchemas(NewDiffableSchemaFromCompiledSchema(existingSchema), NewDiffableSchemaFromCompiledSchema(comparisonSchema))
	require.NoError(t, err)

	require.Len(t, diff.ChangedCaveats, 1)
	require.Contains(t, diff.ChangedCaveats, "someCaveat")
	require.Len(t, diff.ChangedCaveats["someCaveat"].Deltas(), 1)
	require.Equal(t, caveats.CaveatExpressionChanged, diff.ChangedCaveats["someCaveat"].Deltas()[0].Type)
}

func TestDiffSchemasWithChangedCaveatComment(t *testing.T) {
	existingSchema, err := compiler.Compile(compiler.InputSchema{
		Source: input.Source("schema"),
		SchemaString: `// hi there
		caveat someCaveat(someparam int) { someparam < 42 }`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	comparisonSchema, err := compiler.Compile(compiler.InputSchema{
		Source: input.Source("schema"),
		SchemaString: `// hello there
		caveat someCaveat(someparam int) { someparam < 42 }`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	diff, err := DiffSchemas(NewDiffableSchemaFromCompiledSchema(existingSchema), NewDiffableSchemaFromCompiledSchema(comparisonSchema))
	require.NoError(t, err)

	require.Len(t, diff.ChangedCaveats, 1)
	require.Contains(t, diff.ChangedCaveats, "someCaveat")
	require.Len(t, diff.ChangedCaveats["someCaveat"].Deltas(), 1)
	require.Equal(t, caveats.CaveatCommentsChanged, diff.ChangedCaveats["someCaveat"].Deltas()[0].Type)
}

type checker func(*testing.T, *DiffableSchema)

func TestDiffableSchema(t *testing.T) {
	tcs := []struct {
		name     string
		schema   string
		checkers []checker
	}{
		{
			name: "basic schema",
			schema: `
			definition user {}

			caveat someCaveat(someparam int) { someparam < 42 }

			definition resource {
				relation owner: user
				relation viewer: user
				permission view = owner + viewer
			}
			`,
			checkers: []checker{
				func(t *testing.T, ds *DiffableSchema) {
					ns, ok := ds.GetNamespace("user")
					require.True(t, ok)
					require.Equal(t, "user", ns.Name)
				},
				func(t *testing.T, ds *DiffableSchema) {
					ns, ok := ds.GetNamespace("resource")
					require.True(t, ok)
					require.Equal(t, "resource", ns.Name)
				},
				func(t *testing.T, ds *DiffableSchema) {
					caveat, ok := ds.GetCaveat("someCaveat")
					require.True(t, ok)
					require.Equal(t, "someCaveat", caveat.Name)
				},
				func(t *testing.T, ds *DiffableSchema) {
					_, ok := ds.GetRelation("user", "owner")
					require.False(t, ok)
				},
				func(t *testing.T, ds *DiffableSchema) {
					_, ok := ds.GetRelation("resource", "owner")
					require.True(t, ok)
				},
				func(t *testing.T, ds *DiffableSchema) {
					_, ok := ds.GetRelation("resource", "viewer")
					require.True(t, ok)
				},
				func(t *testing.T, ds *DiffableSchema) {
					_, ok := ds.GetNamespace("nonexistent")
					require.False(t, ok)
				},
				func(t *testing.T, ds *DiffableSchema) {
					_, ok := ds.GetCaveat("nonexistent")
					require.False(t, ok)
				},
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			schema, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: tc.schema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			diffableSchema := NewDiffableSchemaFromCompiledSchema(schema)
			for index, check := range tc.checkers {
				check := check
				t.Run(fmt.Sprintf("check-%d", index), func(t *testing.T) {
					check(t, &diffableSchema)
				})
			}
		})
	}
}
