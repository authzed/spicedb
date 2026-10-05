package queryopttest

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Empty builds a leaf that never grants access.
func Empty() query.Outline {
	return query.Outline{Type: query.NullIteratorType}
}

// Fixed builds a leaf from relationship strings, failing the test on invalid input.
// Each relationship is an independent path, for example "doc:d#read@user:alice".
func Fixed(t testing.TB, relationships ...string) query.Outline {
	t.Helper()
	paths := make([]query.Path, 0, len(relationships))

	for _, text := range relationships {
		relationship, err := tuple.Parse(text)
		require.NoError(t, err, "invalid fixed path %q", text)
		paths = append(paths, *query.FromRelationship(relationship))
	}

	return query.Outline{Type: query.FixedIteratorType, Args: &query.IteratorArgs{FixedPaths: paths}}
}

// Union builds a tree granting access through any child, in the supplied order.
func Union(children ...query.Outline) query.Outline {
	return composite(query.UnionIteratorType, children...)
}

// Intersection builds a tree requiring access through every child.
func Intersection(children ...query.Outline) query.Outline {
	return composite(query.IntersectionIteratorType, children...)
}

// Exclusion builds a tree subtracting the right branch's grants from the left.
func Exclusion(left, right query.Outline) query.Outline {
	return composite(query.ExclusionIteratorType, left, right)
}

// Arrow builds a traversal joining the left branch's subjects to the right branch's resources.
// This represents an ordinary -> traversal; use FromSchema for schema-resolved arrows.
func Arrow(left, right query.Outline) query.Outline {
	return composite(query.ArrowIteratorType, left, right)
}

func composite(kind query.IteratorType, children ...query.Outline) query.Outline {
	// Own the child slice so later edits to a caller's slice do not change this tree.
	return query.Outline{Type: kind, SubOutlines: slices.Clone(children)}
}

// Canonical assigns canonical IDs and keys to a test tree, failing on invalid outlines.
// Canonicalization may reorder children; refer to the returned tree when selecting signal targets.
func Canonical(t testing.TB, root query.Outline) query.CanonicalOutline {
	t.Helper()
	outline, err := query.CanonicalizeOutline(root)
	require.NoError(t, err)
	return outline
}

// FromSchema compiles schema text and builds the named relation or permission's query.
// It uses the production schema planner, including its relation and arrow resolution.
func FromSchema(t testing.TB, text, definition, relation string, options ...query.BuildOption) query.CanonicalOutline {
	t.Helper()
	compiled, err := compiler.Compile(
		compiler.InputSchema{Source: input.Source("outline-test"), SchemaString: text},
		compiler.AllowUnprefixedObjectType(),
	)
	require.NoError(t, err)
	fullSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, compiled.CaveatDefinitions)
	require.NoError(t, err)
	outline, err := query.BuildOutlineFromSchema(fullSchema, definition, relation, options...)
	require.NoError(t, err)
	return outline
}
