package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

const walkWrapperSchema = `caveat is_weekend(day int) { day == 6 || day == 7 }

definition user {}

definition document {
	relation viewer: user
	relation editor: user with is_weekend
	permission view = viewer + editor
}`

func compileWalkWrapperSchema(t *testing.T) *Schema {
	t.Helper()
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: walkWrapperSchema,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	return schema
}

func TestWalkCaveat_PublicWrapper(t *testing.T) {
	schema := compileWalkWrapperSchema(t)
	caveat, ok := schema.caveats["is_weekend"]
	require.True(t, ok)

	visitor := &testVisitor{}
	_, err := WalkCaveat(caveat, visitor, struct{}{})
	require.NoError(t, err)
	require.Len(t, visitor.caveats, 1)
	require.Same(t, caveat, visitor.caveats[0])
}

func TestWalkCaveatWithOptions_PublicWrapper(t *testing.T) {
	schema := compileWalkWrapperSchema(t)
	caveat := schema.caveats["is_weekend"]

	visitor := &testVisitor{}
	opts, err := NewWalkOptions().WithStrategy(WalkPostOrder).Build()
	require.NoError(t, err)

	_, err = WalkCaveatWithOptions(caveat, visitor, struct{}{}, opts)
	require.NoError(t, err)
	require.Len(t, visitor.caveats, 1)
}

func TestWalkBaseRelation_PublicWrapper(t *testing.T) {
	schema := compileWalkWrapperSchema(t)
	def := schema.definitions["document"]
	rel := def.relations["viewer"]
	require.NotEmpty(t, rel.baseRelations)

	visitor := &testVisitor{}
	_, err := WalkBaseRelation(rel.baseRelations[0], visitor, struct{}{})
	require.NoError(t, err)
	require.Len(t, visitor.baseRelations, 1)
}

func TestWalkBaseRelationWithOptions_PublicWrapper(t *testing.T) {
	schema := compileWalkWrapperSchema(t)
	rel := schema.definitions["document"].relations["viewer"]

	visitor := &testVisitor{}
	opts, err := NewWalkOptions().Build()
	require.NoError(t, err)

	_, err = WalkBaseRelationWithOptions(rel.baseRelations[0], visitor, struct{}{}, opts)
	require.NoError(t, err)
	require.Len(t, visitor.baseRelations, 1)
}

// TestWalkOptions_WithStrategyValueReceiver covers the value-receiver form of
// WithStrategy on WalkOptions (distinct from the *WalkOptionsBuilder method).
func TestWalkOptions_WithStrategyValueReceiver(t *testing.T) {
	original := defaultWalkOptions()
	require.Equal(t, WalkPreOrder, original.strategy)

	updated := original.WithStrategy(WalkPostOrder)
	require.Equal(t, WalkPostOrder, updated.strategy)
	// Original should remain unchanged (value receiver).
	require.Equal(t, WalkPreOrder, original.strategy)
}

func TestWalkOptionsBuilder_MustBuildPanicsOnError(t *testing.T) {
	// Passing nil to WithTraverseArrowTargets triggers an error inside ResolveSchema.
	builder := NewWalkOptions().
		WithStrategy(WalkPostOrder).
		WithTraverseArrowTargets(nil)

	require.Panics(t, func() {
		_ = builder.MustBuild()
	})
}
