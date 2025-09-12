package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	internalcaveats "github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestSimplifyLeafCaveat(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)

	// Create test caveat
	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"value": caveattypes.Default.IntType,
	})
	require.NoError(err)

	compiled, err := caveats.CompileCaveatWithName(env, "value >= 42", "test_caveat")
	require.NoError(err)

	serialized, err := compiled.Serialize()
	require.NoError(err)

	caveatDef := &core.CaveatDefinition{
		Name:                 "test_caveat",
		SerializedExpression: serialized,
		ParameterTypes:       env.EncodedParametersTypes(),
	}

	// Create datastore with caveat
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteCaveats(ctx, []*core.CaveatDefinition{caveatDef})
	})
	require.NoError(err)

	reader := ds.SnapshotReader(revision)
	runner := internalcaveats.NewCaveatRunner(caveattypes.Default.TypeSet)

	// Create caveat expression without context
	caveatExpr := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: &core.ContextualizedCaveat{
				CaveatName: "test_caveat",
				Context:    nil, // No context in expression, will provide in call
			},
		},
	}

	t.Run("CaveatTrue", func(t *testing.T) {
		// Context that makes caveat true (45 >= 42)
		context := map[string]any{"value": int64(45)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, caveatExpr, context, reader)
		require.NoError(err)
		require.Nil(simplified) // nil means unconditionally true
		require.True(passes)    // should pass
	})

	t.Run("CaveatFalse", func(t *testing.T) {
		// Context that makes caveat false (30 < 42)
		context := map[string]any{"value": int64(30)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, caveatExpr, context, reader)
		require.NoError(err)
		require.NotNil(simplified) // Should return the expression (indicates what failed)
		require.False(passes)      // should not pass
		require.Equal(caveatExpr, simplified)
	})

	t.Run("CaveatPartial", func(t *testing.T) {
		// No context - caveat should be partial
		context := map[string]any{}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, caveatExpr, context, reader)
		require.NoError(err)
		require.NotNil(simplified) // Should return the expression (partial)
		require.True(passes)       // should pass conditionally
		require.Equal(caveatExpr, simplified)
	})
}

func TestSimplifyAndOperation(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)

	// Create two test caveats
	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"a": caveattypes.Default.IntType,
		"b": caveattypes.Default.IntType,
	})
	require.NoError(err)

	caveat1, err := caveats.CompileCaveatWithName(env, "a >= 10", "caveat1")
	require.NoError(err)
	caveat2, err := caveats.CompileCaveatWithName(env, "b >= 20", "caveat2")
	require.NoError(err)

	serialized1, err := caveat1.Serialize()
	require.NoError(err)
	serialized2, err := caveat2.Serialize()
	require.NoError(err)

	caveatDef1 := &core.CaveatDefinition{
		Name:                 "caveat1",
		SerializedExpression: serialized1,
		ParameterTypes:       env.EncodedParametersTypes(),
	}
	caveatDef2 := &core.CaveatDefinition{
		Name:                 "caveat2",
		SerializedExpression: serialized2,
		ParameterTypes:       env.EncodedParametersTypes(),
	}

	// Create datastore with caveats
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteCaveats(ctx, []*core.CaveatDefinition{caveatDef1, caveatDef2})
	})
	require.NoError(err)

	reader := ds.SnapshotReader(revision)
	runner := internalcaveats.NewCaveatRunner(caveattypes.Default.TypeSet)

	// Create AND expression: caveat1 AND caveat2
	andExpr := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op: core.CaveatOperation_AND,
				Children: []*core.CaveatExpression{
					{
						OperationOrCaveat: &core.CaveatExpression_Caveat{
							Caveat: &core.ContextualizedCaveat{CaveatName: "caveat1"},
						},
					},
					{
						OperationOrCaveat: &core.CaveatExpression_Caveat{
							Caveat: &core.ContextualizedCaveat{CaveatName: "caveat2"},
						},
					},
				},
			},
		},
	}

	t.Run("BothTrue_ReturnsNil", func(t *testing.T) {
		// Both caveats true: a=15 >= 10, b=25 >= 20
		context := map[string]any{"a": int64(15), "b": int64(25)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, reader)
		require.NoError(err)
		require.Nil(simplified) // AND of all true = true
		require.True(passes)    // should pass
	})

	t.Run("OneTrue_RemovesTrue", func(t *testing.T) {
		// First true, second partial: a=15 >= 10, no b value
		context := map[string]any{"a": int64(15)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, reader)
		require.NoError(err)
		require.NotNil(simplified)
		require.True(passes) // should pass conditionally

		// Should only have caveat2 left
		require.NotNil(simplified.GetCaveat())
		require.Equal("caveat2", simplified.GetCaveat().CaveatName)
	})

	t.Run("OneFalse_ReturnsFalse", func(t *testing.T) {
		// First false: a=5 < 10
		context := map[string]any{"a": int64(5), "b": int64(25)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, reader)
		require.NoError(err)
		require.NotNil(simplified)
		require.False(passes) // should not pass

		// Should return the false caveat
		require.NotNil(simplified.GetCaveat())
		require.Equal("caveat1", simplified.GetCaveat().CaveatName)
	})
}

func TestSimplifyOrOperation(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)

	// Create two test caveats
	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"a": caveattypes.Default.IntType,
		"b": caveattypes.Default.IntType,
	})
	require.NoError(err)

	caveat1, err := caveats.CompileCaveatWithName(env, "a >= 10", "caveat1")
	require.NoError(err)
	caveat2, err := caveats.CompileCaveatWithName(env, "b >= 20", "caveat2")
	require.NoError(err)

	serialized1, err := caveat1.Serialize()
	require.NoError(err)
	serialized2, err := caveat2.Serialize()
	require.NoError(err)

	caveatDef1 := &core.CaveatDefinition{
		Name:                 "caveat1",
		SerializedExpression: serialized1,
		ParameterTypes:       env.EncodedParametersTypes(),
	}
	caveatDef2 := &core.CaveatDefinition{
		Name:                 "caveat2",
		SerializedExpression: serialized2,
		ParameterTypes:       env.EncodedParametersTypes(),
	}

	// Create datastore with caveats
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteCaveats(ctx, []*core.CaveatDefinition{caveatDef1, caveatDef2})
	})
	require.NoError(err)

	reader := ds.SnapshotReader(revision)
	runner := internalcaveats.NewCaveatRunner(caveattypes.Default.TypeSet)

	// Create OR expression: caveat1 OR caveat2
	orExpr := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op: core.CaveatOperation_OR,
				Children: []*core.CaveatExpression{
					{
						OperationOrCaveat: &core.CaveatExpression_Caveat{
							Caveat: &core.ContextualizedCaveat{CaveatName: "caveat1"},
						},
					},
					{
						OperationOrCaveat: &core.CaveatExpression_Caveat{
							Caveat: &core.ContextualizedCaveat{CaveatName: "caveat2"},
						},
					},
				},
			},
		},
	}

	t.Run("OneTrue_ReturnsNil", func(t *testing.T) {
		// First true: a=15 >= 10
		context := map[string]any{"a": int64(15), "b": int64(5)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, reader)
		require.NoError(err)
		require.Nil(simplified) // OR with any true = true
		require.True(passes)    // should pass
	})

	t.Run("OneFalse_RemovesFalse", func(t *testing.T) {
		// First false, second partial: a=5 < 10, no b value
		context := map[string]any{"a": int64(5)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, reader)
		require.NoError(err)
		require.NotNil(simplified)
		require.True(passes) // should pass conditionally

		// Should only have caveat2 left
		require.NotNil(simplified.GetCaveat())
		require.Equal("caveat2", simplified.GetCaveat().CaveatName)
	})

	t.Run("BothFalse_ReturnsExpression", func(t *testing.T) {
		// Both false: a=5 < 10, b=15 < 20
		context := map[string]any{"a": int64(5), "b": int64(15)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, reader)
		require.NoError(err)
		require.NotNil(simplified)
		require.False(passes) // should not pass

		// Should return original expression since all are false
		require.NotNil(simplified.GetOperation())
		require.Equal(core.CaveatOperation_OR, simplified.GetOperation().Op)
	})
}

func TestSimplifyNestedOperations(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)

	// Create test caveats
	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"a": caveattypes.Default.IntType,
		"b": caveattypes.Default.IntType,
		"c": caveattypes.Default.IntType,
	})
	require.NoError(err)

	caveat1, err := caveats.CompileCaveatWithName(env, "a >= 10", "caveat1")
	require.NoError(err)
	caveat2, err := caveats.CompileCaveatWithName(env, "b >= 20", "caveat2")
	require.NoError(err)
	caveat3, err := caveats.CompileCaveatWithName(env, "c >= 30", "caveat3")
	require.NoError(err)

	serialized1, err := caveat1.Serialize()
	require.NoError(err)
	serialized2, err := caveat2.Serialize()
	require.NoError(err)
	serialized3, err := caveat3.Serialize()
	require.NoError(err)

	caveatDefs := []*core.CaveatDefinition{
		{
			Name:                 "caveat1",
			SerializedExpression: serialized1,
			ParameterTypes:       env.EncodedParametersTypes(),
		},
		{
			Name:                 "caveat2",
			SerializedExpression: serialized2,
			ParameterTypes:       env.EncodedParametersTypes(),
		},
		{
			Name:                 "caveat3",
			SerializedExpression: serialized3,
			ParameterTypes:       env.EncodedParametersTypes(),
		},
	}

	// Create datastore with caveats
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteCaveats(ctx, caveatDefs)
	})
	require.NoError(err)

	reader := ds.SnapshotReader(revision)
	runner := internalcaveats.NewCaveatRunner(caveattypes.Default.TypeSet)

	// Create nested expression: (caveat1 OR caveat2) AND caveat3
	nestedExpr := &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Operation{
			Operation: &core.CaveatOperation{
				Op: core.CaveatOperation_AND,
				Children: []*core.CaveatExpression{
					{
						OperationOrCaveat: &core.CaveatExpression_Operation{
							Operation: &core.CaveatOperation{
								Op: core.CaveatOperation_OR,
								Children: []*core.CaveatExpression{
									{
										OperationOrCaveat: &core.CaveatExpression_Caveat{
											Caveat: &core.ContextualizedCaveat{CaveatName: "caveat1"},
										},
									},
									{
										OperationOrCaveat: &core.CaveatExpression_Caveat{
											Caveat: &core.ContextualizedCaveat{CaveatName: "caveat2"},
										},
									},
								},
							},
						},
					},
					{
						OperationOrCaveat: &core.CaveatExpression_Caveat{
							Caveat: &core.ContextualizedCaveat{CaveatName: "caveat3"},
						},
					},
				},
			},
		},
	}

	t.Run("SimplifiesNestedOr", func(t *testing.T) {
		// caveat1 true, caveat3 partial: (true OR x) AND partial = partial
		context := map[string]any{"a": int64(15)} // caveat1 true, others partial

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, nestedExpr, context, reader)
		require.NoError(err)
		require.NotNil(simplified)
		require.True(passes) // should pass conditionally

		// Should simplify to just caveat3
		require.NotNil(simplified.GetCaveat())
		require.Equal("caveat3", simplified.GetCaveat().CaveatName)
	})
}
