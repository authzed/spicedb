package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

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

func TestSimplifyOrWithSameCaveatDifferentContexts(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)

	// Create write_limit caveat: count < limit
	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"count": caveattypes.Default.UIntType,
		"limit": caveattypes.Default.UIntType,
	})
	require.NoError(err)

	writeLimitCaveat, err := caveats.CompileCaveatWithName(env, "count < limit", "write_limit")
	require.NoError(err)

	serialized, err := writeLimitCaveat.Serialize()
	require.NoError(err)

	caveatDef := &core.CaveatDefinition{
		Name:                 "write_limit",
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

	// Create OR expression: write_limit(limit=2) OR write_limit(limit=4)
	orExpr := internalcaveats.Or(
		&core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "write_limit",
					Context:    mustToStruct(t, map[string]any{"limit": uint64(2)}),
				},
			},
		},
		&core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "write_limit",
					Context:    mustToStruct(t, map[string]any{"limit": uint64(4)}),
				},
			},
		},
	)

	t.Run("Count3_ShouldPassSecondBranch", func(t *testing.T) {
		// count=3: (3 < 2) OR (3 < 4) = false OR true = true
		context := map[string]any{"count": uint64(3)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, reader)
		require.NoError(err)
		require.True(passes, "Expected OR expression to pass with count=3")
		require.Nil(simplified, "Expected simplified expression to be nil (unconditionally true)")
	})

	t.Run("Count1_ShouldPassBothBranches", func(t *testing.T) {
		// count=1: (1 < 2) OR (1 < 4) = true OR true = true
		context := map[string]any{"count": uint64(1)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, reader)
		require.NoError(err)
		require.True(passes, "Expected OR expression to pass with count=1")
		require.Nil(simplified, "Expected simplified expression to be nil (unconditionally true)")
	})

	t.Run("Count5_ShouldFailBothBranches", func(t *testing.T) {
		// count=5: (5 < 2) OR (5 < 4) = false OR false = false
		context := map[string]any{"count": uint64(5)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, reader)
		require.NoError(err)
		require.False(passes, "Expected OR expression to fail with count=5")
		require.NotNil(simplified, "Expected simplified expression to contain the failed OR")
	})
}

func TestSimplifyAndWithSameCaveatDifferentContexts(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)

	// Create write_limit caveat: count < limit
	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"count": caveattypes.Default.UIntType,
		"limit": caveattypes.Default.UIntType,
	})
	require.NoError(err)

	writeLimitCaveat, err := caveats.CompileCaveatWithName(env, "count < limit", "write_limit")
	require.NoError(err)

	serialized, err := writeLimitCaveat.Serialize()
	require.NoError(err)

	caveatDef := &core.CaveatDefinition{
		Name:                 "write_limit",
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

	// Create AND expression: write_limit(limit=2) AND write_limit(limit=4)
	andExpr := internalcaveats.And(
		&core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "write_limit",
					Context:    mustToStruct(t, map[string]any{"limit": uint64(2)}),
				},
			},
		},
		&core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "write_limit",
					Context:    mustToStruct(t, map[string]any{"limit": uint64(4)}),
				},
			},
		},
	)

	t.Run("Count1_ShouldPassBothBranchesAndTrue", func(t *testing.T) {
		// count=1: (1 < 2) AND (1 < 4) = true AND true = true
		context := map[string]any{"count": uint64(1)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, reader)
		require.NoError(err)
		require.True(passes, "Expected AND expression to pass with count=1")
		require.Nil(simplified, "Expected simplified expression to be nil (unconditionally true)")
	})

	t.Run("Count3_ShouldFailFirstBranch", func(t *testing.T) {
		// count=3: (3 < 2) AND (3 < 4) = false AND true = false
		context := map[string]any{"count": uint64(3)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, reader)
		require.NoError(err)
		require.False(passes, "Expected AND expression to fail with count=3")
		require.NotNil(simplified, "Expected simplified expression to contain the failed caveat")

		// Should return the failed caveat (limit=2)
		require.NotNil(simplified.GetCaveat())
		require.Equal("write_limit", simplified.GetCaveat().CaveatName)
		require.Equal(float64(2), simplified.GetCaveat().Context.AsMap()["limit"])
	})

	t.Run("Count5_ShouldFailBothBranches", func(t *testing.T) {
		// count=5: (5 < 2) AND (5 < 4) = false AND false = false
		context := map[string]any{"count": uint64(5)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, reader)
		require.NoError(err)
		require.False(passes, "Expected AND expression to fail with count=5")
		require.NotNil(simplified, "Expected simplified expression to contain the failed caveat")

		// Should return the first failed caveat (limit=2)
		require.NotNil(simplified.GetCaveat())
		require.Equal("write_limit", simplified.GetCaveat().CaveatName)
		require.Equal(float64(2), simplified.GetCaveat().Context.AsMap()["limit"])
	})
}

func TestSimplifyNotWithSameCaveatDifferentContexts(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)

	// Create write_limit caveat: count < limit
	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"count": caveattypes.Default.UIntType,
		"limit": caveattypes.Default.UIntType,
	})
	require.NoError(err)

	writeLimitCaveat, err := caveats.CompileCaveatWithName(env, "count < limit", "write_limit")
	require.NoError(err)

	serialized, err := writeLimitCaveat.Serialize()
	require.NoError(err)

	caveatDef := &core.CaveatDefinition{
		Name:                 "write_limit",
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

	// Create NOT expression: NOT write_limit(limit=4)
	notExpr := internalcaveats.Invert(
		&core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "write_limit",
					Context:    mustToStruct(t, map[string]any{"limit": uint64(4)}),
				},
			},
		},
	)

	t.Run("Count3_ShouldPassNegation", func(t *testing.T) {
		// count=3: NOT (3 < 4) = NOT true = false
		context := map[string]any{"count": uint64(3)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, notExpr, context, reader)
		require.NoError(err)
		require.False(passes, "Expected NOT expression to fail with count=3 (since 3 < 4 is true, NOT true = false)")
		require.NotNil(simplified, "Expected simplified expression to contain the failed NOT expression")
	})

	t.Run("Count5_ShouldPassNegation", func(t *testing.T) {
		// count=5: NOT (5 < 4) = NOT false = true
		context := map[string]any{"count": uint64(5)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, notExpr, context, reader)
		require.NoError(err)
		require.True(passes, "Expected NOT expression to pass with count=5 (since 5 < 4 is false, NOT false = true)")
		require.Nil(simplified, "Expected simplified expression to be nil (unconditionally true)")
	})
}

func TestSimplifyComplexNestedExpressions(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)

	// Create two different caveats
	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"count": caveattypes.Default.UIntType,
		"limit": caveattypes.Default.UIntType,
		"quota": caveattypes.Default.UIntType,
	})
	require.NoError(err)

	writeLimitCaveat, err := caveats.CompileCaveatWithName(env, "count < limit", "write_limit")
	require.NoError(err)

	quotaCheckCaveat, err := caveats.CompileCaveatWithName(env, "count < quota", "quota_check")
	require.NoError(err)

	serialized1, err := writeLimitCaveat.Serialize()
	require.NoError(err)

	serialized2, err := quotaCheckCaveat.Serialize()
	require.NoError(err)

	caveatDefs := []*core.CaveatDefinition{
		{
			Name:                 "write_limit",
			SerializedExpression: serialized1,
			ParameterTypes:       env.EncodedParametersTypes(),
		},
		{
			Name:                 "quota_check",
			SerializedExpression: serialized2,
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

	t.Run("OrOfAnds_ComplexNesting", func(t *testing.T) {
		// Create: (write_limit(limit=2) AND quota_check(quota=5)) OR (write_limit(limit=6) AND quota_check(quota=3))
		complexExpr := internalcaveats.Or(
			internalcaveats.And(
				&core.CaveatExpression{
					OperationOrCaveat: &core.CaveatExpression_Caveat{
						Caveat: &core.ContextualizedCaveat{
							CaveatName: "write_limit",
							Context:    mustToStruct(t, map[string]any{"limit": uint64(2)}),
						},
					},
				},
				&core.CaveatExpression{
					OperationOrCaveat: &core.CaveatExpression_Caveat{
						Caveat: &core.ContextualizedCaveat{
							CaveatName: "quota_check",
							Context:    mustToStruct(t, map[string]any{"quota": uint64(5)}),
						},
					},
				},
			),
			internalcaveats.And(
				&core.CaveatExpression{
					OperationOrCaveat: &core.CaveatExpression_Caveat{
						Caveat: &core.ContextualizedCaveat{
							CaveatName: "write_limit",
							Context:    mustToStruct(t, map[string]any{"limit": uint64(6)}),
						},
					},
				},
				&core.CaveatExpression{
					OperationOrCaveat: &core.CaveatExpression_Caveat{
						Caveat: &core.ContextualizedCaveat{
							CaveatName: "quota_check",
							Context:    mustToStruct(t, map[string]any{"quota": uint64(3)}),
						},
					},
				},
			),
		)

		// Test count=1: First AND is (1<2 AND 1<5) = true, so entire OR is true
		context1 := map[string]any{"count": uint64(1)}
		simplified1, passes1, err1 := SimplifyCaveatExpression(ctx, runner, complexExpr, context1, reader)
		require.NoError(err1)
		require.True(passes1, "Expected complex expression to pass with count=1")
		require.Nil(simplified1, "Expected simplified expression to be nil (unconditionally true)")

		// Test count=4: First AND is (4<2 AND 4<5) = false, Second AND is (4<6 AND 4<3) = false, so OR is false
		context2 := map[string]any{"count": uint64(4)}
		simplified2, passes2, err2 := SimplifyCaveatExpression(ctx, runner, complexExpr, context2, reader)
		require.NoError(err2)
		require.False(passes2, "Expected complex expression to fail with count=4")
		require.NotNil(simplified2, "Expected simplified expression to contain the failed OR")
	})

	t.Run("AndOfOrs_ComplexNesting", func(t *testing.T) {
		// Create: (write_limit(limit=2) OR write_limit(limit=6)) AND (quota_check(quota=3) OR quota_check(quota=7))
		complexExpr := internalcaveats.And(
			internalcaveats.Or(
				&core.CaveatExpression{
					OperationOrCaveat: &core.CaveatExpression_Caveat{
						Caveat: &core.ContextualizedCaveat{
							CaveatName: "write_limit",
							Context:    mustToStruct(t, map[string]any{"limit": uint64(2)}),
						},
					},
				},
				&core.CaveatExpression{
					OperationOrCaveat: &core.CaveatExpression_Caveat{
						Caveat: &core.ContextualizedCaveat{
							CaveatName: "write_limit",
							Context:    mustToStruct(t, map[string]any{"limit": uint64(6)}),
						},
					},
				},
			),
			internalcaveats.Or(
				&core.CaveatExpression{
					OperationOrCaveat: &core.CaveatExpression_Caveat{
						Caveat: &core.ContextualizedCaveat{
							CaveatName: "quota_check",
							Context:    mustToStruct(t, map[string]any{"quota": uint64(3)}),
						},
					},
				},
				&core.CaveatExpression{
					OperationOrCaveat: &core.CaveatExpression_Caveat{
						Caveat: &core.ContextualizedCaveat{
							CaveatName: "quota_check",
							Context:    mustToStruct(t, map[string]any{"quota": uint64(7)}),
						},
					},
				},
			),
		)

		// Test count=1: First OR is (1<2 OR 1<6) = true, Second OR is (1<3 OR 1<7) = true, so AND is true
		context1 := map[string]any{"count": uint64(1)}
		simplified1, passes1, err1 := SimplifyCaveatExpression(ctx, runner, complexExpr, context1, reader)
		require.NoError(err1)
		require.True(passes1, "Expected complex expression to pass with count=1")
		require.Nil(simplified1, "Expected simplified expression to be nil (unconditionally true)")

		// Test count=4: First OR is (4<2 OR 4<6) = true, Second OR is (4<3 OR 4<7) = true, so AND is true
		context2 := map[string]any{"count": uint64(4)}
		simplified2, passes2, err2 := SimplifyCaveatExpression(ctx, runner, complexExpr, context2, reader)
		require.NoError(err2)
		require.True(passes2, "Expected complex expression to pass with count=4")
		require.Nil(simplified2, "Expected simplified expression to be nil (unconditionally true)")

		// Test count=8: First OR is (8<2 OR 8<6) = false, so entire AND is false
		context3 := map[string]any{"count": uint64(8)}
		simplified3, passes3, err3 := SimplifyCaveatExpression(ctx, runner, complexExpr, context3, reader)
		require.NoError(err3)
		require.False(passes3, "Expected complex expression to fail with count=8")
		require.NotNil(simplified3, "Expected simplified expression to contain the failed first OR")
	})

	t.Run("NotOfOr_ComplexNesting", func(t *testing.T) {
		// Create: NOT (write_limit(limit=2) OR write_limit(limit=6))
		complexExpr := internalcaveats.Invert(
			internalcaveats.Or(
				&core.CaveatExpression{
					OperationOrCaveat: &core.CaveatExpression_Caveat{
						Caveat: &core.ContextualizedCaveat{
							CaveatName: "write_limit",
							Context:    mustToStruct(t, map[string]any{"limit": uint64(2)}),
						},
					},
				},
				&core.CaveatExpression{
					OperationOrCaveat: &core.CaveatExpression_Caveat{
						Caveat: &core.ContextualizedCaveat{
							CaveatName: "write_limit",
							Context:    mustToStruct(t, map[string]any{"limit": uint64(6)}),
						},
					},
				},
			),
		)

		// Test count=1: Inner OR is (1<2 OR 1<6) = true, so NOT true = false
		context1 := map[string]any{"count": uint64(1)}
		simplified1, passes1, err1 := SimplifyCaveatExpression(ctx, runner, complexExpr, context1, reader)
		require.NoError(err1)
		require.False(passes1, "Expected NOT expression to fail with count=1")
		require.NotNil(simplified1, "Expected simplified expression to contain the failed NOT")

		// Test count=8: Inner OR is (8<2 OR 8<6) = false, so NOT false = true
		context2 := map[string]any{"count": uint64(8)}
		simplified2, passes2, err2 := SimplifyCaveatExpression(ctx, runner, complexExpr, context2, reader)
		require.NoError(err2)
		require.True(passes2, "Expected NOT expression to pass with count=8")
		require.Nil(simplified2, "Expected simplified expression to be nil (unconditionally true)")
	})
}

func mustToStruct(t *testing.T, data map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(data)
	require.NoError(t, err)
	return s
}
