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
	"github.com/authzed/spicedb/pkg/datalayer"
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
		return tx.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{caveatDef})
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
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

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, caveatExpr, context, sr)
		require.NoError(err)
		require.Nil(simplified) // nil means unconditionally true
		require.True(passes)    // should pass
	})

	t.Run("CaveatFalse", func(t *testing.T) {
		// Context that makes caveat false (30 < 42)
		context := map[string]any{"value": int64(30)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, caveatExpr, context, sr)
		require.NoError(err)
		require.NotNil(simplified) // Should return the expression (indicates what failed)
		require.False(passes)      // should not pass
		require.Equal(caveatExpr, simplified)
	})

	t.Run("CaveatPartial", func(t *testing.T) {
		// No context - caveat should be partial
		context := map[string]any{}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, caveatExpr, context, sr)
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
		return tx.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{caveatDef1, caveatDef2})
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
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

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, sr)
		require.NoError(err)
		require.Nil(simplified) // AND of all true = true
		require.True(passes)    // should pass
	})

	t.Run("OneTrue_RemovesTrue", func(t *testing.T) {
		// First true, second partial: a=15 >= 10, no b value
		context := map[string]any{"a": int64(15)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, sr)
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

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, sr)
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
		return tx.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{caveatDef1, caveatDef2})
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
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

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, sr)
		require.NoError(err)
		require.Nil(simplified) // OR with any true = true
		require.True(passes)    // should pass
	})

	t.Run("OneFalse_RemovesFalse", func(t *testing.T) {
		// First false, second partial: a=5 < 10, no b value
		context := map[string]any{"a": int64(5)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, sr)
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

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, sr)
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
		return tx.LegacyWriteCaveats(ctx, caveatDefs)
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
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

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, nestedExpr, context, sr)
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
		return tx.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{caveatDef})
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
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

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, sr)
		require.NoError(err)
		require.True(passes, "Expected OR expression to pass with count=3")
		require.Nil(simplified, "Expected simplified expression to be nil (unconditionally true)")
	})

	t.Run("Count1_ShouldPassBothBranches", func(t *testing.T) {
		// count=1: (1 < 2) OR (1 < 4) = true OR true = true
		context := map[string]any{"count": uint64(1)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, sr)
		require.NoError(err)
		require.True(passes, "Expected OR expression to pass with count=1")
		require.Nil(simplified, "Expected simplified expression to be nil (unconditionally true)")
	})

	t.Run("Count5_ShouldFailBothBranches", func(t *testing.T) {
		// count=5: (5 < 2) OR (5 < 4) = false OR false = false
		context := map[string]any{"count": uint64(5)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, orExpr, context, sr)
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
		return tx.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{caveatDef})
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
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

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, sr)
		require.NoError(err)
		require.True(passes, "Expected AND expression to pass with count=1")
		require.Nil(simplified, "Expected simplified expression to be nil (unconditionally true)")
	})

	t.Run("Count3_ShouldFailFirstBranch", func(t *testing.T) {
		// count=3: (3 < 2) AND (3 < 4) = false AND true = false
		context := map[string]any{"count": uint64(3)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, sr)
		require.NoError(err)
		require.False(passes, "Expected AND expression to fail with count=3")
		require.NotNil(simplified, "Expected simplified expression to contain the failed caveat")

		// Should return the failed caveat (limit=2)
		require.NotNil(simplified.GetCaveat())
		require.Equal("write_limit", simplified.GetCaveat().CaveatName)
		require.Equal(float64(2), simplified.GetCaveat().Context.AsMap()["limit"]) //nolint:testifylint // this value isn't being operated on
	})

	t.Run("Count5_ShouldFailBothBranches", func(t *testing.T) {
		// count=5: (5 < 2) AND (5 < 4) = false AND false = false
		context := map[string]any{"count": uint64(5)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, andExpr, context, sr)
		require.NoError(err)
		require.False(passes, "Expected AND expression to fail with count=5")
		require.NotNil(simplified, "Expected simplified expression to contain the failed caveat")

		// Should return the first failed caveat (limit=2)
		require.NotNil(simplified.GetCaveat())
		require.Equal("write_limit", simplified.GetCaveat().CaveatName)
		require.Equal(float64(2), simplified.GetCaveat().Context.AsMap()["limit"]) //nolint:testifylint // this value isn't being operated on
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
		return tx.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{caveatDef})
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
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

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, notExpr, context, sr)
		require.NoError(err)
		require.False(passes, "Expected NOT expression to fail with count=3 (since 3 < 4 is true, NOT true = false)")
		require.NotNil(simplified, "Expected simplified expression to contain the failed NOT expression")
	})

	t.Run("Count5_ShouldPassNegation", func(t *testing.T) {
		// count=5: NOT (5 < 4) = NOT false = true
		context := map[string]any{"count": uint64(5)}

		simplified, passes, err := SimplifyCaveatExpression(ctx, runner, notExpr, context, sr)
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
		return tx.LegacyWriteCaveats(ctx, caveatDefs)
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
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
		simplified1, passes1, err1 := SimplifyCaveatExpression(ctx, runner, complexExpr, context1, sr)
		require.NoError(err1)
		require.True(passes1, "Expected complex expression to pass with count=1")
		require.Nil(simplified1, "Expected simplified expression to be nil (unconditionally true)")

		// Test count=4: First AND is (4<2 AND 4<5) = false, Second AND is (4<6 AND 4<3) = false, so OR is false
		context2 := map[string]any{"count": uint64(4)}
		simplified2, passes2, err2 := SimplifyCaveatExpression(ctx, runner, complexExpr, context2, sr)
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
		simplified1, passes1, err1 := SimplifyCaveatExpression(ctx, runner, complexExpr, context1, sr)
		require.NoError(err1)
		require.True(passes1, "Expected complex expression to pass with count=1")
		require.Nil(simplified1, "Expected simplified expression to be nil (unconditionally true)")

		// Test count=4: First OR is (4<2 OR 4<6) = true, Second OR is (4<3 OR 4<7) = true, so AND is true
		context2 := map[string]any{"count": uint64(4)}
		simplified2, passes2, err2 := SimplifyCaveatExpression(ctx, runner, complexExpr, context2, sr)
		require.NoError(err2)
		require.True(passes2, "Expected complex expression to pass with count=4")
		require.Nil(simplified2, "Expected simplified expression to be nil (unconditionally true)")

		// Test count=8: First OR is (8<2 OR 8<6) = false, so entire AND is false
		context3 := map[string]any{"count": uint64(8)}
		simplified3, passes3, err3 := SimplifyCaveatExpression(ctx, runner, complexExpr, context3, sr)
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
		simplified1, passes1, err1 := SimplifyCaveatExpression(ctx, runner, complexExpr, context1, sr)
		require.NoError(err1)
		require.False(passes1, "Expected NOT expression to fail with count=1")
		require.NotNil(simplified1, "Expected simplified expression to contain the failed NOT")

		// Test count=8: Inner OR is (8<2 OR 8<6) = false, so NOT false = true
		context2 := map[string]any{"count": uint64(8)}
		simplified2, passes2, err2 := SimplifyCaveatExpression(ctx, runner, complexExpr, context2, sr)
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

// Additional tests for uncovered functions in simplify_caveat.go

func TestMergeContextsForExpression(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Test merging contexts for complex expressions
	queryContext := map[string]any{
		"user_id":    "alice",
		"query_data": 123,
	}

	t.Run("Simple caveat expression", func(t *testing.T) {
		t.Parallel()
		// Create a simple caveat with relationship context
		caveatExpr := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "test_caveat",
					Context:    mustToStruct(t, map[string]any{"resource_id": "doc1", "limit": 5}),
				},
			},
		}

		result := mergeContextsForExpression(caveatExpr, queryContext)

		// Should have query context
		require.Equal("alice", result["user_id"])
		require.Equal(123, result["query_data"])

		// Should have relationship context from the caveat
		require.Equal("doc1", result["resource_id"])
		// structpb converts ints to float64
		require.Equal(float64(5), result["limit"]) //nolint:testifylint // this value isn't being operated on
	})

	t.Run("Complex AND expression", func(t *testing.T) {
		t.Parallel()
		// Create AND expression with multiple caveats
		child1 := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat1",
					Context:    mustToStruct(t, map[string]any{"param1": "value1"}),
				},
			},
		}
		child2 := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat2",
					Context:    mustToStruct(t, map[string]any{"param2": "value2"}),
				},
			},
		}
		andExpr := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Operation{
				Operation: &core.CaveatOperation{
					Op:       core.CaveatOperation_AND,
					Children: []*core.CaveatExpression{child1, child2},
				},
			},
		}

		result := mergeContextsForExpression(andExpr, queryContext)

		// Should have query context (takes precedence)
		require.Equal("alice", result["user_id"])
		require.Equal(123, result["query_data"])

		// Should have relationship contexts from both child caveats
		require.Equal("value1", result["param1"])
		require.Equal("value2", result["param2"])
	})

	t.Run("Query context overrides relationship context", func(t *testing.T) {
		t.Parallel()
		// Test that query context takes precedence over relationship context
		caveatExpr := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "test_caveat",
					Context:    mustToStruct(t, map[string]any{"user_id": "bob", "limit": 10}),
				},
			},
		}

		result := mergeContextsForExpression(caveatExpr, queryContext)

		// Query context should override relationship context
		require.Equal("alice", result["user_id"]) // From query, not "bob" from relationship
		require.Equal(123, result["query_data"])
		// From relationship context
		require.Equal(float64(10), result["limit"]) //nolint:testifylint // this value isn't being operated on
	})

	t.Run("Empty expression", func(t *testing.T) {
		t.Parallel()
		result := mergeContextsForExpression(nil, queryContext)

		// Should only have query context
		require.Equal("alice", result["user_id"])
		require.Equal(123, result["query_data"])
		require.Len(result, 2)
	})
}

func TestCollectRelationshipContexts(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	t.Run("Nil expression", func(t *testing.T) {
		t.Parallel()
		contextMap := make(map[string]any)
		collectRelationshipContexts(nil, contextMap)
		require.Empty(contextMap)
	})

	t.Run("Simple caveat", func(t *testing.T) {
		t.Parallel()
		caveatExpr := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "test_caveat",
					Context:    mustToStruct(t, map[string]any{"resource_id": "doc1", "limit": 5}),
				},
			},
		}

		contextMap := make(map[string]any)
		collectRelationshipContexts(caveatExpr, contextMap)

		require.Equal("doc1", contextMap["resource_id"])
		require.Equal(float64(5), contextMap["limit"]) // nolint:testifylint // these values aren't being operated on
		require.Len(contextMap, 2)
	})

	t.Run("Caveat without context", func(t *testing.T) {
		t.Parallel()
		caveatExpr := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "test_caveat",
					// No Context field
				},
			},
		}

		contextMap := make(map[string]any)
		collectRelationshipContexts(caveatExpr, contextMap)

		require.Empty(contextMap)
	})

	t.Run("AND operation with multiple caveats", func(t *testing.T) {
		t.Parallel()
		child1 := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat1",
					Context:    mustToStruct(t, map[string]any{"param1": "value1", "shared": "from_child1"}),
				},
			},
		}
		child2 := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat2",
					Context:    mustToStruct(t, map[string]any{"param2": "value2", "shared": "from_child2"}),
				},
			},
		}
		andExpr := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Operation{
				Operation: &core.CaveatOperation{
					Op:       core.CaveatOperation_AND,
					Children: []*core.CaveatExpression{child1, child2},
				},
			},
		}

		contextMap := make(map[string]any)
		collectRelationshipContexts(andExpr, contextMap)

		require.Equal("value1", contextMap["param1"])
		require.Equal("value2", contextMap["param2"])
		// First occurrence wins for duplicate keys
		require.Equal("from_child1", contextMap["shared"])
	})

	t.Run("Nested operations", func(t *testing.T) {
		t.Parallel()
		// Create OR( AND(caveat1, caveat2), caveat3 )
		innerChild1 := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat1",
					Context:    mustToStruct(t, map[string]any{"param1": "value1"}),
				},
			},
		}
		innerChild2 := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat2",
					Context:    mustToStruct(t, map[string]any{"param2": "value2"}),
				},
			},
		}
		andExpr := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Operation{
				Operation: &core.CaveatOperation{
					Op:       core.CaveatOperation_AND,
					Children: []*core.CaveatExpression{innerChild1, innerChild2},
				},
			},
		}
		outerChild := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "caveat3",
					Context:    mustToStruct(t, map[string]any{"param3": "value3"}),
				},
			},
		}
		orExpr := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Operation{
				Operation: &core.CaveatOperation{
					Op:       core.CaveatOperation_OR,
					Children: []*core.CaveatExpression{andExpr, outerChild},
				},
			},
		}

		contextMap := make(map[string]any)
		collectRelationshipContexts(orExpr, contextMap)

		// Should collect contexts from all leaf caveats
		require.Equal("value1", contextMap["param1"])
		require.Equal("value2", contextMap["param2"])
		require.Equal("value3", contextMap["param3"])
		require.Len(contextMap, 3)
	})

	t.Run("Operation without children", func(t *testing.T) {
		t.Parallel()
		emptyOp := &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Operation{
				Operation: &core.CaveatOperation{
					Op:       core.CaveatOperation_AND,
					Children: nil,
				},
			},
		}

		contextMap := make(map[string]any)
		collectRelationshipContexts(emptyOp, contextMap)

		require.Empty(contextMap)
	})
}

func TestSimplifyWithEmptyContext(t *testing.T) {
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

	serialized1, _ := caveat1.Serialize()
	serialized2, _ := caveat2.Serialize()
	serialized3, _ := caveat3.Serialize()

	caveatDefs := []*core.CaveatDefinition{
		{Name: "caveat1", SerializedExpression: serialized1, ParameterTypes: env.EncodedParametersTypes()},
		{Name: "caveat2", SerializedExpression: serialized2, ParameterTypes: env.EncodedParametersTypes()},
		{Name: "caveat3", SerializedExpression: serialized3, ParameterTypes: env.EncodedParametersTypes()},
	}

	// Create datastore with caveats
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.LegacyWriteCaveats(ctx, caveatDefs)
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
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

	// Empty context - all caveats should be partial
	emptyContext := map[string]any{}
	simplified, passes, err := SimplifyCaveatExpression(ctx, runner, nestedExpr, emptyContext, sr)
	require.NoError(err)
	require.NotNil(simplified, "Should return the expression (partial)")
	require.True(passes, "Should pass conditionally with empty context")

	// Should return a complex expression since all children are partial
	require.NotNil(simplified.GetOperation())
}

func TestSimplifyNotConditional(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	require := require.New(t)

	// Create test caveat: count < limit
	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"count": caveattypes.Default.UIntType,
		"limit": caveattypes.Default.UIntType,
	})
	require.NoError(err)

	limitCaveat, err := caveats.CompileCaveatWithName(env, "count < limit", "limit_check")
	require.NoError(err)

	serialized, err := limitCaveat.Serialize()
	require.NoError(err)

	caveatDef := &core.CaveatDefinition{
		Name:                 "limit_check",
		SerializedExpression: serialized,
		ParameterTypes:       env.EncodedParametersTypes(),
	}

	// Create datastore with caveat
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{caveatDef})
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
	runner := internalcaveats.NewCaveatRunner(caveattypes.Default.TypeSet)

	// Create NOT expression: NOT limit_check(limit=10)
	// Base case: when child is conditional (partial), NOT conditional should remain conditional
	notExpr := internalcaveats.Invert(
		&core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{
					CaveatName: "limit_check",
					Context:    mustToStruct(t, map[string]any{"limit": uint64(10)}),
				},
			},
		},
	)

	// Test with partial/missing context - child caveat is conditional, so NOT should also be conditional
	// We provide limit but not count, making the child caveat partial
	partialContext := map[string]any{} // No count provided, only limit is in relationship context

	simplified, passes, err := SimplifyCaveatExpression(ctx, runner, notExpr, partialContext, sr)
	require.NoError(err)
	require.True(passes, "NOT of conditional caveat should pass conditionally")
	require.NotNil(simplified, "NOT of conditional caveat should remain as NOT expression")

	// Verify the structure: should still be a NOT operation with the child caveat
	require.NotNil(simplified.GetOperation(), "Result should be an operation")
	require.Equal(core.CaveatOperation_NOT, simplified.GetOperation().Op, "Should be a NOT operation")
	require.Len(simplified.GetOperation().Children, 1, "NOT should have one child")
	require.NotNil(simplified.GetOperation().Children[0].GetCaveat(), "Child should be the original caveat")
	require.Equal("limit_check", simplified.GetOperation().Children[0].GetCaveat().CaveatName)
}

func TestSimplifyDeeplyNestedCaveats(t *testing.T) {
	ctx := context.Background()
	require := require.New(t)

	// Create multiple test caveats for deep nesting
	env, err := caveats.EnvForVariablesWithDefaultTypeSet(map[string]caveattypes.VariableType{
		"a": caveattypes.Default.IntType,
		"b": caveattypes.Default.IntType,
		"c": caveattypes.Default.IntType,
		"d": caveattypes.Default.IntType,
		"e": caveattypes.Default.IntType,
		"f": caveattypes.Default.IntType,
	})
	require.NoError(err)

	caveatA, err := caveats.CompileCaveatWithName(env, "a >= 10", "caveat_a")
	require.NoError(err)
	caveatB, err := caveats.CompileCaveatWithName(env, "b >= 20", "caveat_b")
	require.NoError(err)
	caveatC, err := caveats.CompileCaveatWithName(env, "c >= 30", "caveat_c")
	require.NoError(err)
	caveatD, err := caveats.CompileCaveatWithName(env, "d >= 40", "caveat_d")
	require.NoError(err)
	caveatE, err := caveats.CompileCaveatWithName(env, "e >= 50", "caveat_e")
	require.NoError(err)
	caveatF, err := caveats.CompileCaveatWithName(env, "f >= 60", "caveat_f")
	require.NoError(err)

	serializedA, _ := caveatA.Serialize()
	serializedB, _ := caveatB.Serialize()
	serializedC, _ := caveatC.Serialize()
	serializedD, _ := caveatD.Serialize()
	serializedE, _ := caveatE.Serialize()
	serializedF, _ := caveatF.Serialize()

	caveatDefs := []*core.CaveatDefinition{
		{Name: "caveat_a", SerializedExpression: serializedA, ParameterTypes: env.EncodedParametersTypes()},
		{Name: "caveat_b", SerializedExpression: serializedB, ParameterTypes: env.EncodedParametersTypes()},
		{Name: "caveat_c", SerializedExpression: serializedC, ParameterTypes: env.EncodedParametersTypes()},
		{Name: "caveat_d", SerializedExpression: serializedD, ParameterTypes: env.EncodedParametersTypes()},
		{Name: "caveat_e", SerializedExpression: serializedE, ParameterTypes: env.EncodedParametersTypes()},
		{Name: "caveat_f", SerializedExpression: serializedF, ParameterTypes: env.EncodedParametersTypes()},
	}

	// Create datastore with caveats
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.LegacyWriteCaveats(ctx, caveatDefs)
	})
	require.NoError(err)

	dl := datalayer.NewDataLayer(ds)
	sr, srErr := dl.SnapshotReader(revision).ReadSchema()
	require.NoError(srErr)
	runner := internalcaveats.NewCaveatRunner(caveattypes.Default.TypeSet)

	// Helper to create caveat expressions
	makeCaveat := func(name string) *core.CaveatExpression {
		return &core.CaveatExpression{
			OperationOrCaveat: &core.CaveatExpression_Caveat{
				Caveat: &core.ContextualizedCaveat{CaveatName: name},
			},
		}
	}

	t.Run("ThreeLevels_AND_OR_AND", func(t *testing.T) {
		// Create: ((caveat_a AND caveat_b) OR (caveat_c AND caveat_d)) AND caveat_e
		// Level 1: caveat_a AND caveat_b
		level1Left := internalcaveats.And(makeCaveat("caveat_a"), makeCaveat("caveat_b"))
		// Level 1: caveat_c AND caveat_d
		level1Right := internalcaveats.And(makeCaveat("caveat_c"), makeCaveat("caveat_d"))
		// Level 2: (level1Left OR level1Right)
		level2 := internalcaveats.Or(level1Left, level1Right)
		// Level 3: level2 AND caveat_e
		deepExpr := internalcaveats.And(level2, makeCaveat("caveat_e"))

		// Test 1: All values satisfy all caveats
		context1 := map[string]any{"a": int64(15), "b": int64(25), "c": int64(35), "d": int64(45), "e": int64(55)}
		simplified1, passes1, err1 := SimplifyCaveatExpression(ctx, runner, deepExpr, context1, sr)
		require.NoError(err1)
		require.True(passes1, "All caveats satisfied should pass")
		require.Nil(simplified1, "Should simplify to unconditionally true")

		// Test 2: Only left branch of OR satisfies, and E satisfies
		// (a=15>=10 AND b=25>=20) OR (c=25<30 AND d=35<40) = true OR false = true, then true AND e=55>=50 = true
		context2 := map[string]any{"a": int64(15), "b": int64(25), "c": int64(25), "d": int64(35), "e": int64(55)}
		simplified2, passes2, err2 := SimplifyCaveatExpression(ctx, runner, deepExpr, context2, sr)
		require.NoError(err2)
		require.True(passes2, "Left branch of OR satisfies, should pass")
		require.Nil(simplified2, "Should simplify to unconditionally true")

		// Test 3: OR branch passes but E fails
		// (a=15>=10 AND b=25>=20) = true, then true AND e=45<50 = false
		context3 := map[string]any{"a": int64(15), "b": int64(25), "c": int64(25), "d": int64(35), "e": int64(45)}
		simplified3, passes3, err3 := SimplifyCaveatExpression(ctx, runner, deepExpr, context3, sr)
		require.NoError(err3)
		require.False(passes3, "E fails, entire expression should fail")
		require.NotNil(simplified3, "Should return failed caveat")

		// Test 4: Neither OR branch satisfies
		// Both AND branches fail, so OR fails, making entire expression fail
		context4 := map[string]any{"a": int64(5), "b": int64(15), "c": int64(25), "d": int64(35), "e": int64(55)}
		simplified4, passes4, err4 := SimplifyCaveatExpression(ctx, runner, deepExpr, context4, sr)
		require.NoError(err4)
		require.False(passes4, "OR branch fails, entire expression should fail")
		require.NotNil(simplified4, "Should return failed expression")
	})

	t.Run("FourLevels_OR_AND_OR_AND", func(t *testing.T) {
		// Create: (((caveat_a OR caveat_b) AND caveat_c) OR ((caveat_d OR caveat_e) AND caveat_f))
		// Level 1: caveat_a OR caveat_b
		level1Left := internalcaveats.Or(makeCaveat("caveat_a"), makeCaveat("caveat_b"))
		// Level 2: (level1Left AND caveat_c)
		level2Left := internalcaveats.And(level1Left, makeCaveat("caveat_c"))

		// Level 1: caveat_d OR caveat_e
		level1Right := internalcaveats.Or(makeCaveat("caveat_d"), makeCaveat("caveat_e"))
		// Level 2: (level1Right AND caveat_f)
		level2Right := internalcaveats.And(level1Right, makeCaveat("caveat_f"))

		// Level 3: level2Left OR level2Right
		deepExpr := internalcaveats.Or(level2Left, level2Right)

		// Test 1: Left branch fully satisfies (a passes, c passes)
		context1 := map[string]any{"a": int64(15), "b": int64(5), "c": int64(35), "d": int64(5), "e": int64(5), "f": int64(5)}
		simplified1, passes1, err1 := SimplifyCaveatExpression(ctx, runner, deepExpr, context1, sr)
		require.NoError(err1)
		require.True(passes1, "Left branch satisfies, should pass")
		require.Nil(simplified1, "Should simplify to unconditionally true")

		// Test 2: Right branch satisfies (e passes, f passes)
		context2 := map[string]any{"a": int64(5), "b": int64(5), "c": int64(35), "d": int64(5), "e": int64(55), "f": int64(65)}
		simplified2, passes2, err2 := SimplifyCaveatExpression(ctx, runner, deepExpr, context2, sr)
		require.NoError(err2)
		require.True(passes2, "Right branch satisfies, should pass")
		require.Nil(simplified2, "Should simplify to unconditionally true")

		// Test 3: Both branches fail
		context3 := map[string]any{"a": int64(5), "b": int64(5), "c": int64(35), "d": int64(5), "e": int64(5), "f": int64(65)}
		simplified3, passes3, err3 := SimplifyCaveatExpression(ctx, runner, deepExpr, context3, sr)
		require.NoError(err3)
		require.False(passes3, "Both branches fail, should fail")
		require.NotNil(simplified3, "Should return failed expression")
	})

	t.Run("FiveLevels_Mixed_Operations", func(t *testing.T) {
		// Create: ((caveat_a OR caveat_b) AND (caveat_c OR caveat_d)) OR (caveat_e AND caveat_f)
		// This creates 5 levels of alternating AND/OR operations
		// Level 1: caveat_a OR caveat_b
		orAB := internalcaveats.Or(makeCaveat("caveat_a"), makeCaveat("caveat_b"))
		// Level 1: caveat_c OR caveat_d
		orCD := internalcaveats.Or(makeCaveat("caveat_c"), makeCaveat("caveat_d"))
		// Level 2: (caveat_a OR caveat_b) AND (caveat_c OR caveat_d)
		andLeft := internalcaveats.And(orAB, orCD)
		// Level 2: caveat_e AND caveat_f
		andRight := internalcaveats.And(makeCaveat("caveat_e"), makeCaveat("caveat_f"))
		// Level 3: ((caveat_a OR caveat_b) AND (caveat_c OR caveat_d)) OR (caveat_e AND caveat_f)
		deepExpr := internalcaveats.Or(andLeft, andRight)

		// Test 1: All pass
		context1 := map[string]any{"a": int64(15), "b": int64(25), "c": int64(35), "d": int64(45), "e": int64(55), "f": int64(65)}
		simplified1, passes1, err1 := SimplifyCaveatExpression(ctx, runner, deepExpr, context1, sr)
		require.NoError(err1)
		require.True(passes1, "All pass, should pass")
		require.Nil(simplified1, "Should simplify to unconditionally true")

		// Test 2: Left branch passes (a passes, c passes)
		context2 := map[string]any{"a": int64(15), "b": int64(5), "c": int64(35), "d": int64(5), "e": int64(5), "f": int64(5)}
		simplified2, passes2, err2 := SimplifyCaveatExpression(ctx, runner, deepExpr, context2, sr)
		require.NoError(err2)
		require.True(passes2, "Left branch passes, should pass")
		require.Nil(simplified2, "Should simplify to unconditionally true")

		// Test 3: Right branch passes (e and f pass)
		context3 := map[string]any{"a": int64(5), "b": int64(5), "c": int64(35), "d": int64(5), "e": int64(55), "f": int64(65)}
		simplified3, passes3, err3 := SimplifyCaveatExpression(ctx, runner, deepExpr, context3, sr)
		require.NoError(err3)
		require.True(passes3, "Right branch passes, should pass")
		require.Nil(simplified3, "Should simplify to unconditionally true")

		// Test 4: Left branch partial fails (a passes but c and d fail)
		context4 := map[string]any{"a": int64(15), "b": int64(5), "c": int64(5), "d": int64(5), "e": int64(5), "f": int64(5)}
		simplified4, passes4, err4 := SimplifyCaveatExpression(ctx, runner, deepExpr, context4, sr)
		require.NoError(err4)
		require.False(passes4, "Both branches fail, should fail")
		require.NotNil(simplified4, "Should return failed expression")

		// Test 5: Right branch partial fails (only e passes, not f)
		context5 := map[string]any{"a": int64(5), "b": int64(5), "c": int64(5), "d": int64(5), "e": int64(55), "f": int64(5)}
		simplified5, passes5, err5 := SimplifyCaveatExpression(ctx, runner, deepExpr, context5, sr)
		require.NoError(err5)
		require.False(passes5, "Both branches fail, should fail")
		require.NotNil(simplified5, "Should return failed expression")
	})
}
