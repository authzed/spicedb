package caveats

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
)

func TestCompiledCaveatCacheGetOrCompile(t *testing.T) {
	env := caveats.NewEnvironmentWithTypeSet(caveattypes.Default.TypeSet)
	compiled, err := caveats.CompileCaveatWithName(env, "1 == 1", "test")
	require.NoError(t, err)

	c := &CompiledCaveatCache{}
	calls := 0
	compile := func() (*caveats.CompiledCaveat, error) {
		calls++
		return compiled, nil
	}

	// First access compiles; subsequent accesses return the cached instance without recompiling.
	got1, err := c.GetOrCompile("a", compile)
	require.NoError(t, err)
	require.Same(t, compiled, got1)

	got2, err := c.GetOrCompile("a", compile)
	require.NoError(t, err)
	require.Same(t, compiled, got2)
	require.Equal(t, 1, calls, "compile should be invoked once per name")

	// Distinct names compile independently.
	_, err = c.GetOrCompile("b", compile)
	require.NoError(t, err)
	require.Equal(t, 2, calls)

	// Errors propagate and are not cached.
	boom := errors.New("boom")
	_, err = c.GetOrCompile("c", func() (*caveats.CompiledCaveat, error) {
		return nil, boom
	})
	require.ErrorIs(t, err, boom)
}
