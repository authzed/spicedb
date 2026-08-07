package caveats

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/caveats/types"
)

func TestAddVariable(t *testing.T) {
	req := require.New(t)
	env := NewEnvironmentWithDefaultTypeSet()
	err := env.AddVariable("foobar", types.Default.IntType)
	req.NoError(err)
	err = env.AddVariable("foobar", types.Default.IntType)
	req.Error(err)
}

// TestAddVariableInvalidatesCachedCelEnvironment ensures that a variable added
// after the CEL environment has been built is visible to later compilations.
func TestAddVariableInvalidatesCachedCelEnvironment(t *testing.T) {
	req := require.New(t)
	env := NewEnvironmentWithDefaultTypeSet()

	req.NoError(env.AddVariable("first", types.Default.IntType))
	_, err := CompileCaveatWithName(env, "first == 42", "first_caveat")
	req.NoError(err)

	req.NoError(env.AddVariable("second", types.Default.IntType))
	_, err = CompileCaveatWithName(env, "first == 42 && second == 43", "second_caveat")
	req.NoError(err)
}

// TestCelEnvironmentIsCached ensures the CEL environment is only built once
// for an unchanged environment.
func TestCelEnvironmentIsCached(t *testing.T) {
	req := require.New(t)
	env := NewEnvironmentWithDefaultTypeSet()
	req.NoError(env.AddVariable("first", types.Default.IntType))

	first, err := env.asCelEnvironment()
	req.NoError(err)

	second, err := env.asCelEnvironment()
	req.NoError(err)
	req.Same(first, second)

	req.NoError(env.AddVariable("second", types.Default.IntType))
	third, err := env.asCelEnvironment()
	req.NoError(err)
	req.NotSame(first, third)
}

// TestConcurrentEnvironmentsFromSharedTypeSet ensures that environments derived
// from the same (shared) TypeSet can be built and used concurrently.
func TestConcurrentEnvironmentsFromSharedTypeSet(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			paramName := fmt.Sprintf("param_%d", i)
			env := NewEnvironmentWithDefaultTypeSet()
			assert.NoError(t, env.AddVariable(paramName, types.Default.IntType))
			assert.NoError(t, env.AddVariable("shared", types.Default.StringType))

			compiled, err := CompileCaveatWithName(env, fmt.Sprintf("%s == %d && shared == 'hi'", paramName, i), "somecaveat")
			assert.NoError(t, err)
			assert.NotNil(t, compiled)
		}()
	}
	wg.Wait()
}
