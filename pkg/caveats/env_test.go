package caveats

import (
	"testing"

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
