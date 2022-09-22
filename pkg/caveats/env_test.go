package caveats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddVariable(t *testing.T) {
	req := require.New(t)
	env := NewEnvironment()
	err := env.AddVariable("foobar", IntType)
	req.NoError(err)
	err = env.AddVariable("foobar", IntType)
	req.Error(err)
}
