package spiceerrors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMustBug(t *testing.T) {
	require.True(t, IsInTests())
	assert.Panics(t, func() {
		err := MustBugf("some error")
		require.Error(t, err)
	}, "The code did not panic")
}
