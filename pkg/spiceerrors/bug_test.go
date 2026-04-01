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

func TestMustSafecast(t *testing.T) {
	// Valid conversion returns the correct value.
	result := MustSafecast[uint64](42)
	assert.Equal(t, uint64(42), result)

	// Overflow panics when running under tests (IsInTests() == true).
	assert.Panics(t, func() {
		MustSafecast[uint8](256)
	})
}
