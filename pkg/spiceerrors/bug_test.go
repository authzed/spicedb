package spiceerrors

import (
	"os"
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
	require.True(t, IsInTests())

	// Test successful conversion
	t.Run("successful conversion", func(t *testing.T) {
		result := MustSafecast[uint64](42)
		assert.Equal(t, uint64(42), result)

		result2 := MustSafecast[int32](100)
		assert.Equal(t, int32(100), result2)
	})

	// Test that conversion failure panics in tests
	t.Run("conversion failure panics in tests", func(t *testing.T) {
		assert.Panics(t, func() {
			// Try to convert a negative number to unsigned
			MustSafecast[uint64](-1)
		}, "Expected panic on invalid conversion")
	})

	// Test conversion from larger to smaller type that fits
	t.Run("conversion within range", func(t *testing.T) {
		result := MustSafecast[uint32](uint64(100))
		assert.Equal(t, uint32(100), result)
	})

	// Test production behavior (returns zero value without panic)
	t.Run("production behavior returns zero on failure", func(t *testing.T) {
		// Temporarily simulate production by removing test flags from os.Args
		originalArgs := os.Args
		defer func() {
			os.Args = originalArgs
		}()

		// Remove all -test.* flags to simulate production
		var nonTestArgs []string
		for _, arg := range os.Args {
			if len(arg) < 6 || arg[:6] != "-test." {
				nonTestArgs = append(nonTestArgs, arg)
			}
		}
		os.Args = nonTestArgs

		// Verify we're now simulating production
		require.False(t, IsInTests(), "Should simulate production mode")

		// Test that conversion failure returns zero in production
		result := MustSafecast[uint64](-1)
		assert.Equal(t, uint64(0), result, "Expected zero value in production mode")

		// Test overflow case
		result2 := MustSafecast[uint8](300)
		assert.Equal(t, uint8(0), result2, "Expected zero value for overflow in production mode")
	})
}
