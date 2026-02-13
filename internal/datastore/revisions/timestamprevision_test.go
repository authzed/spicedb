package revisions

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestZeroTimestampRevision(t *testing.T) {
	require.False(t, TimestampRevision(0).LessThan(zeroTimestampRevision))
	require.True(t, TimestampRevision(0).Equal(zeroTimestampRevision))
	require.False(t, TimestampRevision(0).GreaterThan(zeroTimestampRevision))

	require.False(t, TimestampRevision(1).LessThan(zeroTimestampRevision))
	require.False(t, TimestampRevision(1).Equal(zeroTimestampRevision))
	require.True(t, TimestampRevision(1).GreaterThan(zeroTimestampRevision))
}

func TestTimestampRevisionKey(t *testing.T) {
	testCases := []struct {
		name      string
		timestamp int64
	}{
		{
			name:      "zero",
			timestamp: 0,
		},
		{
			name:      "small timestamp",
			timestamp: 1000000000,
		},
		{
			name:      "large timestamp",
			timestamp: 1703283409994227985,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rev := NewForTimestamp(tc.timestamp)

			// Key should be deterministic
			key1 := rev.Key()
			key2 := rev.Key()
			require.Equal(t, key1, key2, "Key() should be deterministic")

			// Key should equal String() for timestamp revisions
			require.Equal(t, rev.String(), key1, "Key() should match String() for timestamp revisions")

			// Equal revisions should have equal keys
			rev2 := NewForTimestamp(tc.timestamp)
			require.Equal(t, rev.Key(), rev2.Key(), "Equal revisions should have equal keys")

			// Different revisions should have different keys
			rev3 := NewForTimestamp(tc.timestamp + 1)
			require.NotEqual(t, rev.Key(), rev3.Key(), "Different revisions should have different keys")
		})
	}
}
