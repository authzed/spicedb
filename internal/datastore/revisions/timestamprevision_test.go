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
