package dispatch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheKeyPrefixOverlap(t *testing.T) {
	encountered := map[string]struct{}{}
	for _, prefix := range cachePrefixes {
		_, ok := encountered[string(prefix)]
		require.False(t, ok)
		encountered[string(prefix)] = struct{}{}
	}
}
