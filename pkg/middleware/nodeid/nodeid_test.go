package nodeid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	nodeID := Get()
	require.Contains(t, nodeID, spiceDBPrefix)
}
