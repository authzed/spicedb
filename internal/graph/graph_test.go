package graph

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestChunkSizes(t *testing.T) {
	t.Parallel()
	for index, cs := range progressiveDispatchChunkSizes {
		require.LessOrEqual(t, cs, datastore.FilterMaximumIDCount)
		if index > 0 {
			require.Greater(t, cs, progressiveDispatchChunkSizes[index-1])
		}
	}
}

func TestMaxDispatchChunkSize(t *testing.T) {
	t.Parallel()
	require.LessOrEqual(t, maxDispatchChunkSize, datastore.FilterMaximumIDCount)
}
