package memdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHeadRevision(t *testing.T) {
	ds, err := NewMemdbDatastore(0, 0, 500*time.Millisecond)
	require.NoError(t, err)

	olderResult, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)
	err = ds.CheckRevision(t.Context(), olderResult.Revision)
	require.NoError(t, err)

	time.Sleep(550 * time.Millisecond)

	// GC window elapsed, last revision is returned even if outside GC window
	newerResult, err := ds.HeadRevision(t.Context())
	require.NoError(t, err)
	err = ds.CheckRevision(t.Context(), newerResult.Revision)
	require.NoError(t, err)
}
