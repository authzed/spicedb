package memdb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHeadRevision(t *testing.T) {
	ds, err := NewMemdbDatastore(0, 0, 500*time.Millisecond)
	require.NoError(t, err)

	older, err := ds.HeadRevision(context.Background())
	require.NoError(t, err)
	err = ds.CheckRevision(context.Background(), older)
	require.NoError(t, err)

	time.Sleep(550 * time.Millisecond)

	// GC window elapsed, last revision is returned even if outside GC window
	newer, err := ds.HeadRevision(context.Background())
	require.NoError(t, err)
	err = ds.CheckRevision(context.Background(), newer)
	require.NoError(t, err)
}

func (mdb *memdbDatastore) ExampleRetryableError() error {
	return errSerialization
}
