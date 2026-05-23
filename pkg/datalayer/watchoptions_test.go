package datalayer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestBuildAndValidateWatchOptions(t *testing.T) {
	t.Run("WatchBufferWriteTimeout", func(t *testing.T) {
		c, err := buildAndValidateWatchOptions(datastore.ServerWatchOptions{WatchBufferWriteTimeout: 1 * time.Second}, datastore.ClientWatchOptions{}, datastore.WatchOptions{WatchBufferWriteTimeout: 2 * time.Second})
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, 2*time.Second, c.WatchBufferWriteTimeout)
	})
	t.Run("WatchConnectTimeout", func(t *testing.T) {
		c, err := buildAndValidateWatchOptions(datastore.ServerWatchOptions{WatchConnectTimeout: 1 * time.Second}, datastore.ClientWatchOptions{}, datastore.WatchOptions{WatchConnectTimeout: 2 * time.Second})
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, 2*time.Second, c.WatchConnectTimeout)
	})
	t.Run("WatchBufferLength", func(t *testing.T) {
		c, err := buildAndValidateWatchOptions(datastore.ServerWatchOptions{WatchBufferLength: 100}, datastore.ClientWatchOptions{}, datastore.WatchOptions{WatchBufferLength: 200})
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, uint16(200), c.WatchBufferLength)
	})
	t.Run("CheckpointInterval", func(t *testing.T) {
		c, err := buildAndValidateWatchOptions(datastore.ServerWatchOptions{CheckpointInterval: 1}, datastore.ClientWatchOptions{}, datastore.WatchOptions{CheckpointInterval: 2})
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, time.Duration(2), c.CheckpointInterval)
	})
	t.Run("Invalid CheckpointInterval", func(t *testing.T) {
		_, err := buildAndValidateWatchOptions(datastore.ServerWatchOptions{CheckpointInterval: -1}, datastore.ClientWatchOptions{}, datastore.WatchOptions{CheckpointInterval: -1})
		require.Error(t, err)
	})
}
