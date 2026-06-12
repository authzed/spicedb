package datalayer

import (
	"errors"

	"github.com/authzed/spicedb/pkg/datastore"
)

// mergeWatchOptions constructs complete WatchOptions by merging server options,
// client options, and datastore defaults.
// Datastore defaults take precedence over server options.
// Client options cannot be overridden.
func mergeWatchOptions(
	serverOptions datastore.ServerWatchOptions,
	clientOptions datastore.ClientWatchOptions,
	datastoreDefaults datastore.WatchOptions,
) (datastore.WatchOptions, error) {
	watchChangeBufferMaximumSize, err := watchBufferSize(serverOptions.MaximumBufferedChangesByteSize)
	if err != nil {
		return datastore.WatchOptions{}, err
	}

	merged := datastore.WatchOptions{
		Content:                        clientOptions.Content,
		EmissionStrategy:               clientOptions.EmissionStrategy,
		CheckpointInterval:             serverOptions.CheckpointInterval,
		WatchBufferLength:              serverOptions.WatchBufferLength,
		WatchBufferWriteTimeout:        serverOptions.WatchBufferWriteTimeout,
		WatchConnectTimeout:            serverOptions.WatchConnectTimeout,
		MaximumBufferedChangesByteSize: watchChangeBufferMaximumSize,
	}

	if datastoreDefaults.CheckpointInterval > 0 {
		merged.CheckpointInterval = datastoreDefaults.CheckpointInterval
	}
	if merged.CheckpointInterval < 0 {
		return datastore.WatchOptions{}, errors.New("invalid checkpoint interval given")
	}
	if datastoreDefaults.WatchBufferLength > 0 {
		merged.WatchBufferLength = datastoreDefaults.WatchBufferLength
	}
	if datastoreDefaults.WatchBufferWriteTimeout > 0 {
		merged.WatchBufferWriteTimeout = datastoreDefaults.WatchBufferWriteTimeout
	}
	if datastoreDefaults.WatchConnectTimeout > 0 {
		merged.WatchConnectTimeout = datastoreDefaults.WatchConnectTimeout
	}
	if datastoreDefaults.MaximumBufferedChangesByteSize > 0 {
		merged.MaximumBufferedChangesByteSize = datastoreDefaults.MaximumBufferedChangesByteSize
	}

	return merged, nil
}
