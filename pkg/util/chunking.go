package util

import (
	"github.com/authzed/spicedb/internal/logging"
)

// ForEachChunk executes the given handler for each chunk of items in the slice.
func ForEachChunk[T any](data []T, chunkSize uint64, handler func(items []T)) {
	if chunkSize == 0 {
		logging.Warn().Int("invalid-chunk-size", int(chunkSize)).Msg("ForEachChunk got an invalid chunk size; defaulting to 1")
		chunkSize = 1
	}

	dataLength := uint64(len(data))
	chunkCount := (dataLength / chunkSize) + 1
	for chunkIndex := uint64(0); chunkIndex < chunkCount; chunkIndex++ {
		chunkStart := chunkIndex * chunkSize
		chunkEnd := (chunkIndex + 1) * chunkSize
		if chunkEnd > dataLength {
			chunkEnd = dataLength
		}

		chunk := data[chunkStart:chunkEnd]
		if len(chunk) > 0 {
			handler(chunk)
		}
	}
}
