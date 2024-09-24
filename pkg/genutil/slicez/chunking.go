package slicez

import (
	"github.com/authzed/spicedb/internal/logging"
)

// ForEachChunk executes the given handler for each chunk of items in the slice.
func ForEachChunk[T any](data []T, chunkSize uint16, handler func(items []T)) {
	_, _ = ForEachChunkUntil(data, chunkSize, func(items []T) (bool, error) {
		handler(items)
		return true, nil
	})
}

func ForEachChunkUntil[T any](data []T, chunkSize uint16, handler func(items []T) (bool, error)) (bool, error) {
	if chunkSize == 0 {
		logging.Warn().Int("invalid-chunk-size", int(chunkSize)).Msg("ForEachChunk got an invalid chunk size; defaulting to 100")
		chunkSize = 100
	}

	dataLength := uint64(len(data))
	chunkSize64 := uint64(chunkSize)
	chunkCount := (dataLength / chunkSize64) + 1
	for chunkIndex := uint64(0); chunkIndex < chunkCount; chunkIndex++ {
		chunkStart := chunkIndex * chunkSize64
		chunkEnd := (chunkIndex + 1) * chunkSize64
		if chunkEnd > dataLength {
			chunkEnd = dataLength
		}

		chunk := data[chunkStart:chunkEnd]
		if len(chunk) > 0 {
			ok, err := handler(chunk)
			if err != nil {
				return false, err
			}
			if !ok {
				return ok, nil
			}
		}
	}

	return true, nil
}
