package slicez

import (
	"slices"

	"github.com/authzed/spicedb/internal/logging"
)

const DefaultChunkSize = 100

// ForEachChunk executes the given handler for each chunk of items in the slice.
func ForEachChunk[T any](data []T, chunkSize uint16, handler func(items []T)) {
	_, _ = ForEachChunkUntil(data, chunkSize, func(items []T) (bool, error) {
		handler(items)
		return true, nil
	})
}

func ForEachChunkUntil[T any](data []T, chunkSize uint16, f func([]T) (bool, error)) (bool, error) {
	if chunkSize == 0 {
		logging.Warn().Int("invalid-chunk-size", int(chunkSize)).
			Int("default-chunk-size", DefaultChunkSize).
			Msg("Received zero chunk size - falling back to default")
		chunkSize = DefaultChunkSize
	}

	for items := range slices.Chunk(data, int(chunkSize)) {
		if ok, err := f(items); err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}
