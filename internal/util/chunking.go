package util

// ForEachChunk executes the given handler for each chunk of items in the slice.
func ForEachChunk[T any](data []T, chunkSize uint64, handler func(items []T)) {
	dataLength := uint64(len(data))
	chunkCount := (dataLength / chunkSize) + 1
	for chunkIndex := uint64(0); chunkIndex < chunkCount; chunkIndex++ {
		chunkStart := chunkIndex * chunkSize
		chunkEnd := (chunkIndex + 1) * chunkSize
		if chunkEnd > dataLength {
			chunkEnd = dataLength
		}

		handler(data[chunkStart:chunkEnd])
	}
}
