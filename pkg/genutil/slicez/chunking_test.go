package slicez

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForEachChunk(t *testing.T) {
	t.Parallel()

	for _, datasize := range []int{0, 1, 5, 10, 50, 100, 250} {
		datasize := datasize
		for _, chunksize := range []uint16{1, 2, 3, 5, 10, 50} {
			chunksize := chunksize
			t.Run(fmt.Sprintf("test-%d-%d", datasize, chunksize), func(t *testing.T) {
				t.Parallel()

				data := make([]int, 0, datasize)
				for i := 0; i < datasize; i++ {
					data = append(data, i)
				}

				found := make([]int, 0, datasize)
				ForEachChunk(data, chunksize, func(items []int) {
					found = append(found, items...)
					require.True(t, len(items) <= int(chunksize))
					require.True(t, len(items) > 0)
				})
				require.Equal(t, data, found)
			})
		}
	}
}

func TestForEachChunkOverflowPanic(t *testing.T) {
	t.Parallel()

	datasize := math.MaxUint16
	chunksize := uint16(50)
	data := make([]int, 0, datasize)
	for i := 0; i < datasize; i++ {
		data = append(data, i)
	}

	found := make([]int, 0, datasize)
	ForEachChunk(data, chunksize, func(items []int) {
		found = append(found, items...)
		require.True(t, len(items) <= int(chunksize))
		require.True(t, len(items) > 0)
	})

	require.Equal(t, data, found)
}

func TestForEachChunkOverflowIncorrect(t *testing.T) {
	t.Parallel()

	chunksize := uint16(50)
	for _, datasize := range []int{math.MaxUint16 + int(chunksize), 10_000_000} {
		datasize := datasize
		t.Run(fmt.Sprintf("test-%d-%d", datasize, chunksize), func(t *testing.T) {
			t.Parallel()

			data := make([]int, 0, datasize)
			for i := 0; i < datasize; i++ {
				data = append(data, i)
			}

			found := make([]int, 0, datasize)
			ForEachChunk(data, chunksize, func(items []int) {
				found = append(found, items...)
				require.True(t, len(items) <= int(chunksize))
				require.True(t, len(items) > 0)
			})

			require.Equal(t, data, found)
		})
	}
}
