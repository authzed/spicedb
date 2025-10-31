package slicez

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForEachChunk(t *testing.T) {
	t.Parallel()

	for _, dataSize := range []int{0, 1, 5, 10, 50, 100, 250} {
		for _, chunkSize := range []uint16{1, 2, 3, 5, 10, 50} {
			t.Run(fmt.Sprintf("test-%d-%d", dataSize, chunkSize), func(t *testing.T) {
				t.Parallel()

				data := make([]int, dataSize)
				for i := range dataSize {
					data[i] = i
				}

				found := make([]int, 0, dataSize)
				ForEachChunk(data, chunkSize, func(items []int) {
					found = append(found, items...)
					require.LessOrEqual(t, len(items), int(chunkSize))
					require.NotEmpty(t, items)
				})
				require.Equal(t, data, found)
			})
		}
	}
}

func TestForEachChunkUntil(t *testing.T) {
	t.Parallel()

	data := []int{1, 2, 3, 4, 5}
	errAny := errors.New("any error")

	tests := []struct {
		lastIndex int
		err       error
		ok        bool
	}{
		{lastIndex: len(data), ok: true, err: nil},
		{lastIndex: 3, ok: false, err: errAny},
		{lastIndex: 3, ok: false, err: nil},
	}
	for _, tc := range tests {
		index := 0

		ok, err := ForEachChunkUntil(data, 1, func(chunk []int) (bool, error) {
			if index >= tc.lastIndex {
				return tc.ok, tc.err
			}
			index++
			return true, nil
		})
		require.Equal(t, ok, tc.ok)
		require.ErrorIs(t, err, tc.err)
		require.Equal(t, tc.lastIndex, index)
	}
}

func TestForEachChunkZeroChunkSize(t *testing.T) {
	data := []int{1, 2, 3}
	found := make([]int, 0, len(data))
	ForEachChunk(data, 0, func(chunk []int) {
		found = append(found, chunk...)
	})
	require.Equal(t, data, found)
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
		require.LessOrEqual(t, len(items), int(chunksize))
		require.NotEmpty(t, items)
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
				require.LessOrEqual(t, len(items), int(chunksize))
				require.NotEmpty(t, items)
			})

			require.Equal(t, data, found)
		})
	}
}
