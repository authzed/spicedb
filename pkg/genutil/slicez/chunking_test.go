package slicez

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForEachChunk(t *testing.T) {
	for _, datasize := range []int{0, 1, 5, 10, 50, 100, 250} {
		datasize := datasize
		for _, chunksize := range []uint16{1, 2, 3, 5, 10, 50} {
			chunksize := chunksize
			t.Run(fmt.Sprintf("test-%d-%d", datasize, chunksize), func(t *testing.T) {
				data := []int{}
				for i := 0; i < datasize; i++ {
					data = append(data, i)
				}

				found := []int{}
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
