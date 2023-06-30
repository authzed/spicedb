package slicez

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUniqueSlice(t *testing.T) {
	tcs := []struct {
		input  []int
		output []int
	}{
		{
			[]int{},
			[]int{},
		},
		{
			[]int{1, 2, 3},
			[]int{1, 2, 3},
		},
		{
			[]int{2, 3, 1},
			[]int{2, 3, 1},
		},
		{
			[]int{2, 3, 1, 2},
			[]int{2, 3, 1},
		},
		{
			[]int{2, 3, 1, 2, 1},
			[]int{2, 3, 1},
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%v", tc.input), func(t *testing.T) {
			require.Equal(t, tc.output, Unique(tc.input))
		})
	}
}
