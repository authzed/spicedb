package query

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// FuzzPathOrder tests the ordering properties of PathOrder function
func FuzzPathOrder(f *testing.F) {
	// Add some seed corpus
	f.Add([]byte{0})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255})
	f.Add(bytes.Repeat([]byte{42}, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Skip if data is too small
		if len(data) < 8 {
			t.Skip()
		}

		// Generate 3 random paths
		gen := newOutlineGenerator(data)
		a := gen.randomPath()
		b := gen.randomPath()
		c := gen.randomPath()

		// Test reflexivity: PathOrder(x, x) == 0
		require.Equal(t, 0, PathOrder(a, a), "PathOrder should be reflexive: PathOrder(a, a) == 0")
		require.Equal(t, 0, PathOrder(b, b), "PathOrder should be reflexive: PathOrder(b, b) == 0")
		require.Equal(t, 0, PathOrder(c, c), "PathOrder should be reflexive: PathOrder(c, c) == 0")

		// Test antisymmetry: if PathOrder(x, y) < 0, then PathOrder(y, x) > 0
		ab := PathOrder(a, b)
		ba := PathOrder(b, a)

		switch {
		case ab < 0:
			require.Positive(t, ba, "PathOrder should be antisymmetric: if PathOrder(a, b) < 0, then PathOrder(b, a) > 0")
		case ab > 0:
			require.Negative(t, ba, "PathOrder should be antisymmetric: if PathOrder(a, b) > 0, then PathOrder(b, a) < 0")
		default:
			require.Equal(t, 0, ba, "PathOrder should be antisymmetric: if PathOrder(a, b) == 0, then PathOrder(b, a) == 0")
		}

		// Test transitivity: if PathOrder(a, b) < 0 and PathOrder(b, c) < 0, then PathOrder(a, c) < 0
		bc := PathOrder(b, c)
		ac := PathOrder(a, c)

		switch {
		case ab < 0 && bc < 0:
			// Case 1: a < b and b < c => a < c
			require.Negative(t, ac, "PathOrder transitivity: if a < b and b < c, then a < c")
		case ab > 0 && bc > 0:
			// Case 2: a > b and b > c => a > c
			require.Positive(t, ac, "PathOrder transitivity: if a > b and b > c, then a > c")
		case ab > 0:
			// Otherwise, b is least element.
			paths := []Path{a, b, c}
			slices.SortFunc(paths, PathOrder)
			require.Equal(t, 0, PathOrder(paths[0], b))
		case ab < 0:
			// Or b is the greatest element
			paths := []Path{a, b, c}
			slices.SortFunc(paths, PathOrder)
			require.Equal(t, 0, PathOrder(paths[2], b))
		}

		// Test more complex transitivity chains
		// If a == b and b < c, then a < c
		if ab == 0 && bc < 0 {
			require.Negative(t, ac, "PathOrder transitivity: if a == b and b < c, then a < c")
		}

		// If a == b and b > c, then a > c
		if ab == 0 && bc > 0 {
			require.Positive(t, ac, "PathOrder transitivity: if a == b and b > c, then a > c")
		}

		// If a < b and b == c, then a < c
		if ab < 0 && bc == 0 {
			require.Negative(t, ac, "PathOrder transitivity: if a < b and b == c, then a < c")
		}

		// If a > b and b == c, then a > c
		if ab > 0 && bc == 0 {
			require.Positive(t, ac, "PathOrder transitivity: if a > b and b == c, then a > c")
		}

		// If all are equal
		if ab == 0 && bc == 0 {
			require.Equal(t, 0, ac, "PathOrder transitivity: if a == b and b == c, then a == c")
		}
	})
}
