package slicez

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		pred     func(int) bool
		expected []int
	}{
		{
			name:     "filter even numbers",
			input:    []int{1, 2, 3, 4, 5, 6},
			pred:     func(x int) bool { return x%2 == 0 },
			expected: []int{2, 4, 6},
		},
		{
			name:     "filter numbers greater than 3",
			input:    []int{1, 2, 3, 4, 5},
			pred:     func(x int) bool { return x > 3 },
			expected: []int{4, 5},
		},
		{
			name:     "filter all elements (always true)",
			input:    []int{1, 2, 3},
			pred:     func(x int) bool { return true },
			expected: []int{1, 2, 3},
		},
		{
			name:     "filter no elements (always false)",
			input:    []int{1, 2, 3},
			pred:     func(x int) bool { return false },
			expected: []int{},
		},
		{
			name:     "empty slice",
			input:    []int{},
			pred:     func(x int) bool { return x > 0 },
			expected: []int{},
		},
		{
			name:     "single element matching",
			input:    []int{5},
			pred:     func(x int) bool { return x == 5 },
			expected: []int{5},
		},
		{
			name:     "single element not matching",
			input:    []int{5},
			pred:     func(x int) bool { return x == 3 },
			expected: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, Filter(tt.input, tt.pred))
		})
	}
}

func TestFilterStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		pred     func(string) bool
		expected []string
	}{
		{
			name:     "filter strings starting with 'a'",
			input:    []string{"apple", "banana", "apricot", "cherry"},
			pred:     func(s string) bool { return strings.HasPrefix(s, "a") },
			expected: []string{"apple", "apricot"},
		},
		{
			name:     "filter strings longer than 4 characters",
			input:    []string{"cat", "dog", "elephant", "bird"},
			pred:     func(s string) bool { return len(s) > 4 },
			expected: []string{"elephant"},
		},
		{
			name:     "empty string slice",
			input:    []string{},
			pred:     func(s string) bool { return len(s) > 0 },
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, Filter(tt.input, tt.pred))
		})
	}
}

func TestMap(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		fn       func(int) int
		expected []int
	}{
		{
			name:     "double each number",
			input:    []int{1, 2, 3, 4},
			fn:       func(x int) int { return x * 2 },
			expected: []int{2, 4, 6, 8},
		},
		{
			name:     "add 10 to each number",
			input:    []int{1, 2, 3},
			fn:       func(x int) int { return x + 10 },
			expected: []int{11, 12, 13},
		},
		{
			name:     "square each number",
			input:    []int{2, 3, 4},
			fn:       func(x int) int { return x * x },
			expected: []int{4, 9, 16},
		},
		{
			name:     "identity function",
			input:    []int{1, 2, 3},
			fn:       func(x int) int { return x },
			expected: []int{1, 2, 3},
		},
		{
			name:     "empty slice",
			input:    []int{},
			fn:       func(x int) int { return x * 2 },
			expected: []int{},
		},
		{
			name:     "single element",
			input:    []int{5},
			fn:       func(x int) int { return x * 3 },
			expected: []int{15},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, Map(tt.input, tt.fn))
		})
	}
}

func TestMapDifferentTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		fn       func(int) string
		expected []string
	}{
		{
			name:     "convert int to string",
			input:    []int{1, 2, 3},
			fn:       strconv.Itoa,
			expected: []string{"1", "2", "3"},
		},
		{
			name:     "convert int to formatted string",
			input:    []int{1, 2, 3},
			fn:       func(x int) string { return "num:" + strconv.Itoa(x) },
			expected: []string{"num:1", "num:2", "num:3"},
		},
		{
			name:     "empty slice different types",
			input:    []int{},
			fn:       strconv.Itoa,
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, Map(tt.input, tt.fn))
		})
	}
}

func TestUnique(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		expected []int
	}{
		{
			name:     "remove duplicates",
			input:    []int{1, 2, 2, 3, 3, 3, 4},
			expected: []int{1, 2, 3, 4},
		},
		{
			name:     "no duplicates",
			input:    []int{1, 2, 3, 4},
			expected: []int{1, 2, 3, 4},
		},
		{
			name:     "all duplicates",
			input:    []int{5, 5, 5, 5},
			expected: []int{5},
		},
		{
			name:     "empty slice",
			input:    []int{},
			expected: []int{},
		},
		{
			name:     "single element",
			input:    []int{42},
			expected: []int{42},
		},
		{
			name:     "duplicates at beginning",
			input:    []int{1, 1, 2, 3, 4},
			expected: []int{1, 2, 3, 4},
		},
		{
			name:     "duplicates at end",
			input:    []int{1, 2, 3, 4, 4},
			expected: []int{1, 2, 3, 4},
		},
		{
			name:     "duplicates scattered",
			input:    []int{1, 3, 2, 3, 1, 4, 2},
			expected: []int{1, 3, 2, 4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, Unique(tt.input))
		})
	}
}

func TestUniqueStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "remove duplicate strings",
			input:    []string{"apple", "banana", "apple", "cherry", "banana"},
			expected: []string{"apple", "banana", "cherry"},
		},
		{
			name:     "no duplicate strings",
			input:    []string{"apple", "banana", "cherry"},
			expected: []string{"apple", "banana", "cherry"},
		},
		{
			name:     "all same strings",
			input:    []string{"hello", "hello", "hello"},
			expected: []string{"hello"},
		},
		{
			name:     "empty string slice",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "single string",
			input:    []string{"unique"},
			expected: []string{"unique"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, Unique(tt.input))
		})
	}
}
