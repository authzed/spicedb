// Copyright 2019 Jimmy Zelinskie
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package stringz implements a collection of utility functions for
// manipulating strings and lists of strings.
package stringz

import (
	"errors"
	"strings"
)

// ErrInconsistentUnpackLen is returned when Unpack is provided two slices
// without the same length.
var ErrInconsistentUnpackLen = errors.New("the length of the unpacked is not equal to the provided input")

// SliceContains returns true if the provided string is in the provided string
// slice.
func SliceContains(ys []string, x string) bool {
	for _, y := range ys {
		if x == y {
			return true
		}
	}
	return false
}

// SliceIndex returns the index of the first instance of x in ys, or -1 if it
// is not present.
func SliceIndex(ys []string, x string) int {
	for i, y := range ys {
		if x == y {
			return i
		}
	}
	return -1
}

// Dedup returns a new slice with any duplicates removed.
func Dedup(xs []string) []string {
	set := make(map[string]struct{}, 0)
	ys := make([]string, 0, len(xs))
	for _, x := range xs {
		if _, alreadyExists := set[x]; alreadyExists {
			continue
		}
		ys = append(ys, x)
		set[x] = struct{}{}
	}

	return ys
}

// DefaultEmpty returns the fallback when val is empty string.
//
// This function is inspired by Python's `dict.get()`.
func DefaultEmpty(val, fallback string) string {
	return Default(val, fallback, "")
}

// Default returns a fallback value when the provided value is equal to any
// of the zero values.
func Default(val, fallback string, zeroValues ...string) string {
	for _, zeroValue := range zeroValues {
		if val == zeroValue {
			return fallback
		}
	}

	return val
}

// SliceEqual returns true if two string slices are the same.
// This function is sensitive to order.
func SliceEqual(xs, ys []string) bool {
	if len(xs) != len(ys) {
		return false
	}

	for i, x := range xs {
		if x != ys[i] {
			return false
		}
	}

	return true
}

// MatrixEqual returns true if two [][]string are equal.
// This function is sensitive to order.
func MatrixEqual(xs, ys [][]string) bool {
	if len(xs) != len(ys) {
		return false
	}

	for i, x := range xs {
		if !SliceEqual(x, ys[i]) {
			return false
		}
	}

	return true
}

// TrimPrefixIndex trims everything before the provided index.
func TrimPrefixIndex(s, index string) string {
	i := strings.Index(s, index)
	if i <= 0 {
		return s
	}
	return s[i+len(index):]
}

// TrimSurrounding returns a string with both a prefix and suffix trimmed from
// it.
//
// Do not confuse this with strings.Trim() which removes characters in a cutset
// rather than working on prefixes and suffixes.
func TrimSurrounding(s, surrounding string) string {
	s = strings.TrimPrefix(s, surrounding)
	return strings.TrimSuffix(s, surrounding)
}

// SliceMap is a functional-style mapping function for slices of strings.
//
// This is particularly useful when you would normally use a for-loop, but want
// `defer` to execute for each iteration.
func SliceMap(xs []string, fn func(string) error) error {
	for _, x := range xs {
		err := fn(x)
		if err != nil {
			return err
		}
	}
	return nil
}

// Join is strings.Join, but variadic.
func Join(prefix string, xs ...string) string { return strings.Join(xs, prefix) }

// CopyStringMap returns a new copy of a map of strings.
func CopyStringMap(xs map[string]string) map[string]string {
	// Zero allocation path.
	if xs == nil {
		return nil
	}

	ys := make(map[string]string, len(xs))
	for k, v := range xs {
		ys[k] = v
	}
	return ys
}

// Unpack assigns a slice into local variables.
func Unpack(xs []string, vars ...*string) error {
	if len(xs) != len(vars) {
		return ErrInconsistentUnpackLen
	}
	for i, x := range xs {
		*vars[i] = x
	}
	return nil
}

// SplitExact splits the string `s` into `len(vars)` number of strings and
// unpacks them into those vars.
func SplitExact(s, sep string, vars ...*string) error {
	exploded := strings.Split(s, sep)
	return Unpack(exploded, vars...)
}

// SlicePermutations returns all permutations of a string slice.
//
// It is equivalent to `SliceCombinationsR(xs, len(xs))`.
func SlicePermutations(xs []string) [][]string {
	return SlicePermutationsR(xs, len(xs))
}

// SlicePermutationsR returns successive r-length permutations of elements in
// the provided string slice.
//
// If r is less than 0 or larger than the length of the pool, nil is returned.
//
// The permutation tuples are emitted in lexicographic ordering according to
// the order of the input iterable. So, if the input iterable is sorted, the
// combination tuples will be produced in sorted order.
//
// Elements are treated as unique based on their position, not on their value.
// So if the input elements are unique, there will be no repeat values in each
// permutation.
//
// This is the algorithm used in Python's itertools library:
// itertools.permutations(iterable, r=None)
func SlicePermutationsR(pool []string, r int) [][]string {
	if r <= 0 || pool == nil || r > len(pool) {
		return nil
	}
	n := len(pool)

	indices := make([]int, n)
	for i := range pool {
		indices[i] = i
	}

	var ys [][]string

	{
		var y []string
		for _, i := range indices[:r] {
			y = append(y, pool[i])
		}
		ys = append(ys, y)
	}

	cycles := make([]int, n-(n-r))
	for i := range cycles {
		cycles[i] = n - i
	}

	for {
		broke := false

		for i := r - 1; i >= 0; i-- {
			cycles[i] = cycles[i] - 1
			if cycles[i] == 0 {
				indices = append(indices[:i], append(indices[i+1:], indices[i:i+1]...)...)
				cycles[i] = n - i
			} else {
				j := cycles[i]
				indices[i], indices[len(indices)-j] = indices[len(indices)-j], indices[i]

				var y []string
				for _, i := range indices[:r] {
					y = append(y, pool[i])
				}
				ys = append(ys, y)

				broke = true
				break
			}
		}
		if !broke {
			return ys
		}
	}
}

// SliceCombinationsR returns r-length subsequences of elements from the
// provided string slice.
//
// If r is less than 0 or larger than the length of the pool, nil is returned.
//
// The combinations are emitted in lexicographic ordering according to the
// order of the input iterable. So, if the input iterable is sorted, the
// combination tuples will be produced in sorted order.
//
// Elements are treated as unique based on their position, not on their value.
// So if the input elements are unique, there will be no repeat values in each
// combination.
//
// This is the algorithm used in Python's itertools library:
// itertools.combinations(iterable, r)
func SliceCombinationsR(pool []string, r int) [][]string {
	if r <= 0 || pool == nil || r > len(pool) {
		return nil
	}
	n := len(pool)

	indices := make([]int, r)
	for i := range indices {
		indices[i] = i
	}

	var ys [][]string

	{
		var y []string
		for _, j := range indices {
			y = append(y, pool[j])
		}
		ys = append(ys, y)
	}

	for {
		i := -1
		broke := false
		for i = r - 1; i >= 0; i-- {
			if indices[i] != i+n-r {
				broke = true
				break
			}
		}
		if !broke {
			return ys
		}

		indices[i] = indices[i] + 1
		for j := i + 1; j < r; j++ {
			indices[j] = indices[j-1] + 1
		}

		var y []string
		for _, j := range indices {
			y = append(y, pool[j])
		}
		ys = append(ys, y)
	}
}

// SliceCombinationsWithReplacement returns r-length subsequences of elements
// from the provided string slice allowing individual elements to be repeated
// more than once.
//
// If r is less than 0 or larger than the length of the pool, nil is returned.
//
// The combination tuples are emitted in lexicographic ordering according to
// the order of the input iterable. So, if the input iterable is sorted, the
// combination tuples will be produced in sorted order.
//
// Elements are treated as unique based on their position, not on their value.
// So if the input elements are unique, the generated combinations will also be
// unique.
//
// This is the algorithm used in Python's itertools library:
// itertools.combinations_with_replacement(iterable, r)
func SliceCombinationsWithReplacement(pool []string, r int) [][]string {
	if r <= 0 || pool == nil || r > len(pool) {
		return nil
	}
	n := len(pool)

	indices := make([]int, r)
	var ys [][]string

	{
		var y []string
		for _, j := range indices {
			y = append(y, pool[j])
		}
		ys = append(ys, y)
	}

	for {
		i := -1
		broke := false
		for i = r - 1; i >= 0; i-- {
			if indices[i] != n-1 {
				broke = true
				break
			}
		}
		if !broke {
			return ys
		}

		newIndices := make([]int, r-i)
		for j := range newIndices {
			newIndices[j] = indices[i] + 1
		}
		indices = append(indices[:i], newIndices...)

		var y []string
		for _, j := range indices {
			y = append(y, pool[j])
		}
		ys = append(ys, y)
	}
}
