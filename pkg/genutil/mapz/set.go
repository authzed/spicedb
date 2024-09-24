package mapz

import (
	"maps"

	"github.com/rs/zerolog"

	expmaps "golang.org/x/exp/maps"
)

// Set implements a very basic generic set.
type Set[T comparable] struct {
	values map[T]struct{}
}

// NewSet returns a new set.
func NewSet[T comparable](items ...T) *Set[T] {
	s := &Set[T]{
		values: map[T]struct{}{},
	}
	for _, item := range items {
		s.values[item] = struct{}{}
	}
	return s
}

// Has returns true if the set contains the given value.
func (s *Set[T]) Has(value T) bool {
	_, exists := s.values[value]
	return exists
}

// Add adds the given value to the set and returns true. If
// the value is already present, returns false.
func (s *Set[T]) Add(value T) bool {
	if s.Has(value) {
		return false
	}

	s.values[value] = struct{}{}
	return true
}

// Insert adds the given value to the set.
func (s *Set[T]) Insert(value T) {
	s.values[value] = struct{}{}
}

// Delete removes the value from the set, returning nothing.
func (s *Set[T]) Delete(value T) {
	delete(s.values, value)
}

// Extend adds all the values to the set.
func (s *Set[T]) Extend(values []T) {
	for _, value := range values {
		s.values[value] = struct{}{}
	}
}

// Merge adds all the values from the other set to this set.
func (s *Set[T]) Merge(other *Set[T]) {
	for value := range other.values {
		s.values[value] = struct{}{}
	}
}

// Union adds all the values from the other set to this set,
// returning a new set.
func (s *Set[T]) Union(other *Set[T]) *Set[T] {
	cpy := s.Copy()
	for value := range other.values {
		cpy.values[value] = struct{}{}
	}
	return cpy
}

// IntersectionDifference removes any values from this set that
// are not shared with the other set. Returns the same set.
func (s *Set[T]) IntersectionDifference(other *Set[T]) *Set[T] {
	for value := range s.values {
		if !other.Has(value) {
			delete(s.values, value)
		}
	}
	return s
}

// RemoveAll removes all values from this set found in the other set.
func (s *Set[T]) RemoveAll(other *Set[T]) {
	for value := range other.values {
		delete(s.values, value)
	}
}

// Subtract subtracts the other set from this set, returning a new set.
func (s *Set[T]) Subtract(other *Set[T]) *Set[T] {
	cpy := s.Copy()
	cpy.RemoveAll(other)
	return cpy
}

// Copy returns a copy of this set.
func (s *Set[T]) Copy() *Set[T] {
	return &Set[T]{
		values: maps.Clone(s.values),
	}
}

// Intersect removes any values from this set that
// are not shared with the other set, returning a new set.
func (s *Set[T]) Intersect(other *Set[T]) *Set[T] {
	cpy := s.Copy()
	for value := range cpy.values {
		if !other.Has(value) {
			delete(cpy.values, value)
		}
	}
	return cpy
}

// Equal returns true if both sets have the same elements
func (s *Set[T]) Equal(other *Set[T]) bool {
	return maps.Equal(s.values, other.values)
}

// IsEmpty returns true if the set is empty.
func (s *Set[T]) IsEmpty() bool {
	return len(s.values) == 0
}

// AsSlice returns the set as a slice of values.
func (s *Set[T]) AsSlice() []T {
	if len(s.values) == 0 {
		return nil
	}

	return expmaps.Keys(s.values)
}

// Len returns the length of the set.
func (s *Set[T]) Len() int {
	return len(s.values)
}

// ForEach executes the callback for each item in the set until an error is encountered.
func (s *Set[T]) ForEach(callback func(value T) error) error {
	for value := range s.values {
		if err := callback(value); err != nil {
			return err
		}
	}

	return nil
}

func (s *Set[T]) MarshalZerologObject(e *zerolog.Event) {
	xs := zerolog.Arr()
	for _, value := range s.values {
		xs.Interface(value)
	}
	e.Array("values", xs)
}
