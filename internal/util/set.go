package util

// Set implements a very basic generic set.
type Set[T comparable] struct {
	values map[T]struct{}
}

// NewSet returns a new set.
func NewSet[T comparable]() *Set[T] {
	return &Set[T]{
		values: map[T]struct{}{},
	}
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

// Remove removes the value from the set, returning whether
// the element was present when the call was made.
func (s *Set[T]) Remove(value T) bool {
	if !s.Has(value) {
		return false
	}

	delete(s.values, value)
	return true
}

// Extend adds all the values to the set.
func (s *Set[T]) Extend(values []T) {
	for _, value := range values {
		s.values[value] = struct{}{}
	}
}

// IntersectionDifference removes any values from this set that
// are not shared with the other set.
func (s *Set[T]) IntersectionDifference(other *Set[T]) {
	for value := range s.values {
		if !other.Has(value) {
			s.Remove(value)
		}
	}
}

// RemoveAll removes all values from this set found in the other set.
func (s *Set[T]) RemoveAll(other *Set[T]) {
	for value := range other.values {
		s.Remove(value)
	}
}

// IsEmpty returns true if the set is empty.
func (s *Set[T]) IsEmpty() bool {
	return len(s.values) == 0
}

// AsSlice returns the set as a slice of values.
func (s *Set[T]) AsSlice() []T {
	slice := make([]T, 0, len(s.values))
	for value := range s.values {
		slice = append(slice, value)
	}
	return slice
}
