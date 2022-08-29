package util

import (
	"golang.org/x/exp/maps"

	"github.com/authzed/spicedb/pkg/tuple"
)

// Subject is a subject that can be placed into a BaseSubjectSet.
type Subject interface {
	// GetSubjectId returns the ID of the subject. For wildcards, this should be `*`.
	GetSubjectId() string

	// GetExcludedSubjectIds returns the list of subject IDs excluded. Should only have values
	// for wildcards.
	GetExcludedSubjectIds() []string
}

// BaseSubjectSet defines a set that tracks accessible subjects. It is generic to allow
// other implementations to define the kind of tracking information associated with each subject.
//
// NOTE: Unlike a traditional set, unions between wildcards and a concrete subject will result
// in *both* being present in the set, to maintain the proper set semantics around wildcards.
type BaseSubjectSet[T Subject] struct {
	values      map[string]T
	constructor func(subjectID string, excludedSubjectIDs []string, sources ...T) T
	combiner    func(existing T, added T) T
}

// NewBaseSubjectSet creates a new base subject set for use underneath well-typed implementation.
//
// The constructor function is a function that returns a new instancre of type T for a particular
// subject ID.
// The combiner function is optional, and if given, is used to combine existing elements in the
// set into a new element. This is typically used in debug packages for tracking of additional
// metadata.
func NewBaseSubjectSet[T Subject](
	constructor func(subjectID string, excludedSubjectIDs []string, sources ...T) T,
	combiner func(existing T, added T) T,
) BaseSubjectSet[T] {
	return BaseSubjectSet[T]{
		values:      map[string]T{},
		constructor: constructor,
		combiner:    combiner,
	}
}

// Add adds the found subject to the set. This is equivalent to a Union operation between the
// existing set of subjects and a set containing the single subject.
func (bss BaseSubjectSet[T]) Add(foundSubject T) bool {
	existing, ok := bss.values[foundSubject.GetSubjectId()]
	if !ok {
		bss.values[foundSubject.GetSubjectId()] = foundSubject
	}

	if foundSubject.GetSubjectId() == tuple.PublicWildcard {
		if ok {
			// Intersect any exceptions, as union between one wildcard and another is a wildcard
			// with the exceptions intersected.
			//
			// As a concrete example, given `user:* - user:tom` and `user:* - user:sarah`, the union
			// of the two will be `*`, since each handles the other user.
			excludedIds := NewSet[string](existing.GetExcludedSubjectIds()...).IntersectionDifference(NewSet[string](foundSubject.GetExcludedSubjectIds()...))
			bss.values[tuple.PublicWildcard] = bss.constructor(tuple.PublicWildcard, excludedIds.AsSlice(), existing, foundSubject)
		}
	} else {
		// If there is an existing wildcard, remove the subject from its exclusions list.
		if existingWildcard, ok := bss.values[tuple.PublicWildcard]; ok {
			excludedIds := NewSet[string](existingWildcard.GetExcludedSubjectIds()...)
			excludedIds.Remove(foundSubject.GetSubjectId())
			bss.values[tuple.PublicWildcard] = bss.constructor(tuple.PublicWildcard, excludedIds.AsSlice(), existingWildcard)
		}
	}

	if bss.combiner != nil && ok {
		bss.values[foundSubject.GetSubjectId()] = bss.combiner(bss.values[foundSubject.GetSubjectId()], foundSubject)
	}

	return !ok
}

// Subtract subtracts the given subject found the set.
func (bss BaseSubjectSet[T]) Subtract(foundSubject T) {
	// If the subject being removed is a wildcard, then remove any non-excluded items and adjust
	// the existing wildcard.
	if foundSubject.GetSubjectId() == tuple.PublicWildcard {
		exclusions := NewSet[string](foundSubject.GetExcludedSubjectIds()...)
		for existingSubjectID := range bss.values {
			if existingSubjectID == tuple.PublicWildcard {
				continue
			}

			if !exclusions.Has(existingSubjectID) {
				delete(bss.values, existingSubjectID)
			}
		}

		// Check for an existing wildcard and adjust accordingly.
		if existing, ok := bss.values[tuple.PublicWildcard]; ok {
			// A subtraction of a wildcard from another wildcard subtracts the exclusions from the second.
			// from the first, and places them into the subject set directly.
			//
			// As a concrete example, given `user:* - user:tom` - `user:* - user:sarah`, the subtraction
			// of the two will be `user:sarah`, since sarah is in the first set and not in the second.
			existingExclusions := NewSet[string](existing.GetExcludedSubjectIds()...)
			for _, subjectID := range foundSubject.GetExcludedSubjectIds() {
				if !existingExclusions.Has(subjectID) {
					bss.values[subjectID] = bss.constructor(subjectID, nil, foundSubject)
				}
			}
		}

		delete(bss.values, tuple.PublicWildcard)
		return
	}

	// Remove the subject itself from the set.
	delete(bss.values, foundSubject.GetSubjectId())

	// If wildcard exists within the subject set, add the found subject to the exclusion list.
	if wildcard, ok := bss.values[tuple.PublicWildcard]; ok {
		exclusions := NewSet[string](wildcard.GetExcludedSubjectIds()...)
		exclusions.Add(foundSubject.GetSubjectId())
		bss.values[tuple.PublicWildcard] = bss.constructor(tuple.PublicWildcard, exclusions.AsSlice(), wildcard)
	}
}

// SubtractAll subtracts the other set of subjects from this set of subtracts, modifying this
// set in place.
func (bss BaseSubjectSet[T]) SubtractAll(other BaseSubjectSet[T]) {
	for _, fs := range other.values {
		bss.Subtract(fs)
	}
}

// IntersectionDifference performs an intersection between this set and the other set, modifying
// this set in place.
func (bss BaseSubjectSet[T]) IntersectionDifference(other BaseSubjectSet[T]) {
	// Check if the other set has a wildcard. If so, remove any subjects found in the exclusion
	// list.
	//
	// As a concrete example, given `user:tom` and `user:* - user:sarah`, the intersection should
	// return `user:tom`, because everyone but `sarah` (including `tom`) is in the second set.
	otherWildcard, hasOtherWildcard := other.values[tuple.PublicWildcard]
	if hasOtherWildcard {
		exclusion := NewSet[string](otherWildcard.GetExcludedSubjectIds()...)
		for subjectID := range bss.values {
			if subjectID != tuple.PublicWildcard {
				if exclusion.Has(subjectID) {
					delete(bss.values, subjectID)
				}
			}
		}
	}

	// Remove any concrete subjects, if the other does not have a wildcard.
	if !hasOtherWildcard {
		for subjectID := range bss.values {
			if subjectID != tuple.PublicWildcard {
				if _, ok := other.values[subjectID]; !ok {
					delete(bss.values, subjectID)
				}
			}
		}
	}

	// Handle the case where the current set has a wildcard. We have to do two operations:
	//
	// 1) If the current set has a wildcard, either add the exclusions together if the other set
	// also has a wildcard, or remove it if it did not.
	//
	// 2) We also add in any other set members that  are not in the wildcard's exclusion set, as
	// an intersection between a wildcard with exclusions and concrete types will always return
	// concrete types as well.
	if wildcard, ok := bss.values[tuple.PublicWildcard]; ok {
		exclusions := NewSet[string](wildcard.GetExcludedSubjectIds()...)

		if hasOtherWildcard {
			toBeExcluded := NewSet[string]()
			toBeExcluded.Extend(wildcard.GetExcludedSubjectIds())
			toBeExcluded.Extend(otherWildcard.GetExcludedSubjectIds())
			bss.values[tuple.PublicWildcard] = bss.constructor(tuple.PublicWildcard, toBeExcluded.AsSlice(), wildcard, otherWildcard)
		} else {
			// Remove this wildcard.
			delete(bss.values, tuple.PublicWildcard)
		}

		// Add any concrete items from the other set into this set. This is necebssary because an
		// intersection between a wildcard and a concrete should always return that concrete, except
		// if it is within the wildcard's exclusion list.
		//
		// As a concrete example, given `user:* - user:tom` and `user:sarah`, the first set contains
		// all users except `tom` (and thus includes `sarah`) and the second is `sarah`, so the result
		// must include `sarah`.
		for subjectID, fs := range other.values {
			if subjectID != tuple.PublicWildcard && !exclusions.Has(subjectID) {
				bss.values[subjectID] = fs
			}
		}
	}

	// If a combiner is defined, run it over all values from both sets.
	if bss.combiner != nil {
		for subjectID := range bss.values {
			if added, ok := other.values[subjectID]; ok {
				bss.values[subjectID] = bss.combiner(bss.values[subjectID], added)
			}
		}
	}
}

// UnionWith adds the given subjects to this set, via a union call.
func (bss BaseSubjectSet[T]) UnionWith(foundSubjects []T) {
	for _, fs := range foundSubjects {
		bss.Add(fs)
	}
}

// UnionWithSet performs a union operation between this set and the other set, modifying this
// set in place.
func (bss BaseSubjectSet[T]) UnionWithSet(other BaseSubjectSet[T]) {
	bss.UnionWith(other.AsSlice())
}

// Get returns the found subject with the given ID in the set, if any.
func (bss BaseSubjectSet[T]) Get(id string) (T, bool) {
	found, ok := bss.values[id]
	return found, ok
}

// IsEmpty returns whether the subject set is empty.
func (bss BaseSubjectSet[T]) IsEmpty() bool {
	return len(bss.values) == 0
}

// AsSlice returns the contents of the subject set as a slice of found subjects.
func (bss BaseSubjectSet[T]) AsSlice() []T {
	slice := make([]T, 0, len(bss.values))
	for _, fs := range bss.values {
		slice = append(slice, fs)
	}
	return slice
}

// Clone returns a clone of this subject set. Note that this is a shallow clone.
// NOTE: Should only be used when performance is not a concern.
func (bss BaseSubjectSet[T]) Clone() BaseSubjectSet[T] {
	return BaseSubjectSet[T]{maps.Clone(bss.values), bss.constructor, bss.combiner}
}

// UnsafeRemoveExact removes the *exact* matching subject, with no wildcard handling.
// This should ONLY be used for testing.
func (bss BaseSubjectSet[T]) UnsafeRemoveExact(foundSubject T) {
	delete(bss.values, foundSubject.GetSubjectId())
}
