package tuple

import (
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
)

// ONRSet is a set of ObjectAndRelation's.
type ONRSet struct {
	onrs map[string]*v0.ObjectAndRelation
}

// NewONRSet creates a new set.
func NewONRSet(onrs ...*v0.ObjectAndRelation) *ONRSet {
	created := &ONRSet{
		onrs: map[string]*v0.ObjectAndRelation{},
	}
	created.Update(onrs)
	return created
}

// Length returns the size of the set.
func (ons *ONRSet) Length() uint32 {
	return uint32(len(ons.onrs))
}

// IsEmpty returns whether the set is empty.
func (ons *ONRSet) IsEmpty() bool {
	return len(ons.onrs) == 0
}

// Has returns true if the set contains the given ONR.
func (ons *ONRSet) Has(onr *v0.ObjectAndRelation) bool {
	_, ok := ons.onrs[StringONR(onr)]
	return ok
}

// Add adds the given ONR to the set. Returns true if the object was not in the set before this
// call and false otherwise.
func (ons *ONRSet) Add(onr *v0.ObjectAndRelation) bool {
	_, ok := ons.onrs[StringONR(onr)]
	if ok {
		return false
	}

	ons.onrs[StringONR(onr)] = onr
	return true
}

// Update updates the set by adding the given ONRs to it.
func (ons *ONRSet) Update(onrs []*v0.ObjectAndRelation) {
	for _, onr := range onrs {
		ons.Add(onr)
	}
}

// UpdateFrom updates the set by adding the ONRs found in the other set to it.
func (ons *ONRSet) UpdateFrom(otherSet *ONRSet) {
	for _, onr := range otherSet.onrs {
		ons.Add(onr)
	}
}

// Intersect returns an intersection between this ONR set and the other set provided.
func (ons *ONRSet) Intersect(otherSet *ONRSet) *ONRSet {
	updated := NewONRSet()
	for _, onr := range ons.onrs {
		if otherSet.Has(onr) {
			updated.Add(onr)
		}
	}
	return updated
}

// Subtract returns a subtraction from this ONR set of the other set provided.
func (ons *ONRSet) Subtract(otherSet *ONRSet) *ONRSet {
	updated := NewONRSet()
	for _, onr := range ons.onrs {
		if !otherSet.Has(onr) {
			updated.Add(onr)
		}
	}
	return updated
}

// With returns a copy of this ONR set with the given element added.
func (ons *ONRSet) With(onr *v0.ObjectAndRelation) *ONRSet {
	updated := NewONRSet()
	for _, current := range ons.onrs {
		updated.Add(current)
	}
	updated.Add(onr)
	return updated
}

// AsSlice returns the ONRs found in the set as a slice.
func (ons *ONRSet) AsSlice() []*v0.ObjectAndRelation {
	slice := make([]*v0.ObjectAndRelation, 0, len(ons.onrs))
	for _, onr := range ons.onrs {
		slice = append(slice, onr)
	}
	return slice
}
