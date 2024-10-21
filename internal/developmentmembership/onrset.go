package developmentmembership

import (
	"github.com/ccoveille/go-safecast"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TODO(jschorr): Replace with the generic set over tuple.ObjectAndRelation

// ONRSet is a set of ObjectAndRelation's.
type ONRSet struct {
	onrs *mapz.Set[tuple.ObjectAndRelation]
}

// NewONRSet creates a new set.
func NewONRSet(onrs ...tuple.ObjectAndRelation) ONRSet {
	created := ONRSet{
		onrs: mapz.NewSet[tuple.ObjectAndRelation](),
	}
	created.Update(onrs)
	return created
}

// Length returns the size of the set.
func (ons ONRSet) Length() uint64 {
	// This is the length of a set so we should never fall out of bounds.
	length, _ := safecast.ToUint64(ons.onrs.Len())
	return length
}

// IsEmpty returns whether the set is empty.
func (ons ONRSet) IsEmpty() bool {
	return ons.onrs.IsEmpty()
}

// Has returns true if the set contains the given ONR.
func (ons ONRSet) Has(onr tuple.ObjectAndRelation) bool {
	return ons.onrs.Has(onr)
}

// Add adds the given ONR to the set. Returns true if the object was not in the set before this
// call and false otherwise.
func (ons ONRSet) Add(onr tuple.ObjectAndRelation) bool {
	return ons.onrs.Add(onr)
}

// Update updates the set by adding the given ONRs to it.
func (ons ONRSet) Update(onrs []tuple.ObjectAndRelation) {
	for _, onr := range onrs {
		ons.Add(onr)
	}
}

// UpdateFrom updates the set by adding the ONRs found in the other set to it.
func (ons ONRSet) UpdateFrom(otherSet ONRSet) {
	if otherSet.onrs == nil {
		return
	}
	ons.onrs.Merge(otherSet.onrs)
}

// Intersect returns an intersection between this ONR set and the other set provided.
func (ons ONRSet) Intersect(otherSet ONRSet) ONRSet {
	return ONRSet{ons.onrs.Intersect(otherSet.onrs)}
}

// Subtract returns a subtraction from this ONR set of the other set provided.
func (ons ONRSet) Subtract(otherSet ONRSet) ONRSet {
	return ONRSet{ons.onrs.Subtract(otherSet.onrs)}
}

// Union returns a copy of this ONR set with the other set's elements added in.
func (ons ONRSet) Union(otherSet ONRSet) ONRSet {
	return ONRSet{ons.onrs.Union(otherSet.onrs)}
}

// AsSlice returns the ONRs found in the set as a slice.
func (ons ONRSet) AsSlice() []tuple.ObjectAndRelation {
	slice := make([]tuple.ObjectAndRelation, 0, ons.Length())
	_ = ons.onrs.ForEach(func(onr tuple.ObjectAndRelation) error {
		slice = append(slice, onr)
		return nil
	})
	return slice
}
