package developmentmembership

import (
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// onrStruct is a struct holding a namespace and relation.
type onrStruct struct {
	Namespace string
	ObjectID  string
	Relation  string
}

// ONRSet is a set of ObjectAndRelation's.
type ONRSet struct {
	onrs *mapz.Set[onrStruct]
}

// NewONRSet creates a new set.
func NewONRSet(onrs ...*core.ObjectAndRelation) ONRSet {
	created := ONRSet{
		onrs: mapz.NewSet[onrStruct](),
	}
	created.Update(onrs)
	return created
}

// Length returns the size of the set.
func (ons ONRSet) Length() uint64 {
	return uint64(ons.onrs.Len())
}

// IsEmpty returns whether the set is empty.
func (ons ONRSet) IsEmpty() bool {
	return ons.onrs.IsEmpty()
}

// Has returns true if the set contains the given ONR.
func (ons ONRSet) Has(onr *core.ObjectAndRelation) bool {
	key := onrStruct{onr.Namespace, onr.ObjectId, onr.Relation}
	return ons.onrs.Has(key)
}

// Add adds the given ONR to the set. Returns true if the object was not in the set before this
// call and false otherwise.
func (ons ONRSet) Add(onr *core.ObjectAndRelation) bool {
	key := onrStruct{onr.Namespace, onr.ObjectId, onr.Relation}
	return ons.onrs.Add(key)
}

// Update updates the set by adding the given ONRs to it.
func (ons ONRSet) Update(onrs []*core.ObjectAndRelation) {
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
func (ons ONRSet) AsSlice() []*core.ObjectAndRelation {
	slice := make([]*core.ObjectAndRelation, 0, ons.Length())
	_ = ons.onrs.ForEach(func(rr onrStruct) error {
		slice = append(slice, &core.ObjectAndRelation{
			Namespace: rr.Namespace,
			ObjectId:  rr.ObjectID,
			Relation:  rr.Relation,
		})
		return nil
	})
	return slice
}
