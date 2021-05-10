package namespace

import (
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ONRSet is a set of ObjectAndRelation's.
type ONRSet struct {
	onrs map[string]*pb.ObjectAndRelation
}

// NewONRSet creates a new set.
func NewONRSet() *ONRSet {
	return &ONRSet{
		onrs: map[string]*pb.ObjectAndRelation{},
	}
}

// IsEmpty returns whether the set is empty.
func (ons *ONRSet) IsEmpty() bool {
	return len(ons.onrs) == 0
}

// Has returns true if the set contains the given ONR.
func (ons *ONRSet) Has(onr *pb.ObjectAndRelation) bool {
	_, ok := ons.onrs[tuple.StringONR(onr)]
	return ok
}

// Add adds the given ONR to the set.
func (ons *ONRSet) Add(onr *pb.ObjectAndRelation) {
	ons.onrs[tuple.StringONR(onr)] = onr
}

// Update updates the set by adding the given ONRs to it.
func (ons *ONRSet) Update(onrs []*pb.ObjectAndRelation) {
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

// AsSlice returns the ONRs found in the set as a slice.
func (ons *ONRSet) AsSlice() []*pb.ObjectAndRelation {
	slice := []*pb.ObjectAndRelation{}
	for _, onr := range ons.onrs {
		slice = append(slice, onr)
	}
	return slice
}
