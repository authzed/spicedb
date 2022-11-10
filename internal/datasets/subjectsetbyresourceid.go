package datasets

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// NewSubjectSetByResourceID creates and returns a map of subject sets, indexed by resource ID.
func NewSubjectSetByResourceID() SubjectSetByResourceID {
	return SubjectSetByResourceID{
		subjectSetByResourceID: map[string]SubjectSet{},
	}
}

// SubjectSetByResourceID defines a helper type which maps from a resource ID to its associated found
// subjects, in the form of a subject set per resource ID.
type SubjectSetByResourceID struct {
	subjectSetByResourceID map[string]SubjectSet
}

func (ssr SubjectSetByResourceID) add(resourceID string, subject *v1.FoundSubject) {
	_, ok := ssr.subjectSetByResourceID[resourceID]
	if !ok {
		ssr.subjectSetByResourceID[resourceID] = NewSubjectSet()
	}
	ssr.subjectSetByResourceID[resourceID].Add(subject)
}

// AddFromRelationship adds the subject found in the given relationship to this map, indexed at
// the resource ID specified in the relationship.
func (ssr SubjectSetByResourceID) AddFromRelationship(relationship *core.RelationTuple) {
	ssr.add(relationship.ResourceAndRelation.ObjectId, &v1.FoundSubject{
		SubjectId:        relationship.Subject.ObjectId,
		CaveatExpression: wrapCaveat(relationship.Caveat),
	})
}

// UnionWith unions the map's sets with the other map of sets provided.
func (ssr SubjectSetByResourceID) UnionWith(other map[string]*v1.FoundSubjects) {
	for resourceID, subjects := range other {
		for _, subject := range subjects.FoundSubjects {
			ssr.add(resourceID, subject)
		}
	}
}

// IntersectionDifference performs an in-place intersection between the two maps' sets.
func (ssr SubjectSetByResourceID) IntersectionDifference(other SubjectSetByResourceID) {
	for otherResourceID, otherSubjectSet := range other.subjectSetByResourceID {
		existing, ok := ssr.subjectSetByResourceID[otherResourceID]
		if !ok {
			continue
		}

		existing.IntersectionDifference(otherSubjectSet)
		if existing.IsEmpty() {
			delete(ssr.subjectSetByResourceID, otherResourceID)
		}
	}

	for existingResourceID := range ssr.subjectSetByResourceID {
		_, ok := other.subjectSetByResourceID[existingResourceID]
		if !ok {
			delete(ssr.subjectSetByResourceID, existingResourceID)
			continue
		}
	}
}

// SubtractAll subtracts all sets in the other map from this map's sets.
func (ssr SubjectSetByResourceID) SubtractAll(other SubjectSetByResourceID) {
	for otherResourceID, otherSubjectSet := range other.subjectSetByResourceID {
		existing, ok := ssr.subjectSetByResourceID[otherResourceID]
		if !ok {
			continue
		}

		existing.SubtractAll(otherSubjectSet)
		if existing.IsEmpty() {
			delete(ssr.subjectSetByResourceID, otherResourceID)
		}
	}
}

// IsEmpty returns true if the map is empty.
func (ssr SubjectSetByResourceID) IsEmpty() bool {
	return len(ssr.subjectSetByResourceID) == 0
}

// AsMap converts the map into a map for storage in a proto.
func (ssr SubjectSetByResourceID) AsMap() map[string]*v1.FoundSubjects {
	mapped := make(map[string]*v1.FoundSubjects, len(ssr.subjectSetByResourceID))
	for resourceID, subjectsSet := range ssr.subjectSetByResourceID {
		mapped[resourceID] = subjectsSet.AsFoundSubjects()
	}
	return mapped
}
