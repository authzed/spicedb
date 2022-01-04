package membership

import (
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	"github.com/authzed/spicedb/pkg/tuple"
)

func isWildcard(subject *v0.ObjectAndRelation) bool {
	return subject.ObjectId == tuple.PublicWildcard
}

// TrackingSubjectSet defines a set that tracks accessible subjects and their associated
// relationships.
//
// NOTE: This is designed solely for the developer API and testing and should *not* be used in any
// performance sensitive code.
//
// NOTE: Unlike a traditional set, unions between wildcards and a concrete subject will result
// in *both* being present in the set, to maintain the proper relationship tracking and reporting
// of concrete subjects.
//
// TODO(jschorr): Once we have stable generics support, break into a standard SubjectSet and
// a tracking variant built on top of it.
type TrackingSubjectSet map[string]FoundSubject

// NewTrackingSubjectSet creates a new TrackingSubjectSet, with optional initial subjects.
func NewTrackingSubjectSet(subjects ...FoundSubject) TrackingSubjectSet {
	var toReturn TrackingSubjectSet = make(map[string]FoundSubject)
	toReturn.Add(subjects...)
	return toReturn
}

// AddFrom adds the subjects found in the other set to this set.
func (tss TrackingSubjectSet) AddFrom(otherSet TrackingSubjectSet) {
	for _, value := range otherSet {
		tss.Add(value)
	}
}

// RemoveFrom removes any subjects found in the other set from this set.
func (tss TrackingSubjectSet) RemoveFrom(otherSet TrackingSubjectSet) {
	for _, otherSAR := range otherSet {
		tss.Remove(otherSAR.subject)
	}
}

// Add adds the given subjects to this set.
func (tss TrackingSubjectSet) Add(subjectsAndResources ...FoundSubject) {
	tss.AddWithResources(subjectsAndResources, nil)
}

// AddWithResources adds the given subjects to this set, with the additional resources appended
// for each subject to be included in their relationships.
func (tss TrackingSubjectSet) AddWithResources(subjectsAndResources []FoundSubject, additionalResources *tuple.ONRSet) {
	for _, sar := range subjectsAndResources {
		found, ok := tss[toKey(sar.subject)]
		if ok {
			tss[toKey(sar.subject)] = found.union(sar)
		} else {
			tss[toKey(sar.subject)] = sar
		}
	}
}

// Get returns the found subject in the set, if any.
func (tss TrackingSubjectSet) Get(subject *v0.ObjectAndRelation) (FoundSubject, bool) {
	found, ok := tss[toKey(subject)]
	return found, ok
}

// Contains returns true if the set contains the given subject.
func (tss TrackingSubjectSet) Contains(subject *v0.ObjectAndRelation) bool {
	_, ok := tss[toKey(subject)]
	return ok
}

// removeExact removes the given subject(s) from the set. If the subject is a wildcard, only
// the exact matching wildcard will be removed.
func (tss TrackingSubjectSet) removeExact(subjects ...*v0.ObjectAndRelation) {
	for _, subject := range subjects {
		delete(tss, toKey(subject))
	}
}

// Remove removes the given subject(s) from the set. If the subject is a wildcard, all matching
// subjects are removed. If the subject matches a wildcard in the existing set, then it is added
// to that wildcard as an exclusion.
func (tss TrackingSubjectSet) Remove(subjects ...*v0.ObjectAndRelation) {
	for _, subject := range subjects {
		delete(tss, toKey(subject))

		// Delete any entries matching the wildcard, if applicable.
		if isWildcard(subject) {
			// Remove any subjects matching the type.
			for key := range tss {
				current := fromKey(key)
				if current.Namespace == subject.Namespace {
					delete(tss, key)
				}
			}
		} else {
			// Check for any wildcards matching and, if found, add to the exclusion.
			for _, existing := range tss {
				wildcardType, ok := existing.WildcardType()
				if ok && wildcardType == subject.Namespace {
					existing.excludedSubjects.Add(subject)
				}
			}
		}
	}
}

// WithType returns any subjects in the set with the given object type.
func (tss TrackingSubjectSet) WithType(objectType string) []FoundSubject {
	toReturn := make([]FoundSubject, 0, len(tss))
	for _, current := range tss {
		if current.subject.Namespace == objectType {
			toReturn = append(toReturn, current)
		}
	}
	return toReturn
}

// Exclude returns a new set that contains the items in this set minus those in the other set.
func (tss TrackingSubjectSet) Exclude(otherSet TrackingSubjectSet) TrackingSubjectSet {
	newSet := NewTrackingSubjectSet()
	newSet.AddFrom(tss)
	newSet.RemoveFrom(otherSet)
	return newSet
}

// Intersect returns a new set that contains the items in this set *and* the other set. Note that
// if wildcard is found in *both* sets, it will be returned *along* with any concrete subjects found
// on the other side of the intersection.
func (tss TrackingSubjectSet) Intersect(otherSet TrackingSubjectSet) TrackingSubjectSet {
	newSet := NewTrackingSubjectSet()
	for _, current := range tss {
		// Add directly if shared by both.
		other, ok := otherSet.Get(current.subject)
		if ok {
			newSet.Add(current.intersect(other))
		}

		// If the current is a wildcard, and add any matching.
		if isWildcard(current.subject) {
			newSet.AddWithResources(otherSet.WithType(current.subject.Namespace), current.relationships)
		}
	}

	for _, current := range otherSet {
		// If the current is a wildcard, add any matching.
		if isWildcard(current.subject) {
			newSet.AddWithResources(tss.WithType(current.subject.Namespace), current.relationships)
		}
	}

	return newSet
}

// ToSlice returns a slice of all subjects found in the set.
func (tss TrackingSubjectSet) ToSlice() []FoundSubject {
	toReturn := make([]FoundSubject, 0, len(tss))
	for _, current := range tss {
		toReturn = append(toReturn, current)
	}
	return toReturn
}

// ToFoundSubjects returns the set as a FoundSubjects struct.
func (tss TrackingSubjectSet) ToFoundSubjects() FoundSubjects {
	return FoundSubjects{tss}
}

func toKey(subject *v0.ObjectAndRelation) string {
	return fmt.Sprintf("%s %s %s", subject.Namespace, subject.ObjectId, subject.Relation)
}

func fromKey(key string) *v0.ObjectAndRelation {
	subject := &v0.ObjectAndRelation{}
	fmt.Sscanf(key, "%s %s %s", &subject.Namespace, &subject.ObjectId, &subject.Relation)
	return subject
}
