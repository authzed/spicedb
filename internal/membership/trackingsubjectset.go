package membership

import (
	"fmt"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/util"
)

// TrackingSubjectSet defines a set that tracks accessible subjects and their associated
// relationships.
//
// NOTE: This is designed solely for the developer API and testing and should *not* be used in any
// performance sensitive code.
type TrackingSubjectSet struct {
	setByType map[string]util.BaseSubjectSet[FoundSubject]
}

// NewTrackingSubjectSet creates a new TrackingSubjectSet, with optional initial subjects.
func NewTrackingSubjectSet(subjects ...FoundSubject) *TrackingSubjectSet {
	tss := &TrackingSubjectSet{
		setByType: map[string]util.BaseSubjectSet[FoundSubject]{},
	}
	for _, subject := range subjects {
		tss.Add(subject)
	}
	return tss
}

// AddFrom adds the subjects found in the other set to this set.
func (tss *TrackingSubjectSet) AddFrom(otherSet *TrackingSubjectSet) {
	for key, oss := range otherSet.setByType {
		tss.getSetForKey(key).UnionWithSet(oss)
	}
}

// RemoveFrom removes any subjects found in the other set from this set.
func (tss *TrackingSubjectSet) RemoveFrom(otherSet *TrackingSubjectSet) {
	for key, oss := range otherSet.setByType {
		tss.getSetForKey(key).SubtractAll(oss)
	}
}

// Add adds the given subjects to this set.
func (tss *TrackingSubjectSet) Add(subjectsAndResources ...FoundSubject) {
	for _, fs := range subjectsAndResources {
		tss.getSet(fs).Add(fs)
	}
}

func keyFor(fs FoundSubject) string {
	return fmt.Sprintf("%s#%s", fs.subject.Namespace, fs.subject.Relation)
}

func (tss *TrackingSubjectSet) getSetForKey(key string) util.BaseSubjectSet[FoundSubject] {
	if existing, ok := tss.setByType[key]; ok {
		return existing
	}

	parts := strings.Split(key, "#")

	created := util.NewBaseSubjectSet[FoundSubject](
		func(subjectID string, excludedSubjectIDs []string, sources ...FoundSubject) FoundSubject {
			fs := NewFoundSubject(&core.ObjectAndRelation{
				Namespace: parts[0],
				ObjectId:  subjectID,
				Relation:  parts[1],
			})
			fs.excludedSubjectIds = excludedSubjectIDs
			for _, source := range sources {
				fs.relationships.UpdateFrom(source.relationships)
			}
			return fs
		},
		func(existing FoundSubject, added FoundSubject) FoundSubject {
			fs := NewFoundSubject(existing.subject)
			fs.excludedSubjectIds = existing.excludedSubjectIds
			fs.relationships = existing.relationships.Union(added.relationships)
			return fs
		},
	)
	tss.setByType[key] = created
	return created
}

func (tss *TrackingSubjectSet) getSet(fs FoundSubject) util.BaseSubjectSet[FoundSubject] {
	fsKey := keyFor(fs)
	return tss.getSetForKey(fsKey)
}

// Get returns the found subject in the set, if any.
func (tss *TrackingSubjectSet) Get(subject *core.ObjectAndRelation) (FoundSubject, bool) {
	set, ok := tss.setByType[fmt.Sprintf("%s#%s", subject.Namespace, subject.Relation)]
	if !ok {
		return FoundSubject{}, false
	}

	return set.Get(subject.ObjectId)
}

// Contains returns true if the set contains the given subject.
func (tss *TrackingSubjectSet) Contains(subject *core.ObjectAndRelation) bool {
	_, ok := tss.Get(subject)
	return ok
}

// Exclude returns a new set that contains the items in this set minus those in the other set.
func (tss *TrackingSubjectSet) Exclude(otherSet *TrackingSubjectSet) *TrackingSubjectSet {
	newSet := NewTrackingSubjectSet()

	for key, bss := range tss.setByType {
		cloned := bss.Clone()
		if oss, ok := otherSet.setByType[key]; ok {
			cloned.SubtractAll(oss)
		}

		newSet.setByType[key] = cloned
	}

	return newSet
}

// Intersect returns a new set that contains the items in this set *and* the other set. Note that
// if wildcard is found in *both* sets, it will be returned *along* with any concrete subjects found
// on the other side of the intersection.
func (tss *TrackingSubjectSet) Intersect(otherSet *TrackingSubjectSet) *TrackingSubjectSet {
	newSet := NewTrackingSubjectSet()

	for key, bss := range tss.setByType {
		if oss, ok := otherSet.setByType[key]; ok {
			cloned := bss.Clone()
			cloned.IntersectionDifference(oss)
			newSet.setByType[key] = cloned
		}
	}

	return newSet
}

// removeExact removes the given subject(s) from the set. If the subject is a wildcard, only
// the exact matching wildcard will be removed.
func (tss TrackingSubjectSet) removeExact(subjects ...*core.ObjectAndRelation) {
	for _, subject := range subjects {
		if set, ok := tss.setByType[fmt.Sprintf("%s#%s", subject.Namespace, subject.Relation)]; ok {
			set.UnsafeRemoveExact(FoundSubject{
				subject: subject,
			})
		}
	}
}

// ToSlice returns a slice of all subjects found in the set.
func (tss TrackingSubjectSet) ToSlice() []FoundSubject {
	subjects := []FoundSubject{}
	for _, bss := range tss.setByType {
		subjects = append(subjects, bss.AsSlice()...)
	}

	return subjects
}

// ToFoundSubjects returns the set as a FoundSubjects struct.
func (tss TrackingSubjectSet) ToFoundSubjects() FoundSubjects {
	return FoundSubjects{tss}
}

// IsEmpty returns true if the tracking subject set is empty.
func (tss TrackingSubjectSet) IsEmpty() bool {
	for _, bss := range tss.setByType {
		if !bss.IsEmpty() {
			return false
		}
	}
	return true
}
