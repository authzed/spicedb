package util

import (
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// SubjectSet defines a set that tracks accessible subjects.
//
// NOTE: Unlike a traditional set, unions between wildcards and a concrete subject will result
// in *both* being present in the set, to maintain the proper set semantics around wildcards.
type SubjectSet struct {
	BaseSubjectSet[*v1.FoundSubject]
}

// NewSubjectSet creates and returns a new subject set.
func NewSubjectSet() SubjectSet {
	return SubjectSet{
		BaseSubjectSet: NewBaseSubjectSet[*v1.FoundSubject](subjectSetConstructor),
	}
}

func (ss SubjectSet) SubtractAll(other SubjectSet) {
	ss.BaseSubjectSet.SubtractAll(other.BaseSubjectSet)
}

func (ss SubjectSet) IntersectionDifference(other SubjectSet) {
	ss.BaseSubjectSet.IntersectionDifference(other.BaseSubjectSet)
}

func (ss SubjectSet) UnionWithSet(other SubjectSet) {
	ss.BaseSubjectSet.UnionWithSet(other.BaseSubjectSet)
}

func subjectSetConstructor(subjectID string, caveatExpression *v1.CaveatExpression, excludedSubjects []*v1.FoundSubject, sources ...*v1.FoundSubject) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId:        subjectID,
		CaveatExpression: caveatExpression,
		ExcludedSubjects: excludedSubjects,
	}
}
