package datasets

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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
		BaseSubjectSet: NewBaseSubjectSet(subjectSetConstructor),
	}
}

func (ss SubjectSet) SubtractAll(other SubjectSet) {
	ss.BaseSubjectSet.SubtractAll(other.BaseSubjectSet)
}

func (ss SubjectSet) MustIntersectionDifference(other SubjectSet) {
	ss.BaseSubjectSet.MustIntersectionDifference(other.BaseSubjectSet)
}

func (ss SubjectSet) IntersectionDifference(other SubjectSet) error {
	return ss.BaseSubjectSet.IntersectionDifference(other.BaseSubjectSet)
}

func (ss SubjectSet) MustUnionWithSet(other SubjectSet) {
	ss.BaseSubjectSet.MustUnionWithSet(other.BaseSubjectSet)
}

func (ss SubjectSet) UnionWithSet(other SubjectSet) error {
	return ss.BaseSubjectSet.UnionWithSet(other.BaseSubjectSet)
}

// WithParentCaveatExpression returns a copy of the subject set with the parent caveat expression applied
// to all members of this set.
func (ss SubjectSet) WithParentCaveatExpression(parentCaveatExpr *core.CaveatExpression) SubjectSet {
	return SubjectSet{ss.BaseSubjectSet.WithParentCaveatExpression(parentCaveatExpr)}
}

func (ss SubjectSet) AsFoundSubjects() *v1.FoundSubjects {
	return &v1.FoundSubjects{
		FoundSubjects: ss.AsSlice(),
	}
}

func subjectSetConstructor(subjectID string, caveatExpression *core.CaveatExpression, excludedSubjects []*v1.FoundSubject, _ ...*v1.FoundSubject) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId:        subjectID,
		CaveatExpression: caveatExpression,
		ExcludedSubjects: excludedSubjects,
	}
}
