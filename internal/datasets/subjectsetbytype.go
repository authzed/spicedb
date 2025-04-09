package datasets

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// SubjectByTypeSet is a set of SubjectSet's, grouped by their subject types.
type SubjectByTypeSet struct {
	byType map[string]SubjectSet
}

// NewSubjectByTypeSet creates and returns a new SubjectByTypeSet.
func NewSubjectByTypeSet() *SubjectByTypeSet {
	return &SubjectByTypeSet{
		byType: map[string]SubjectSet{},
	}
}

// AddSubjectOf adds the subject found in the given relationship, along with its caveat.
func (s *SubjectByTypeSet) AddSubjectOf(relationship tuple.Relationship) error {
	return s.AddSubject(relationship.Subject, relationship.OptionalCaveat)
}

// AddConcreteSubject adds a non-caveated subject to the set.
func (s *SubjectByTypeSet) AddConcreteSubject(subject tuple.ObjectAndRelation) error {
	return s.AddSubject(subject, nil)
}

// AddSubject adds the specified subject to the set.
func (s *SubjectByTypeSet) AddSubject(subject tuple.ObjectAndRelation, caveat *core.ContextualizedCaveat) error {
	key := tuple.JoinRelRef(subject.ObjectType, subject.Relation)
	if _, ok := s.byType[key]; !ok {
		s.byType[key] = NewSubjectSet()
	}

	return s.byType[key].Add(&v1.FoundSubject{
		SubjectId:        subject.ObjectID,
		SubjectData:      subject.ObjectData,
		CaveatExpression: wrapCaveat(caveat),
	})
}

// ForEachType invokes the handler for each type of ObjectAndRelation found in the set, along
// with all IDs of objects of that type.
func (s *SubjectByTypeSet) ForEachType(handler func(rr *core.RelationReference, subjects SubjectSet)) {
	for key, subjects := range s.byType {
		ns, rel := tuple.MustSplitRelRef(key)
		handler(&core.RelationReference{
			Namespace: ns,
			Relation:  rel,
		}, subjects)
	}
}

// Map runs the mapper function over each type of object in the set, returning a new SubjectByTypeSet with
// the object type replaced by that returned by the mapper function.
func (s *SubjectByTypeSet) Map(mapper func(rr *core.RelationReference) (*core.RelationReference, error)) (*SubjectByTypeSet, error) {
	mapped := NewSubjectByTypeSet()
	for key, subjectset := range s.byType {
		ns, rel := tuple.MustSplitRelRef(key)
		updatedType, err := mapper(&core.RelationReference{
			Namespace: ns,
			Relation:  rel,
		})
		if err != nil {
			return nil, err
		}
		if updatedType == nil {
			continue
		}

		key := tuple.JoinRelRef(updatedType.Namespace, updatedType.Relation)
		if existing, ok := mapped.byType[key]; ok {
			cloned := subjectset.Clone()
			if err := cloned.UnionWithSet(existing); err != nil {
				return nil, err
			}
			mapped.byType[key] = cloned
		} else {
			mapped.byType[key] = subjectset
		}
	}
	return mapped, nil
}

// IsEmpty returns true if the set is empty.
func (s *SubjectByTypeSet) IsEmpty() bool {
	return len(s.byType) == 0
}

// Len returns the number of keys in the set.
func (s *SubjectByTypeSet) Len() int {
	return len(s.byType)
}

// SubjectSetForType returns the subject set associated with the given subject type, if any.
func (s *SubjectByTypeSet) SubjectSetForType(rr *core.RelationReference) (SubjectSet, bool) {
	found, ok := s.byType[tuple.JoinRelRef(rr.Namespace, rr.Relation)]
	return found, ok
}

func wrapCaveat(caveat *core.ContextualizedCaveat) *core.CaveatExpression {
	if caveat == nil {
		return nil
	}

	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: caveat,
		},
	}
}
