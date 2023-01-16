package datasets

import (
	"fmt"
	"strings"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// TODO(jschorr): See if there is a nice way we can combine this withn ONRByTypeSet and the multimap
// used in Check to allow for a simple implementation.

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
func (s *SubjectByTypeSet) AddSubjectOf(relationship *core.RelationTuple) error {
	return s.AddSubject(relationship.Subject, relationship.Caveat)
}

// AddConcreteSubject adds a non-caveated subject to the set.
func (s *SubjectByTypeSet) AddConcreteSubject(subject *core.ObjectAndRelation) error {
	return s.AddSubject(subject, nil)
}

// AddSubject adds the specified subject to the set.
func (s *SubjectByTypeSet) AddSubject(subject *core.ObjectAndRelation, caveat *core.ContextualizedCaveat) error {
	typeKey := fmt.Sprintf("%s#%s", subject.Namespace, subject.Relation)
	if _, ok := s.byType[typeKey]; !ok {
		s.byType[typeKey] = NewSubjectSet()
	}

	return s.byType[typeKey].Add(&v1.FoundSubject{
		SubjectId:        subject.ObjectId,
		CaveatExpression: wrapCaveat(caveat),
	})
}

// ForEachType invokes the handler for each type of ObjectAndRelation found in the set, along
// with all IDs of objects of that type.
func (s *SubjectByTypeSet) ForEachType(handler func(rr *core.RelationReference, subjects SubjectSet)) {
	for key, subjects := range s.byType {
		parts := strings.Split(key, "#")
		handler(&core.RelationReference{
			Namespace: parts[0],
			Relation:  parts[1],
		}, subjects)
	}
}

// Map runs the mapper function over each type of object in the set, returning a new ONRByTypeSet with
// the object type replaced by that returned by the mapper function.
func (s *SubjectByTypeSet) Map(mapper func(rr *core.RelationReference) (*core.RelationReference, error)) (*SubjectByTypeSet, error) {
	mapped := NewSubjectByTypeSet()
	for key, subjectset := range s.byType {
		parts := strings.Split(key, "#")
		updatedType, err := mapper(&core.RelationReference{
			Namespace: parts[0],
			Relation:  parts[1],
		})
		if err != nil {
			return nil, err
		}
		if updatedType == nil {
			continue
		}
		updatedTypeKey := fmt.Sprintf("%s#%s", updatedType.Namespace, updatedType.Relation)
		mapped.byType[updatedTypeKey] = subjectset
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
	typeKey := fmt.Sprintf("%s#%s", rr.Namespace, rr.Relation)
	found, ok := s.byType[typeKey]
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
