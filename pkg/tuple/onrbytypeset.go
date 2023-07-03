package tuple

import (
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ONRByTypeSet is a set of ObjectAndRelation's, grouped by namespace+relation.
type ONRByTypeSet struct {
	byType map[string][]string
}

// NewONRByTypeSet creates and returns a new ONRByTypeSet.
func NewONRByTypeSet() *ONRByTypeSet {
	return &ONRByTypeSet{
		byType: map[string][]string{},
	}
}

// Add adds the specified ObjectAndRelation to the set.
func (s *ONRByTypeSet) Add(onr *core.ObjectAndRelation) {
	key := JoinRelRef(onr.Namespace, onr.Relation)
	if _, ok := s.byType[key]; !ok {
		s.byType[key] = []string{}
	}

	s.byType[key] = append(s.byType[key], onr.ObjectId)
}

// ForEachType invokes the handler for each type of ObjectAndRelation found in the set, along
// with all IDs of objects of that type.
func (s *ONRByTypeSet) ForEachType(handler func(rr *core.RelationReference, objectIds []string)) {
	for key, objectIds := range s.byType {
		ns, rel := MustSplitRelRef(key)
		handler(&core.RelationReference{
			Namespace: ns,
			Relation:  rel,
		}, slicez.Unique(objectIds))
	}
}

// Map runs the mapper function over each type of object in the set, returning a new ONRByTypeSet with
// the object type replaced by that returned by the mapper function.
func (s *ONRByTypeSet) Map(mapper func(rr *core.RelationReference) (*core.RelationReference, error)) (*ONRByTypeSet, error) {
	mapped := NewONRByTypeSet()
	for key, objectIds := range s.byType {
		ns, rel := MustSplitRelRef(key)
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
		mapped.byType[JoinRelRef(updatedType.Namespace, updatedType.Relation)] = slicez.Unique(objectIds)
	}
	return mapped, nil
}

// IsEmpty returns true if the set is empty.
func (s *ONRByTypeSet) IsEmpty() bool {
	return len(s.byType) == 0
}

// Len returns the number of keys in the set.
func (s *ONRByTypeSet) Len() int {
	return len(s.byType)
}
