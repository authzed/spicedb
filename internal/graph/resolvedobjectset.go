package graph

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/tuple"
)

type resolvedObjectSet struct {
	entries map[string]ResolvedObject
}

func newSet() *resolvedObjectSet {
	return &resolvedObjectSet{
		entries: map[string]ResolvedObject{},
	}
}

func (s *resolvedObjectSet) key(value ResolvedObject) string {
	return fmt.Sprintf("%s:%s#%s", value.ONR.Namespace, value.ONR.ObjectId, value.ONR.Relation)
}

func (s *resolvedObjectSet) add(value ResolvedObject) bool {
	_, ok := s.entries[s.key(value)]
	if ok {
		return false
	}

	s.entries[s.key(value)] = value
	return true
}

func (s *resolvedObjectSet) update(ros []ResolvedObject) bool {
	changed := false
	for _, value := range ros {
		if s.add(value) {
			changed = true
		}
	}
	return changed
}

func (s *resolvedObjectSet) EmitForTrace(tracer DebugTracer) {
	for _, value := range s.entries {
		tracer.Child(tuple.StringONR(value.ONR))
	}
}

func (s *resolvedObjectSet) length() int {
	return len(s.entries)
}

func (s *resolvedObjectSet) asSlice() []ResolvedObject {
	slice := []ResolvedObject{}
	for _, value := range s.entries {
		slice = append(slice, value)
	}
	return slice
}

func (s *resolvedObjectSet) remove(value ResolvedObject) {
	delete(s.entries, s.key(value))
}

func (s *resolvedObjectSet) intersect(otherSet *resolvedObjectSet) *resolvedObjectSet {
	updated := newSet()
	for key, value := range s.entries {
		_, ok := otherSet.entries[key]
		if ok {
			updated.add(value)
		}
	}
	return updated
}

func (s *resolvedObjectSet) exclude(otherSet *resolvedObjectSet) *resolvedObjectSet {
	updated := newSet()
	updated.update(s.asSlice())

	for _, value := range otherSet.entries {
		updated.remove(value)
	}

	return updated
}
