package query

import (
	"github.com/google/uuid"

	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// SelfIterator is an iterator that produces a synthetic relation for every
// Resource in the subiterator that connects it to
// streamed from the sub-iterator to a specified alias relation.
type SelfIterator struct {
	id           string
	relation     string
	canonicalKey CanonicalKey
	// typeName is the name of the type associated with the self definition.
	// It's used to generate the ObjectType for ResourceType() and SubjectTypes().
	typeName string
}

var _ Iterator = &SelfIterator{}

func NewSelfIterator(relation string, typeName string) *SelfIterator {
	return &SelfIterator{
		id:       uuid.NewString(),
		relation: relation,
		typeName: typeName,
	}
}

func (s *SelfIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// for Self, the check returns the reflexive relation if it's in the set of resources
	// or else returns an empty set.
	for _, resource := range resources {
		// NOTE: we assert that the subject and the object are the same and that
		// both are ellipses because self is checking whether the subject and the object
		// are the same, and a subject with a non-ellipsis relation can't semantically
		// match the object.
		if subject.Equals(resource.WithEllipses()) {
			return func(yield func(Path, error) bool) {
				yield(Path{
					Resource: resource,
					Relation: s.relation,
					Subject:  subject,
				}, nil)
			}, nil
		}
	}
	// Return the empty set if none of the resources matches
	return EmptyPathSeq(), nil
}

func (s *SelfIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		yield(Path{
			Resource: resource,
			Relation: s.relation,
			// NOTE: this is WithEllipses() for the same reason as the comment above.
			Subject: resource.WithEllipses(),
		}, nil)
	}, nil
}

func (s *SelfIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	// Only return a self-edge if the subject type matches the iterator's resource type
	if s.typeName != subject.ObjectType {
		return EmptyPathSeq(), nil
	}

	return func(yield func(Path, error) bool) {
		yield(Path{
			Resource: GetObject(subject),
			Relation: s.relation,
			Subject:  subject,
		}, nil)
	}, nil
}

func (s *SelfIterator) Clone() Iterator {
	return &SelfIterator{
		id:       uuid.NewString(),
		relation: s.relation,
		typeName: s.typeName,
	}
}

func (s *SelfIterator) Explain() Explain {
	return Explain{
		Name: "Self",
		Info: "Self(" + s.relation + ")",
	}
}

func (s *SelfIterator) Subiterators() []Iterator {
	return nil
}

func (s *SelfIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return nil, spiceerrors.MustBugf("Trying to replace a Self's subiterators")
}

func (s *SelfIterator) ID() string {
	return s.id
}

func (s *SelfIterator) ResourceType() ([]ObjectType, error) {
	return []ObjectType{{
		Type:        s.typeName,
		Subrelation: tuple.Ellipsis,
	}}, nil
}

func (s *SelfIterator) SubjectTypes() ([]ObjectType, error) {
	// Self is self-referential - subjects are the same type as resources
	return []ObjectType{{
		Type:        s.typeName,
		Subrelation: tuple.Ellipsis,
	}}, nil
}
