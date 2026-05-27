package query

import (
	"errors"
	"fmt"
	"io"

	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

func init() {
	MustRegisterIterator(IteratorSpec{
		Type: SelfIteratorType,
		Name: "Self",
		ConstructWithArgs: func(args *IteratorArgs, _ []Iterator, key CanonicalKey) (Iterator, error) {
			if args == nil || args.RelationName == "" || args.DefinitionName == "" {
				return nil, errors.New("SelfIterator requires RelationName and DefinitionName in Args")
			}
			self := NewSelfIterator(args.RelationName, args.DefinitionName)
			self.canonicalKey = key
			return self, nil
		},
		Deserialize: deserializeSelf,
	})
}

// SelfIterator is an iterator that produces a synthetic relation for every
// Resource in the subiterator that connects it to
// streamed from the sub-iterator to a specified alias relation.
type SelfIterator struct {
	relation     string
	canonicalKey CanonicalKey
	// typeName is the name of the type associated with the self definition.
	// It's used to generate the ObjectType for ResourceType() and SubjectTypes().
	typeName string
}

var _ Iterator = &SelfIterator{}

func NewSelfIterator(relation string, typeName string) *SelfIterator {
	return &SelfIterator{
		relation: relation,
		typeName: typeName,
	}
}

func (s *SelfIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	// NOTE: we assert that the subject and the object are the same and that
	// both are ellipses because self is checking whether the subject and the object
	// are the same, and a subject with a non-ellipsis relation can't semantically
	// match the object.
	if subject.Equals(resource.WithEllipses()) {
		return &Path{
			Resource: resource,
			Relation: s.relation,
			Subject:  subject,
		}, nil
	}
	return nil, nil
}

func (s *SelfIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	return func(yield func(*Path, error) bool) {
		yield(&Path{
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

	return func(yield func(*Path, error) bool) {
		yield(&Path{
			Resource: GetObject(subject),
			Relation: s.relation,
			Subject:  subject,
		}, nil)
	}, nil
}

func (s *SelfIterator) Clone() Iterator {
	return &SelfIterator{
		canonicalKey: s.canonicalKey,
		relation:     s.relation,
		typeName:     s.typeName,
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

func (s *SelfIterator) CanonicalKey() CanonicalKey {
	return s.canonicalKey
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

func (s *SelfIterator) Serialize(w io.Writer) error {
	return SerializeWithHeader(w, SelfIteratorType, s.canonicalKey, func(buf io.Writer) error {
		if err := writeUvarint(buf, 0); err != nil {
			return err
		}
		if err := writeString(buf, s.typeName); err != nil {
			return err
		}
		return writeString(buf, s.relation)
	})
}

func deserializeSelf(body io.Reader, key CanonicalKey, _ *DeserializeContext) (Iterator, error) {
	br := asByteReader(body)
	if _, err := readUvarint(br); err != nil {
		return nil, fmt.Errorf("self flags: %w", err)
	}
	typeName, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("self typeName: %w", err)
	}
	rel, err := readString(br)
	if err != nil {
		return nil, fmt.Errorf("self relation: %w", err)
	}
	self := NewSelfIterator(rel, typeName)
	self.canonicalKey = key
	return self, nil
}
