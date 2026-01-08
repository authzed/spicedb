package query

import (
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Self is an iterator that produces a synthetic relation for every
// Resource in the subiterator that connects it to
// streamed from the sub-iterator to a specified alias relation.
type Self struct {
	relation string
}

var _ Iterator = &Self{}

func NewSelf(relation string) *Self {
	return &Self{
		relation: relation,
	}
}

func (s *Self) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// for Self, the check returns the reflexive relation if it's in the set of resources
	// or else returns an empty set.
	for _, resource := range resources {
		// TODO: does the subject relation need to be considered here?
		if resource.Equals(GetObject(subject)) {
			return func(yield func(Path, error) bool) {
				yield(Path{
					Resource: resource,
					Relation: s.relation,
					Subject: subject,
				}, nil)
			}, nil
		}
	}
	// Return the empty set if none of the resources matches
	return func(yield func(Path, error) bool) {}, nil
}

func (s *Self) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		yield(Path{
			Resource: resource,
			Relation: s.relation,
			// TODO: is this correct?
			Subject:  resource.WithEllipses(),
		}, nil)
	}, nil
}

func (s *Self) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		yield(Path{
			Resource: GetObject(subject),
			Relation: s.relation,
			Subject:  subject,
		}, nil)
	}, nil
}

func (s *Self) Clone() Iterator {
	return &Self{
		relation: s.relation,
	}
}

func (s *Self) Explain() Explain {
	return Explain{
		Name:       "Self",
		Info:       "Self(" + s.relation + ")",
	}
}

func (f *Self) Subiterators() []Iterator {
	return nil
}

func (s *Self) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return nil, spiceerrors.MustBugf("Trying to replace a Self's subiterators")
}
