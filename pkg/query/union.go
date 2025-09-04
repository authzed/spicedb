package query

import (
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Union the set of relations that are in any of underlying subiterators.
// This is equivalent to `permission foo = bar | baz`
type Union struct {
	subIts []Iterator
}

var _ Iterator = &Union{}

func NewUnion() *Union {
	return &Union{}
}

func (u *Union) addSubIterator(subIt Iterator) {
	u.subIts = append(u.subIts, subIt)
}

func (u *Union) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	var out []Relation
	// Collect relations from all sub-iterators
	for _, it := range u.subIts {
		relSeq, err := it.CheckImpl(ctx, resources, subject)
		if err != nil {
			return nil, err
		}
		rels, err := CollectAll(relSeq)
		if err != nil {
			return nil, err
		}

		out = append(out, rels...)
	}

	// Deduplicate relations
	seen := make(map[string]bool)
	var deduplicated []Relation
	for _, rel := range out {
		key := rel.Resource.ObjectType + ":" + rel.Resource.ObjectID + "#" + rel.Resource.Relation + "@" + rel.Subject.ObjectType + ":" + rel.Subject.ObjectID + "#" + rel.Subject.Relation
		if !seen[key] {
			seen[key] = true
			deduplicated = append(deduplicated, rel)
		}
	}

	return func(yield func(Relation, error) bool) {
		for _, rel := range deduplicated {
			if !yield(rel, nil) {
				return
			}
		}
	}, nil
}

func (u *Union) IterSubjectsImpl(ctx *Context, resource Object) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (u *Union) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (u *Union) Clone() Iterator {
	cloned := &Union{
		subIts: make([]Iterator, len(u.subIts)),
	}
	for idx, subIt := range u.subIts {
		cloned.subIts[idx] = subIt.Clone()
	}
	return cloned
}

func (u *Union) Explain() Explain {
	subs := make([]Explain, len(u.subIts))
	for i, it := range u.subIts {
		subs[i] = it.Explain()
	}
	return Explain{
		Info:       "Union",
		SubExplain: subs,
	}
}
