package query

import (
	"slices"

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

func (u *Union) Check(ctx *Context, resourceIDs []string, subjectID string) (RelationSeq, error) {
	remaining := resourceIDs
	var out []Relation
	for _, it := range u.subIts {
		relSeq, err := it.Check(ctx, remaining, subjectID)
		if err != nil {
			return nil, err
		}
		rels, err := CollectAll(relSeq)
		if err != nil {
			return nil, err
		}

		out = append(out, rels...)

		// If some subset have already passed the check, no need to check them again.
		for _, r := range rels {
			if idx := slices.Index(remaining, r.Resource.ObjectID); idx != -1 {
				remaining = slices.Delete(remaining, idx, idx+1)
			}
		}

		if len(remaining) == 0 {
			break
		}
	}
	return func(yield func(Relation, error) bool) {
		for _, rel := range out {
			if !yield(rel, nil) {
				return
			}
		}
	}, nil
}

func (u *Union) IterSubjects(ctx *Context, resourceID string) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (u *Union) IterResources(ctx *Context, subjectID string) (RelationSeq, error) {
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
