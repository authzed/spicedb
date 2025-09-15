package query

import (
	"maps"

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

	// Deduplicate relations based on resource for CheckImpl
	// Since the subject is fixed in CheckImpl, we only need to deduplicate by resource
	seen := make(map[string]Relation)
	for _, rel := range out {
		// Use resource object (type + id) as key for deduplication, not the full resource with relation
		key := rel.Resource.ObjectType + ":" + rel.Resource.ObjectID
		if existing, exists := seen[key]; !exists {
			seen[key] = rel
		} else {
			// If we already have a relationship for this resource,
			// prefer one without caveats (cleaner permission grant)
			if rel.OptionalCaveat == nil && existing.OptionalCaveat != nil {
				seen[key] = rel
			}
		}
	}

	// Convert map to slice
	deduplicated := maps.Values(seen)

	return func(yield func(Relation, error) bool) {
		for rel := range deduplicated {
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
