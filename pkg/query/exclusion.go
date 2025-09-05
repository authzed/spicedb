package query

import (
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Exclusion represents the set of relations that are in the mainSet but not in the excluded set.
// This is equivalent to `permission foo = bar - baz`
type Exclusion struct {
	mainSet  Iterator
	excluded Iterator
}

var _ Iterator = &Exclusion{}

func NewExclusion(mainSet, excluded Iterator) *Exclusion {
	return &Exclusion{
		mainSet:  mainSet,
		excluded: excluded,
	}
}

func (e *Exclusion) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	// Get all relations from the main set
	mainSeq, err := e.mainSet.CheckImpl(ctx, resources, subject)
	if err != nil {
		return nil, err
	}

	mainRels, err := CollectAll(mainSeq)
	if err != nil {
		return nil, err
	}

	// If main set is empty, return empty result
	if len(mainRels) == 0 {
		return func(yield func(Relation, error) bool) {
			// Empty sequence - never yield anything
		}, nil
	}

	// Get all relations from the excluded set
	excludedSeq, err := e.excluded.CheckImpl(ctx, resources, subject)
	if err != nil {
		return nil, err
	}

	excludedRels, err := CollectAll(excludedSeq)
	if err != nil {
		return nil, err
	}

	// Filter main set by excluding relations that are in the excluded set
	return func(yield func(Relation, error) bool) {
		for _, mainRel := range mainRels {
			found := false

			// Check if this relation exists in the excluded set (only compare endpoints: resource and subject object types/IDs)
			for _, excludedRel := range excludedRels {
				if GetObject(mainRel.Resource).Equals(GetObject(excludedRel.Resource)) &&
					GetObject(mainRel.Subject).Equals(GetObject(excludedRel.Subject)) {
					found = true
					break
				}
			}

			// Only yield if not found in excluded set
			if !found {
				if !yield(mainRel, nil) {
					return
				}
			}
		}
	}, nil
}

func (e *Exclusion) IterSubjectsImpl(ctx *Context, resource Object) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (e *Exclusion) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (e *Exclusion) Clone() Iterator {
	return &Exclusion{
		mainSet:  e.mainSet.Clone(),
		excluded: e.excluded.Clone(),
	}
}

func (e *Exclusion) Explain() Explain {
	return Explain{
		Info: "Exclusion",
		SubExplain: []Explain{
			e.mainSet.Explain(),
			e.excluded.Explain(),
		},
	}
}
