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

func (e *Exclusion) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// Get all paths from the main set
	mainSeq, err := e.mainSet.CheckImpl(ctx, resources, subject)
	if err != nil {
		return nil, err
	}

	mainPaths, err := CollectAll(mainSeq)
	if err != nil {
		return nil, err
	}

	// If main set is empty, return empty result
	if len(mainPaths) == 0 {
		return EmptyPathSeq(), nil
	}

	// Get all paths from the excluded set
	excludedSeq, err := e.excluded.CheckImpl(ctx, resources, subject)
	if err != nil {
		return nil, err
	}

	excludedPaths, err := CollectAll(excludedSeq)
	if err != nil {
		return nil, err
	}

	// Filter main set by excluding paths that are in the excluded set
	return func(yield func(*Path, error) bool) {
		for _, mainPath := range mainPaths {
			found := false

			// Check if this path exists in the excluded set (only compare endpoints: resource and subject object types/IDs)
			for _, excludedPath := range excludedPaths {
				if mainPath.Resource.Equals(excludedPath.Resource) &&
					GetObject(mainPath.Subject).Equals(GetObject(excludedPath.Subject)) {
					found = true
					break
				}
			}

			// Only yield if not found in excluded set
			if !found {
				if !yield(mainPath, nil) {
					return
				}
			}
		}
	}, nil
}

func (e *Exclusion) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (e *Exclusion) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
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
