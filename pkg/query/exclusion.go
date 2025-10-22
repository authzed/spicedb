package query

import (
	"github.com/authzed/spicedb/internal/caveats"
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

// combineExclusionCaveats combines caveats for exclusion operations (A - B logic)
// For exclusion: main path is included unless excluded path applies
// If main has caveat_a and excluded has caveat_b, result should be: caveat_a AND NOT caveat_b
// If main has no caveat and excluded has caveat_b, result should be: NOT caveat_b
// If main has caveat_a and excluded has no caveat, result should be completely excluded (return false)
// If neither has caveats, use simple exclusion logic
// Returns (path, shouldInclude) where shouldInclude indicates if the path should be included in results
func combineExclusionCaveats(mainPath, excludedPath Path) (Path, bool) {
	// Case 1: Main has caveat, excluded has no caveat
	// Since excluded always applies (no conditions), main is completely excluded
	if mainPath.Caveat != nil && excludedPath.Caveat == nil {
		return Path{}, false // Completely excluded
	}

	// Case 2: Main has no caveat, excluded has no caveat
	// Simple exclusion - excluded always applies, so main is completely excluded
	if mainPath.Caveat == nil && excludedPath.Caveat == nil {
		return Path{}, false // Completely excluded
	}

	// Case 3: Main has no caveat, excluded has caveat
	// Main applies unconditionally, excluded applies conditionally
	// Result: main path with caveat NOT(excluded_caveat)
	if mainPath.Caveat == nil && excludedPath.Caveat != nil {
		// Return main path with negated excluded caveat
		// This represents "main applies when excluded caveat is false"
		result := mainPath
		result.Caveat = caveats.Invert(excludedPath.Caveat)
		return result, true
	}

	// Case 4: Main has caveat, excluded has caveat
	// Result should be: main_caveat AND NOT(excluded_caveat)
	if mainPath.Caveat != nil && excludedPath.Caveat != nil {
		// Return main path with combined caveat: main_caveat AND NOT(excluded_caveat)
		result := mainPath
		result.Caveat = caveats.And(mainPath.Caveat, caveats.Invert(excludedPath.Caveat))
		return result, true
	}

	return mainPath, true
}

func (e *Exclusion) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// Get all paths from the main set
	ctx.TraceStep(e, "getting paths from main set")
	mainSeq, err := ctx.Check(e.mainSet, resources, subject)
	if err != nil {
		return nil, err
	}

	mainPaths, err := CollectAll(mainSeq)
	if err != nil {
		return nil, err
	}

	ctx.TraceStep(e, "main set returned %d paths", len(mainPaths))

	// If main set is empty, return empty result
	if len(mainPaths) == 0 {
		ctx.TraceStep(e, "main set empty, returning empty")
		return EmptyPathSeq(), nil
	}

	// Get all paths from the excluded set
	ctx.TraceStep(e, "getting paths from excluded set")
	excludedSeq, err := ctx.Check(e.excluded, resources, subject)
	if err != nil {
		return nil, err
	}

	excludedPaths, err := CollectAll(excludedSeq)
	if err != nil {
		return nil, err
	}

	ctx.TraceStep(e, "excluded set returned %d paths", len(excludedPaths))

	// Filter main set by excluding paths that are in the excluded set
	var finalPaths []Path
	for _, mainPath := range mainPaths {
		resultPath := mainPath
		shouldInclude := true

		// Check if this path exists in the excluded set
		for _, excludedPath := range excludedPaths {
			if mainPath.Resource.Equals(excludedPath.Resource) &&
				GetObject(mainPath.Subject).Equals(GetObject(excludedPath.Subject)) {
				// Found matching path in excluded set - combine caveats
				ctx.TraceStep(e, "found matching excluded path, combining caveats")
				resultPath, shouldInclude = combineExclusionCaveats(mainPath, excludedPath)
				break
			}
		}

		// Only include if path is not completely excluded
		if shouldInclude {
			finalPaths = append(finalPaths, resultPath)
		} else {
			ctx.TraceStep(e, "path completely excluded")
		}
	}

	return func(yield func(Path, error) bool) {
		for _, path := range finalPaths {
			if !yield(path, nil) {
				return
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
		Name: "Exclusion",
		Info: "Exclusion",
		SubExplain: []Explain{
			e.mainSet.Explain(),
			e.excluded.Explain(),
		},
	}
}

func (e *Exclusion) Subiterators() []Iterator {
	return []Iterator{e.mainSet, e.excluded}
}

func (e *Exclusion) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &Exclusion{mainSet: newSubs[0], excluded: newSubs[1]}, nil
}
