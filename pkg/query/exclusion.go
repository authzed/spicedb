package query

import (
	"github.com/google/uuid"

	"github.com/authzed/spicedb/internal/caveats"
)

// ExclusionIterator represents the set of relations that are in the mainSet but not in the excluded set.
// This is equivalent to `permission foo = bar - baz`
type ExclusionIterator struct {
	id           string
	mainSet      Iterator
	excluded     Iterator
	canonicalKey CanonicalKey
}

var _ Iterator = &ExclusionIterator{}

func NewExclusionIterator(mainSet, excluded Iterator) *ExclusionIterator {
	return &ExclusionIterator{
		id:       uuid.NewString(),
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

func (e *ExclusionIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// Get all paths from the excluded set first and build a lookup map
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

	// Build a map for O(1) lookup: key is "resourceKey:subjectKey"
	excludedMap := make(map[string]Path, len(excludedPaths))
	for _, excludedPath := range excludedPaths {
		key := excludedPath.Resource.Key() + ":" + GetObject(excludedPath.Subject).Key()
		excludedMap[key] = excludedPath
	}

	// Get the main sequence (this catches immediate errors from main set's CheckImpl)
	ctx.TraceStep(e, "getting sequence from main set")
	mainSeq, err := ctx.Check(e.mainSet, resources, subject)
	if err != nil {
		return nil, err
	}

	// Stream the main set and yield non-excluded paths immediately
	return func(yield func(Path, error) bool) {
		ctx.TraceStep(e, "streaming paths from main set")
		mainCount := 0
		yieldedCount := 0
		for mainPath, err := range mainSeq {
			if err != nil {
				yield(Path{}, err)
				return
			}
			mainCount++

			// Check if this path exists in the excluded set
			key := mainPath.Resource.Key() + ":" + GetObject(mainPath.Subject).Key()
			if excludedPath, found := excludedMap[key]; found {
				// Found matching path in excluded set - combine caveats
				ctx.TraceStep(e, "found matching excluded path, combining caveats")
				resultPath, shouldInclude := combineExclusionCaveats(mainPath, excludedPath)
				if shouldInclude {
					yieldedCount++
					if !yield(resultPath, nil) {
						return
					}
				} else {
					ctx.TraceStep(e, "path completely excluded")
				}
			} else {
				// No exclusion, yield as-is
				yieldedCount++
				if !yield(mainPath, nil) {
					return
				}
			}
		}

		ctx.TraceStep(e, "exclusion completed: %d main paths, %d yielded", mainCount, yieldedCount)
	}, nil
}

func (e *ExclusionIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	// Get all subjects from the excluded set first and build a lookup map
	ctx.TraceStep(e, "getting subjects from excluded set for resource %s:%s", resource.ObjectType, resource.ObjectID)
	excludedSeq, err := ctx.IterSubjects(e.excluded, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}

	excludedPaths, err := CollectAll(excludedSeq)
	if err != nil {
		return nil, err
	}

	ctx.TraceStep(e, "excluded set returned %d paths", len(excludedPaths))

	// Build a map for O(1) lookup: key is subject key
	excludedMap := make(map[string]Path, len(excludedPaths))
	for _, excludedPath := range excludedPaths {
		key := ObjectAndRelationKey(excludedPath.Subject)
		excludedMap[key] = excludedPath
	}

	// Get the main sequence (this catches immediate errors from main set's IterSubjectsImpl)
	ctx.TraceStep(e, "getting sequence from main set")
	mainSeq, err := ctx.IterSubjects(e.mainSet, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}

	// Stream the main set and yield non-excluded subjects immediately
	return func(yield func(Path, error) bool) {
		ctx.TraceStep(e, "streaming subjects from main set")
		mainCount := 0
		yieldedCount := 0
		for mainPath, err := range mainSeq {
			if err != nil {
				yield(Path{}, err)
				return
			}
			mainCount++

			// Check if this subject exists in the excluded set
			key := ObjectAndRelationKey(mainPath.Subject)
			if excludedPath, found := excludedMap[key]; found {
				// Found matching subject in excluded set - combine caveats
				ctx.TraceStep(e, "found matching excluded subject, combining caveats")
				resultPath, shouldInclude := combineExclusionCaveats(mainPath, excludedPath)
				if shouldInclude {
					yieldedCount++
					if !yield(resultPath, nil) {
						return
					}
				} else {
					ctx.TraceStep(e, "subject completely excluded")
				}
			} else {
				// No exclusion, yield as-is
				yieldedCount++
				if !yield(mainPath, nil) {
					return
				}
			}
		}

		ctx.TraceStep(e, "exclusion completed: %d main subjects, %d yielded", mainCount, yieldedCount)
	}, nil
}

func (e *ExclusionIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	// Get all resources from the excluded set first and build a lookup map
	ctx.TraceStep(e, "getting resources from excluded set for subject %s:%s", subject.ObjectType, subject.ObjectID)
	excludedSeq, err := ctx.IterResources(e.excluded, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	excludedPaths, err := CollectAll(excludedSeq)
	if err != nil {
		return nil, err
	}

	ctx.TraceStep(e, "excluded set returned %d paths", len(excludedPaths))

	// Build a map for O(1) lookup: key is resource key
	excludedMap := make(map[string]Path, len(excludedPaths))
	for _, excludedPath := range excludedPaths {
		key := excludedPath.Resource.Key()
		excludedMap[key] = excludedPath
	}

	// Get the main sequence (this catches immediate errors from main set's IterResourcesImpl)
	ctx.TraceStep(e, "getting sequence from main set")
	mainSeq, err := ctx.IterResources(e.mainSet, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	// Stream the main set and yield non-excluded subjects immediately
	return func(yield func(Path, error) bool) {
		ctx.TraceStep(e, "streaming resources from main set")
		mainCount := 0
		yieldedCount := 0
		for mainPath, err := range mainSeq {
			if err != nil {
				yield(Path{}, err)
				return
			}
			mainCount++

			// Check if this resource exists in the excluded set
			key := mainPath.Resource.Key()
			if excludedPath, found := excludedMap[key]; found {
				// Found matching resource in excluded set - combine caveats
				ctx.TraceStep(e, "found matching excluded resource, combining caveats")
				resultPath, shouldInclude := combineExclusionCaveats(mainPath, excludedPath)
				if shouldInclude {
					yieldedCount++
					if !yield(resultPath, nil) {
						return
					}
				} else {
					ctx.TraceStep(e, "resource completely excluded")
				}
			} else {
				// No exclusion, yield as-is
				yieldedCount++
				if !yield(mainPath, nil) {
					return
				}
			}
		}

		ctx.TraceStep(e, "exclusion completed: %d main resources, %d yielded", mainCount, yieldedCount)
	}, nil
}

func (e *ExclusionIterator) Clone() Iterator {
	return &ExclusionIterator{
		id:       uuid.NewString(),
		mainSet:  e.mainSet.Clone(),
		excluded: e.excluded.Clone(),
	}
}

func (e *ExclusionIterator) Explain() Explain {
	return Explain{
		Name: "Exclusion",
		Info: "Exclusion",
		SubExplain: []Explain{
			e.mainSet.Explain(),
			e.excluded.Explain(),
		},
	}
}

func (e *ExclusionIterator) Subiterators() []Iterator {
	return []Iterator{e.mainSet, e.excluded}
}

func (e *ExclusionIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &ExclusionIterator{id: uuid.NewString(), mainSet: newSubs[0], excluded: newSubs[1]}, nil
}

func (e *ExclusionIterator) ID() string {
	return e.id
}

func (e *ExclusionIterator) ResourceType() ([]ObjectType, error) {
	// Exclusion's resources come from the main set
	return e.mainSet.ResourceType()
}

func (e *ExclusionIterator) SubjectTypes() ([]ObjectType, error) {
	// Exclusion's subjects come from the main set only
	// (excluded set is subtracted, doesn't add new types)
	return e.mainSet.SubjectTypes()
}
