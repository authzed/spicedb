package query

import (
	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/tuple"
)

// ExclusionIterator represents the set of relations that are in the mainSet but not in the excluded set.
// This is equivalent to `permission foo = bar - baz`
type ExclusionIterator struct {
	mainSet      Iterator
	excluded     Iterator
	canonicalKey CanonicalKey
}

var _ Iterator = &ExclusionIterator{}

func NewExclusionIterator(mainSet, excluded Iterator) *ExclusionIterator {
	return &ExclusionIterator{
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
// Mutates mainPath in place and returns (mainPath, shouldInclude).
func combineExclusionCaveats(mainPath, excludedPath *Path) (*Path, bool) {
	// Case 1: Main has caveat, excluded has no caveat
	// Since excluded always applies (no conditions), main is completely excluded
	if mainPath.Caveat != nil && excludedPath.Caveat == nil {
		return nil, false // Completely excluded
	}

	// Case 2: Main has no caveat, excluded has no caveat
	// Simple exclusion - excluded always applies, so main is completely excluded
	if mainPath.Caveat == nil && excludedPath.Caveat == nil {
		return nil, false // Completely excluded
	}

	// Case 3: Main has no caveat, excluded has caveat
	// Main applies unconditionally, excluded applies conditionally
	// Result: main path with caveat NOT(excluded_caveat)
	if mainPath.Caveat == nil && excludedPath.Caveat != nil {
		// Mutate main path with negated excluded caveat in place
		// This represents "main applies when excluded caveat is false"
		mainPath.Caveat = caveats.Invert(excludedPath.Caveat)
		return mainPath, true
	}

	// Case 4: Main has caveat, excluded has caveat
	// Result should be: main_caveat AND NOT(excluded_caveat)
	if mainPath.Caveat != nil && excludedPath.Caveat != nil {
		// Mutate main path with combined caveat: main_caveat AND NOT(excluded_caveat)
		mainPath.Caveat = caveats.And(mainPath.Caveat, caveats.Invert(excludedPath.Caveat))
		return mainPath, true
	}

	return mainPath, true
}

func (e *ExclusionIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	// Get the excluded path first
	if ctx.shouldTrace() {
		ctx.TraceStep(e, "getting path from excluded set for resource %s:%s", resource.ObjectType, resource.ObjectID)
	}
	excludedPath, err := ctx.Check(e.excluded, resource, subject)
	if err != nil {
		return nil, err
	}

	if ctx.shouldTrace() {
		if excludedPath != nil {
			ctx.TraceStep(e, "excluded set matched")
		} else {
			ctx.TraceStep(e, "excluded set: no match")
		}
	}

	// Get the main path
	if ctx.shouldTrace() {
		ctx.TraceStep(e, "getting path from main set")
	}
	mainPath, err := ctx.Check(e.mainSet, resource, subject)
	if err != nil {
		return nil, err
	}

	if mainPath == nil {
		if ctx.shouldTrace() {
			ctx.TraceStep(e, "main set: no match, returning nil")
		}
		return nil, nil
	}

	if excludedPath == nil {
		// Nothing to subtract
		if ctx.shouldTrace() {
			ctx.TraceStep(e, "main matched, nothing excluded, returning main path")
		}
		return mainPath, nil
	}

	// Both matched: apply exclusion caveat logic
	if ctx.shouldTrace() {
		ctx.TraceStep(e, "both matched, applying exclusion caveat logic")
	}
	resultPath, shouldInclude := combineExclusionCaveats(mainPath, excludedPath)
	if !shouldInclude {
		if ctx.shouldTrace() {
			ctx.TraceStep(e, "path completely excluded")
		}
		return nil, nil
	}
	return resultPath, nil
}

func (e *ExclusionIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	// Get all subjects from the excluded set first and build a lookup map
	if ctx.shouldTrace() {
		ctx.TraceStep(e, "getting subjects from excluded set for resource %s:%s", resource.ObjectType, resource.ObjectID)
	}
	excludedSeq, err := ctx.IterSubjects(e.excluded, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}

	excludedPaths, err := CollectAll(excludedSeq)
	if err != nil {
		return nil, err
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(e, "excluded set returned %d paths", len(excludedPaths))
	}

	// Build a map for O(1) lookup: key is subject key.
	// Extract the wildcard entry separately — it acts as a "default excluder"
	// that applies to all concrete subjects in the main set.
	var excludedWildcard *Path
	excludedMap := make(map[string]*Path, len(excludedPaths))
	for _, excludedPath := range excludedPaths {
		if excludedPath.Subject.ObjectID == tuple.PublicWildcard {
			excludedWildcard = excludedPath
			continue
		}
		key := ObjectAndRelationKey(excludedPath.Subject)
		excludedMap[key] = excludedPath
	}

	// Get the main sequence (this catches immediate errors from main set's IterSubjectsImpl)
	if ctx.shouldTrace() {
		ctx.TraceStep(e, "getting sequence from main set")
	}
	mainSeq, err := ctx.IterSubjects(e.mainSet, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}

	// Stream the main set and yield non-excluded subjects immediately
	return func(yield func(*Path, error) bool) {
		if ctx.shouldTrace() {
			ctx.TraceStep(e, "streaming subjects from main set")
		}
		mainCount := 0
		yieldedCount := 0
		for mainPath, err := range mainSeq {
			if err != nil {
				yield(nil, err)
				return
			}
			mainCount++

			if mainPath.Subject.ObjectID == tuple.PublicWildcard {
				if excludedWildcard != nil {
					// Both sides have wildcards. The wildcards cancel, but subjects
					// that were excluded from the excluded wildcard "escape" back into
					// the result. For example: viewer:* - (banned:* except sarah) → sarah.
					resultPath, shouldInclude := combineExclusionCaveats(mainPath, excludedWildcard)
					if shouldInclude {
						// Caveated wildcard survives — yield it with tracked exclusions.
						resultPath.ExcludedSubjects = collectExcludedSubjects(excludedMap, mainPath.ExcludedSubjects)
						yieldedCount++
						if !yield(resultPath, nil) {
							return
						}
					}

					// Yield concrete subjects from the excluded wildcard's ExcludedSubjects —
					// these subjects "escaped" the inner exclusion and appear in the result.
					for _, escaped := range excludedWildcard.ExcludedSubjects {
						// The escaped subject is in the main set (via wildcard) and NOT
						// in the excluded set (it was excluded from the exclusion).
						// Combine caveats: main wildcard's caveat AND escaped's caveat.
						synth := *escaped
						synth.Caveat = caveats.And(mainPath.Caveat, escaped.Caveat)
						yieldedCount++
						if !yield(&synth, nil) {
							return
						}
					}
				} else {
					// Main has wildcard, excluded has only concrete subjects.
					// The wildcard passes through; track which concrete subjects were excluded.
					mainPath.ExcludedSubjects = collectExcludedSubjects(excludedMap, mainPath.ExcludedSubjects)
					yieldedCount++
					if !yield(mainPath, nil) {
						return
					}
				}
				continue
			}

			// Check if this concrete subject has a specific exclusion.
			key := ObjectAndRelationKey(mainPath.Subject)
			if excludedPath, found := excludedMap[key]; found {
				if ctx.shouldTrace() {
					ctx.TraceStep(e, "found matching excluded subject, combining caveats")
				}
				resultPath, shouldInclude := combineExclusionCaveats(mainPath, excludedPath)
				if shouldInclude {
					yieldedCount++
					if !yield(resultPath, nil) {
						return
					}
				} else if ctx.shouldTrace() {
					ctx.TraceStep(e, "subject completely excluded")
				}
				continue
			}

			// If the excluded set has a wildcard, it excludes ALL concrete subjects too.
			if excludedWildcard != nil {
				resultPath, shouldInclude := combineExclusionCaveats(mainPath, excludedWildcard)
				if shouldInclude {
					yieldedCount++
					if !yield(resultPath, nil) {
						return
					}
				} else if ctx.shouldTrace() {
					ctx.TraceStep(e, "subject excluded by wildcard")
				}
				continue
			}

			// No exclusion applies, yield as-is
			yieldedCount++
			if !yield(mainPath, nil) {
				return
			}
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(e, "exclusion completed: %d main subjects, %d yielded", mainCount, yieldedCount)
		}
	}, nil
}

// collectExcludedSubjects builds the ExcludedSubjects list from the excluded map,
// merging with any previously tracked exclusions.
func collectExcludedSubjects(excludedMap map[string]*Path, existing []*Path) []*Path {
	if len(excludedMap) == 0 && len(existing) == 0 {
		return nil
	}
	result := make([]*Path, 0, len(excludedMap)+len(existing))
	result = append(result, existing...)
	for _, p := range excludedMap {
		result = append(result, p)
	}
	return result
}

func (e *ExclusionIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	// Get all resources from the excluded set first and build a lookup map
	if ctx.shouldTrace() {
		ctx.TraceStep(e, "getting resources from excluded set for subject %s:%s", subject.ObjectType, subject.ObjectID)
	}
	excludedSeq, err := ctx.IterResources(e.excluded, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	excludedPaths, err := CollectAll(excludedSeq)
	if err != nil {
		return nil, err
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(e, "excluded set returned %d paths", len(excludedPaths))
	}

	// Build a map for O(1) lookup: key is resource key
	excludedMap := make(map[string]*Path, len(excludedPaths))
	for _, excludedPath := range excludedPaths {
		key := excludedPath.Resource.Key()
		excludedMap[key] = excludedPath
	}

	// Get the main sequence (this catches immediate errors from main set's IterResourcesImpl)
	if ctx.shouldTrace() {
		ctx.TraceStep(e, "getting sequence from main set")
	}
	mainSeq, err := ctx.IterResources(e.mainSet, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	// Stream the main set and yield non-excluded subjects immediately
	return func(yield func(*Path, error) bool) {
		if ctx.shouldTrace() {
			ctx.TraceStep(e, "streaming resources from main set")
		}
		mainCount := 0
		yieldedCount := 0
		for mainPath, err := range mainSeq {
			if err != nil {
				yield(nil, err)
				return
			}
			mainCount++

			// Check if this resource exists in the excluded set
			key := mainPath.Resource.Key()
			if excludedPath, found := excludedMap[key]; found {
				// Found matching resource in excluded set - combine caveats
				if ctx.shouldTrace() {
					ctx.TraceStep(e, "found matching excluded resource, combining caveats")
				}
				resultPath, shouldInclude := combineExclusionCaveats(mainPath, excludedPath)
				if shouldInclude {
					yieldedCount++
					if !yield(resultPath, nil) {
						return
					}
				} else if ctx.shouldTrace() {
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

		if ctx.shouldTrace() {
			ctx.TraceStep(e, "exclusion completed: %d main resources, %d yielded", mainCount, yieldedCount)
		}
	}, nil
}

func (e *ExclusionIterator) Clone() Iterator {
	return &ExclusionIterator{
		canonicalKey: e.canonicalKey,
		mainSet:      e.mainSet.Clone(),
		excluded:     e.excluded.Clone(),
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
	return &ExclusionIterator{canonicalKey: e.canonicalKey, mainSet: newSubs[0], excluded: newSubs[1]}, nil
}

func (e *ExclusionIterator) CanonicalKey() CanonicalKey {
	return e.canonicalKey
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
