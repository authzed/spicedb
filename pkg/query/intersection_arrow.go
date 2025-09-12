package query

import (
	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// IntersectionArrow is an iterator that represents the set of relations that
// follow from a walk in the graph where ALL subjects on the left must satisfy
// the right side condition.
//
// Ex: `group.all(member)` - user must be member of ALL groups
type IntersectionArrow struct {
	left  Iterator
	right Iterator
}

var _ Iterator = &IntersectionArrow{}

func NewIntersectionArrow(left, right Iterator) *IntersectionArrow {
	return &IntersectionArrow{
		left:  left,
		right: right,
	}
}

func (ia *IntersectionArrow) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	ctx.TraceEnterIterator("IntersectionArrow", resources, subject)

	return func(yield func(*Path, error) bool) {
		var finalResults []*Path
		defer func() {
			ctx.TraceExitIterator("IntersectionArrow", finalResults)
		}()

		for _, resource := range resources {
			ctx.TraceStep("IntersectionArrow", "processing resource %s:%s", resource.ObjectType, resource.ObjectID)

			subit, err := ia.left.IterSubjectsImpl(ctx, resource)
			if err != nil {
				yield(nil, err)
				return
			}

			// For intersection arrow, we need to track:
			// 1. All left subjects that actually exist (after caveat evaluation)
			// 2. Which ones satisfy the right condition
			// 3. Only yield results if ALL existing left subjects satisfy the right condition

			var validResults []*Path
			leftSubjectCount := 0
			satisfiedCount := 0

			for path, err := range subit {
				if err != nil {
					yield(nil, err)
					return
				}

				// This is a valid left subject (passed its own caveats)
				leftSubjectCount++
				ctx.TraceStep("IntersectionArrow", "found left subject %s:%s (caveat: %v)",
					path.Subject.ObjectType, path.Subject.ObjectID, path.Caveat != nil)

				checkResources := []Object{GetObject(path.Subject)}
				checkit, err := ia.right.CheckImpl(ctx, checkResources, subject)
				if err != nil {
					yield(nil, err)
					return
				}

				// Check if this left subject satisfies the right condition
				rightSatisfied := false
				rightResultCount := 0
				for checkPath, err := range checkit {
					if err != nil {
						yield(nil, err)
						return
					}

					rightResultCount++
					// This left subject satisfies the right condition
					rightSatisfied = true
					satisfiedCount++

					ctx.TraceStep("IntersectionArrow", "left subject %s:%s satisfied right condition",
						path.Subject.ObjectType, path.Subject.ObjectID)

					// Combine caveats from both sides (AND logic)
					var combinedCaveat *core.CaveatExpression
					if path.Caveat != nil && checkPath.Caveat != nil {
						combinedCaveat = caveats.And(path.Caveat, checkPath.Caveat)
					} else if path.Caveat != nil {
						combinedCaveat = path.Caveat
					} else if checkPath.Caveat != nil {
						combinedCaveat = checkPath.Caveat
					}

					combinedPath := &Path{
						Resource:   path.Resource,
						Relation:   path.Relation,
						Subject:    checkPath.Subject,
						Caveat:     combinedCaveat,
						Expiration: checkPath.Expiration,
						Integrity:  checkPath.Integrity,
						Metadata:   make(map[string]any),
					}
					validResults = append(validResults, combinedPath)
					break // Only need one match per left subject for intersection logic
				}

				if rightResultCount == 0 {
					ctx.TraceStep("IntersectionArrow", "left subject %s:%s did NOT satisfy right condition",
						path.Subject.ObjectType, path.Subject.ObjectID)
				}

				// If this left subject doesn't satisfy the right condition, break early
				// since intersection requires ALL subjects to satisfy
				if !rightSatisfied {
					ctx.TraceStep("IntersectionArrow", "intersection FAILED - not all subjects satisfied (got %d/%d)",
						satisfiedCount, leftSubjectCount)
					// Early exit - not all subjects satisfied, so intersection fails
					leftSubjectCount = -1 // Signal failure
					break
				}
			}

			ctx.TraceStep("IntersectionArrow", "intersection check complete - %d left subjects, %d satisfied",
				leftSubjectCount, satisfiedCount)

			// Only yield results if ALL existing left subjects were satisfied
			// (leftSubjectCount > 0 means there were subjects, satisfiedCount == leftSubjectCount means all satisfied)
			if leftSubjectCount > 0 && satisfiedCount == leftSubjectCount {
				ctx.TraceStep("IntersectionArrow", "intersection SUCCESS - yielding %d results", len(validResults))
				for _, result := range validResults {
					finalResults = append(finalResults, result)
					if !yield(result, nil) {
						return
					}
				}
			} else {
				ctx.TraceStep("IntersectionArrow", "intersection FAILED - no results yielded")
			}
		}
	}, nil
}

func (ia *IntersectionArrow) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (ia *IntersectionArrow) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (ia *IntersectionArrow) Clone() Iterator {
	return &IntersectionArrow{
		left:  ia.left.Clone(),
		right: ia.right.Clone(),
	}
}

func (ia *IntersectionArrow) Explain() Explain {
	return Explain{
		Info:       "IntersectionArrow",
		SubExplain: []Explain{ia.left.Explain(), ia.right.Explain()},
	}
}
