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
	return func(yield func(*Path, error) bool) {
		var finalResults []*Path
		for _, resource := range resources {
			ctx.TraceStep(ia, "processing resource %s:%s", resource.ObjectType, resource.ObjectID)

			subit, err := ctx.IterSubjects(ia.left, resource)
			if err != nil {
				yield(nil, err)
				return
			}

			// For intersection arrow, we need to track:
			// 1. All left subjects that actually exist (after caveat evaluation)
			// 2. Which ones satisfy the right condition
			// 3. Only yield results if ALL existing left subjects satisfy the right condition
			// 4. Combine all left-side caveats with AND logic

			var validResults []*Path
			var leftSideCaveats []*core.CaveatExpression
			leftSubjectCount := 0
			satisfiedCount := 0

			for path, err := range subit {
				if err != nil {
					yield(nil, err)
					return
				}

				// This is a valid left subject (passed its own caveats)
				leftSubjectCount++
				ctx.TraceStep(ia, "found left subject %s:%s (caveat: %v)",
					path.Subject.ObjectType, path.Subject.ObjectID, path.Caveat != nil)

				// Collect left-side caveats for AND combination
				if path.Caveat != nil {
					ctx.TraceStep(ia, "left subject %s:%s has caveat",
						path.Subject.ObjectType, path.Subject.ObjectID)
					leftSideCaveats = append(leftSideCaveats, path.Caveat)
				}

				checkResources := []Object{GetObject(path.Subject)}
				checkit, err := ctx.Check(ia.right, checkResources, subject)
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

					ctx.TraceStep(ia, "left subject %s:%s satisfied right condition",
						path.Subject.ObjectType, path.Subject.ObjectID)

					combinedPath := &Path{
						Resource:   path.Resource,
						Relation:   path.Relation,
						Subject:    checkPath.Subject,
						Caveat:     checkPath.Caveat, // Will be combined with left-side caveats later
						Expiration: checkPath.Expiration,
						Integrity:  checkPath.Integrity,
						Metadata:   checkPath.Metadata,
					}
					validResults = append(validResults, combinedPath)
					break // Only need one match per left subject for intersection logic
				}

				if rightResultCount == 0 {
					ctx.TraceStep(ia, "left subject %s:%s did NOT satisfy right condition",
						path.Subject.ObjectType, path.Subject.ObjectID)
				}

				// If this left subject doesn't satisfy the right condition, break early
				// since intersection requires ALL subjects to satisfy
				if !rightSatisfied {
					ctx.TraceStep(ia, "intersection FAILED - not all subjects satisfied (got %d/%d)",
						satisfiedCount, leftSubjectCount)
					// Early exit - not all subjects satisfied, so intersection fails
					leftSubjectCount = -1 // Signal failure
					break
				}
			}

			ctx.TraceStep(ia, "intersection check complete - %d left subjects, %d satisfied",
				leftSubjectCount, satisfiedCount)

			// Only yield results if ALL existing left subjects were satisfied
			// (leftSubjectCount > 0 means there were subjects, satisfiedCount == leftSubjectCount means all satisfied)
			if leftSubjectCount > 0 && satisfiedCount == leftSubjectCount {
				ctx.TraceStep(ia, "intersection SUCCESS - combining %d left-side caveats", len(leftSideCaveats))

				// Combine all left-side caveats with AND logic
				// We need to create a single caveat expression that represents all left-side constraints
				var combinedLeftCaveat *core.CaveatExpression
				if len(leftSideCaveats) > 0 {
					// Build the combined expression using the existing AND logic
					var leftExpr *core.CaveatExpression
					for i, caveatExpr := range leftSideCaveats {
						if i == 0 {
							leftExpr = caveatExpr
						} else {
							leftExpr = caveats.And(leftExpr, caveatExpr)
						}
					}
					combinedLeftCaveat = leftExpr
				}

				// For intersection semantics, we need to create a single path that represents
				// the AND of all individual conditions: each (leftCaveat AND rightCaveat) must be AND'd together
				var intersectionCaveat *core.CaveatExpression

				for i, result := range validResults {
					// Combine this result's left and right caveats
					var resultCaveat *core.CaveatExpression
					if combinedLeftCaveat != nil && result.Caveat != nil {
						resultCaveat = caveats.And(combinedLeftCaveat, result.Caveat)
					} else if combinedLeftCaveat != nil {
						resultCaveat = combinedLeftCaveat
					} else if result.Caveat != nil {
						resultCaveat = result.Caveat
					}

					// For intersection (ALL), we AND each result's combined caveat
					if i == 0 {
						intersectionCaveat = resultCaveat
					} else if resultCaveat != nil {
						if intersectionCaveat != nil {
							intersectionCaveat = caveats.And(intersectionCaveat, resultCaveat)
						} else {
							intersectionCaveat = resultCaveat
						}
					}
				}

				// Return a single path representing the intersection
				if len(validResults) > 0 {
					firstResult := validResults[0]
					finalResult := &Path{
						Resource:   firstResult.Resource,
						Relation:   firstResult.Relation,
						Subject:    firstResult.Subject,
						Caveat:     intersectionCaveat,
						Expiration: firstResult.Expiration,
						Integrity:  firstResult.Integrity,
						Metadata:   firstResult.Metadata,
					}

					finalResults = append(finalResults, finalResult)
					if !yield(finalResult, nil) {
						return
					}
				}
			} else {
				ctx.TraceStep(ia, "intersection FAILED - no results yielded")
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
		Name:       "IntersectionArrow",
		Info:       "IntersectionArrow",
		SubExplain: []Explain{ia.left.Explain(), ia.right.Explain()},
	}
}
