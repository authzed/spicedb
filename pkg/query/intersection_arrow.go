package query

import (
	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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

func (ia *IntersectionArrow) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	ctx.TraceEnterIterator("IntersectionArrow", resources, subject)
	
	return func(yield func(Relation, error) bool) {
		var finalResults []Relation
		defer func() {
			ctx.TraceExitIterator("IntersectionArrow", finalResults)
		}()
		
		for _, resource := range resources {
			ctx.TraceStep("IntersectionArrow", "processing resource %s:%s", resource.ObjectType, resource.ObjectID)
			
			subit, err := ia.left.IterSubjectsImpl(ctx, resource)
			if err != nil {
				yield(Relation{}, err)
				return
			}
			
			// For intersection arrow, we need to track:
			// 1. All left subjects that actually exist (after caveat evaluation)
			// 2. Which ones satisfy the right condition
			// 3. Only yield results if ALL existing left subjects satisfy the right condition
			
			var validResults []Relation
			leftSubjectCount := 0
			satisfiedCount := 0
			
			for rel, err := range subit {
				if err != nil {
					yield(Relation{}, err)
					return
				}
				
				// This is a valid left subject (passed its own caveats)
				leftSubjectCount++
				ctx.TraceStep("IntersectionArrow", "found left subject %s:%s (caveat: %v)", 
					rel.Subject.ObjectType, rel.Subject.ObjectID, rel.OptionalCaveat != nil)
				
				checkResources := []Object{GetObject(rel.Subject)}
				checkit, err := ia.right.CheckImpl(ctx, checkResources, subject)
				if err != nil {
					yield(Relation{}, err)
					return
				}
				
				// Check if this left subject satisfies the right condition
				rightSatisfied := false
				rightResultCount := 0
				for checkrel, err := range checkit {
					if err != nil {
						yield(Relation{}, err)
						return
					}
					
					rightResultCount++
					// This left subject satisfies the right condition
					rightSatisfied = true
					satisfiedCount++
					
					ctx.TraceStep("IntersectionArrow", "left subject %s:%s satisfied right condition", 
						rel.Subject.ObjectType, rel.Subject.ObjectID)
					
					// Combine caveats from both sides (AND logic)
					var combinedCaveat *core.ContextualizedCaveat
					if rel.OptionalCaveat != nil && checkrel.OptionalCaveat != nil {
						leftExpr := caveats.CaveatAsExpr(rel.OptionalCaveat)
						rightExpr := caveats.CaveatAsExpr(checkrel.OptionalCaveat)
						combinedExpr := caveats.And(leftExpr, rightExpr)
						if combinedExpr != nil && combinedExpr.GetCaveat() != nil {
							combinedCaveat = combinedExpr.GetCaveat()
						} else {
							combinedCaveat = rel.OptionalCaveat
						}
					} else if rel.OptionalCaveat != nil {
						combinedCaveat = rel.OptionalCaveat
					} else if checkrel.OptionalCaveat != nil {
						combinedCaveat = checkrel.OptionalCaveat
					}
					
					combinedrel := Relation{
						OptionalCaveat:     combinedCaveat,
						OptionalExpiration: checkrel.OptionalExpiration,
						OptionalIntegrity:  checkrel.OptionalIntegrity,
						RelationshipReference: tuple.RelationshipReference{
							Resource: rel.Resource,
							Subject:  checkrel.Subject,
						},
					}
					validResults = append(validResults, combinedrel)
					break // Only need one match per left subject for intersection logic
				}
				
				if rightResultCount == 0 {
					ctx.TraceStep("IntersectionArrow", "left subject %s:%s did NOT satisfy right condition", 
						rel.Subject.ObjectType, rel.Subject.ObjectID)
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

func (ia *IntersectionArrow) IterSubjectsImpl(ctx *Context, resource Object) (RelationSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (ia *IntersectionArrow) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (RelationSeq, error) {
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