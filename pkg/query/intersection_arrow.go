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
	return func(yield func(Path, error) bool) {
		for _, resource := range resources {
			ctx.TraceStep(ia, "processing resource %s:%s", resource.ObjectType, resource.ObjectID)

			subit, err := ctx.IterSubjects(ia.left, resource)
			if err != nil {
				yield(Path{}, err)
				return
			}

			// For intersection arrow, we need to track:
			// 1. All left subjects that actually exist
			// 2. Which ones satisfy the right condition
			// 3. Only yield results if ALL existing left subjects satisfy the right condition
			// 4. Combine all (leftCaveat AND rightCaveat) pairs with AND logic

			var validResults []Path
			unsatisfied := false

			for path, err := range subit {
				if err != nil {
					yield(Path{}, err)
					return
				}

				// Check if this left subject connects within the right side iterator
				checkResources := []Object{GetObject(path.Subject)}
				checkit, err := ctx.Check(ia.right, checkResources, subject)
				if err != nil {
					yield(Path{}, err)
					return
				}

				// There is only one possible result from this check.
				paths, err := CollectAll(checkit)
				if err != nil {
					yield(Path{}, err)
					return
				}
				if len(paths) == 0 {
					ctx.TraceStep(ia, "left subject %s:%s did NOT connect on the right side",
						path.Subject.ObjectType, path.Subject.ObjectID)
					unsatisfied = true
					break
				}
				checkPath := paths[0]
				ctx.TraceStep(ia, "left subject %s:%s connects with the right side",
					path.Subject.ObjectType, path.Subject.ObjectID)

				// Combine this path's left caveat with the right caveat
				combinedCaveat := caveats.And(path.Caveat, checkPath.Caveat)

				combinedPath := Path{
					Resource:   path.Resource,
					Relation:   path.Relation,
					Subject:    checkPath.Subject,
					Caveat:     combinedCaveat,
					Expiration: checkPath.Expiration,
					Integrity:  checkPath.Integrity,
					Metadata:   checkPath.Metadata,
				}
				validResults = append(validResults, combinedPath)
				// Only need one match per left subject for intersection logic
			}

			if unsatisfied {
				ctx.TraceStep(ia, "intersection FAILED - not all subjects satisfied")
				continue
			}

			ctx.TraceStep(ia, "intersection SUCCESS - combining %d results", len(validResults))

			// For intersection semantics, we need to create a single path that represents
			// the AND of all individual conditions: each (leftCaveat AND rightCaveat) must be AND'd together
			var intersectionCaveat *core.CaveatExpression

			for i, result := range validResults {
				// For intersection (ALL), we AND each result's combined caveat
				if i == 0 {
					intersectionCaveat = result.Caveat
				} else if result.Caveat != nil {
					if intersectionCaveat != nil {
						intersectionCaveat = caveats.And(intersectionCaveat, result.Caveat)
					} else {
						intersectionCaveat = result.Caveat
					}
				}
			}

			// Return a single path representing the intersection, if one exists.
			if len(validResults) > 0 {
				firstResult := validResults[0]
				finalResult := Path{
					Resource:   resource,
					Relation:   "",
					Subject:    subject,
					Caveat:     intersectionCaveat,
					Expiration: firstResult.Expiration,
					Integrity:  firstResult.Integrity,
					Metadata:   firstResult.Metadata,
				}

				if !yield(finalResult, nil) {
					return
				}
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

func (ia *IntersectionArrow) Subiterators() []Iterator {
	return []Iterator{ia.left, ia.right}
}

func (ia *IntersectionArrow) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &IntersectionArrow{left: newSubs[0], right: newSubs[1]}, nil
}
