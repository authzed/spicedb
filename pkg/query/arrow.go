package query

import (
	"fmt"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// arrowDirection specifies which direction to execute the arrow check
type arrowDirection int

const (
	// leftToRight executes IterSubjects on left, Check on right
	leftToRight arrowDirection = iota
	// rightToLeft executes IterResources on right, Check on left
	rightToLeft
)

// ArrowIterator is an iterator that represents the set of paths that
// follow from a walk in the graph.
//
// Ex: `folder->owner` and `left->right`
type ArrowIterator struct {
	left          Iterator
	right         Iterator
	direction     arrowDirection // execution direction
	isSchemaArrow bool           // true for schema arrows (relation->permission), false for subrelation arrows
	canonicalKey  CanonicalKey
}

var _ Iterator = &ArrowIterator{}

func NewArrowIterator(left, right Iterator) *ArrowIterator {
	return &ArrowIterator{
		left:          left,
		right:         right,
		direction:     leftToRight,
		isSchemaArrow: false, // default to subrelation arrow
	}
}

func NewSchemaArrow(left, right Iterator) *ArrowIterator {
	return &ArrowIterator{
		left:          left,
		right:         right,
		direction:     leftToRight,
		isSchemaArrow: true,
	}
}

func (a *ArrowIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// There are three major strategies:
	// - IterSubjects on the left, Check on the right
	// - IterResources on the right, Check on the left
	// - IterSubjects on left, IterResources on right, and intersect the two iterators here (especially if they are known to be sorted)
	//
	// But for now, we cover the first two.
	switch a.direction {
	case leftToRight:
		return a.checkLeftToRight(ctx, resources, subject)
	case rightToLeft:
		return a.checkRightToLeft(ctx, resources, subject)
	default:
		return nil, spiceerrors.MustBugf("unknown arrow direction: %d", a.direction)
	}
}

// checkLeftToRight implements the left-to-right strategy:
// For each resource, IterSubjects on left, then Check on right
func (a *ArrowIterator) checkLeftToRight(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		ctx.TraceStep(a, "processing %d resources", len(resources))

		totalResultPaths := 0
		for resourceIdx, resource := range resources {
			ctx.TraceStep(a, "processing resource %d: %s:%s", resourceIdx, resource.ObjectType, resource.ObjectID)

			subit, err := ctx.IterSubjects(a.left, resource, NoObjectFilter())
			if err != nil {
				yield(Path{}, err)
				return
			}

			leftPathCount := 0
			for path, err := range subit {
				if err != nil {
					yield(Path{}, err)
					return
				}
				leftPathCount++

				checkResources := []Object{GetObject(path.Subject)}
				ctx.TraceStep(a, "checking right side for subject %s:%s", path.Subject.ObjectType, path.Subject.ObjectID)

				checkit, err := ctx.Check(a.right, checkResources, subject)
				if err != nil {
					yield(Path{}, err)
					return
				}

				// Process right check results and combine with left path
				count, ok := processLeftPathSequence(checkit, path, yield)
				totalResultPaths += count
				if !ok {
					return
				}

				ctx.TraceStep(a, "right side returned %d paths for subject %s:%s", count, path.Subject.ObjectType, path.Subject.ObjectID)
			}

			ctx.TraceStep(a, "left side returned %d paths for resource %s:%s", leftPathCount, resource.ObjectType, resource.ObjectID)
		}

		ctx.TraceStep(a, "arrow completed with %d total result paths", totalResultPaths)
	}, nil
}

// checkRightToLeft implements the right-to-left strategy:
// IterResources on right to get candidate resources, then Check on left
func (a *ArrowIterator) checkRightToLeft(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		ctx.TraceStep(a, "arrow check (right-to-left) with %d resources for subject %s:%s",
			len(resources), subject.ObjectType, subject.ObjectID)

		// Strategy: Start from the right side with the target subject
		// Get all resources that connect to subject on the right side
		rightSeq, err := ctx.IterResources(a.right, subject, NoObjectFilter())
		if err != nil {
			yield(Path{}, err)
			return
		}

		rightPathCount := 0
		totalResultPaths := 0
		for rightPath, err := range rightSeq {
			if err != nil {
				yield(Path{}, err)
				return
			}
			rightPathCount++

			// rightPath.Resource is an intermediate object from the right side
			// Now check if any of our input resources connect to this intermediate via left
			intermediateAsSubject := ObjectAndRelation{
				ObjectType: rightPath.Resource.ObjectType,
				ObjectID:   rightPath.Resource.ObjectID,
				Relation:   "",
			}

			leftSeq, err := ctx.Check(a.left, resources, intermediateAsSubject)
			if err != nil {
				yield(Path{}, err)
				return
			}

			// Process left check results and combine with right path
			count, ok := processRightPathSequence(leftSeq, rightPath, yield)
			totalResultPaths += count
			if !ok {
				return
			}

			ctx.TraceStep(a, "left side returned %d paths for intermediate %s:%s",
				count, intermediateAsSubject.ObjectType, intermediateAsSubject.ObjectID)
		}

		ctx.TraceStep(a, "arrow check (right-to-left) completed: %d right paths, %d total result paths",
			rightPathCount, totalResultPaths)
	}, nil
}

// processPathSequence is a helper that iterates a path sequence, combines each path with a fixed path,
// and yields the results. It handles error propagation and counting.
//
// Parameters:
//   - seq: the sequence to iterate over
//   - fixedPath: the path to combine with each iterated path
//   - isLeft: if true, fixedPath is the left side in combineArrowPaths; if false, it's the right side
//   - yield: the yield function to call for each combined path
//
// Returns (count, ok) where count is the number of paths processed and ok is false if iteration
// should stop (due to error or yield returning false), true otherwise.
func processPathSequence(
	seq PathSeq,
	fixedPath Path,
	isLeft bool,
	yield func(Path, error) bool,
) (int, bool) {
	count := 0
	for iteratedPath, err := range seq {
		if err != nil {
			yield(Path{}, err)
			return count, false
		}

		var combinedPath Path
		if isLeft {
			combinedPath = combineArrowPaths(fixedPath, iteratedPath)
		} else {
			combinedPath = combineArrowPaths(iteratedPath, fixedPath)
		}

		count++
		if !yield(combinedPath, nil) {
			return count, false
		}
	}
	return count, true
}

// processLeftPathSequence processes a sequence where the fixed path is on the left side.
// Returns (count, ok) where count is paths processed and ok indicates whether to continue.
func processLeftPathSequence(
	seq PathSeq,
	leftPath Path,
	yield func(Path, error) bool,
) (int, bool) {
	return processPathSequence(seq, leftPath, true, yield)
}

// processRightPathSequence processes a sequence where the fixed path is on the right side.
// Returns (count, ok) where count is paths processed and ok indicates whether to continue.
func processRightPathSequence(
	seq PathSeq,
	rightPath Path,
	yield func(Path, error) bool,
) (int, bool) {
	return processPathSequence(seq, rightPath, false, yield)
}

// combineArrowPaths combines a left path and right path into a single path for arrow operations.
// The combined path uses the resource and relation from the left path, the subject from the right path,
// and combines caveats from both sides using AND logic.
func combineArrowPaths(leftPath, rightPath Path) Path {
	// Combine caveats from both sides using AND logic
	var combinedCaveat *core.CaveatExpression
	switch {
	case leftPath.Caveat != nil && rightPath.Caveat != nil:
		combinedCaveat = caveats.And(leftPath.Caveat, rightPath.Caveat)
	case leftPath.Caveat != nil:
		combinedCaveat = leftPath.Caveat
	case rightPath.Caveat != nil:
		combinedCaveat = rightPath.Caveat
	}

	return Path{
		Resource:   leftPath.Resource,
		Relation:   leftPath.Relation,
		Subject:    rightPath.Subject,
		Caveat:     combinedCaveat,
		Expiration: combineExpiration(leftPath.Expiration, rightPath.Expiration),
		Integrity:  combineIntegrity(leftPath.Integrity, rightPath.Integrity),
		Metadata:   make(map[string]any),
	}
}

func (a *ArrowIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	// Arrow: resource -> left subjects -> right subjects
	// Get subjects from left side, then for each, get subjects from right side
	return func(yield func(Path, error) bool) {
		ctx.TraceStep(a, "iterating subjects for resource %s:%s", resource.ObjectType, resource.ObjectID)

		// Get all subjects from the left side
		leftSeq, err := ctx.IterSubjects(a.left, resource, NoObjectFilter())
		if err != nil {
			yield(Path{}, err)
			return
		}

		leftPathCount := 0
		totalResultPaths := 0
		for leftPath, err := range leftSeq {
			if err != nil {
				yield(Path{}, err)
				return
			}
			leftPathCount++

			// For each left subject, get subjects from right side
			leftSubjectAsResource := GetObject(leftPath.Subject)
			ctx.TraceStep(a, "iterating right side for left subject %s:%s", leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID)

			rightSeq, err := ctx.IterSubjects(a.right, leftSubjectAsResource, filterSubjectType)
			if err != nil {
				yield(Path{}, err)
				return
			}

			// Process right subjects and combine with left path
			count, ok := processLeftPathSequence(rightSeq, leftPath, yield)
			totalResultPaths += count
			if !ok {
				return
			}

			ctx.TraceStep(a, "right side returned %d paths for left subject %s:%s", count, leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID)
		}

		ctx.TraceStep(a, "arrow IterSubjects completed: %d left paths, %d total result paths", leftPathCount, totalResultPaths)
	}, nil
}

func (a *ArrowIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	// Arrow: resource -> left subjects -> right subjects
	// Get resources from right side, then for each, get resources from left side
	return func(yield func(Path, error) bool) {
		ctx.TraceStep(a, "iterating resources for subject %s:%s", subject.ObjectType, subject.ObjectID)

		// Get all resources from the right side
		rightSeq, err := ctx.IterResources(a.right, subject, NoObjectFilter())
		if err != nil {
			yield(Path{}, err)
			return
		}

		rightPathCount := 0
		totalResultPaths := 0
		for rightPath, err := range rightSeq {
			if err != nil {
				yield(Path{}, err)
				return
			}

			// Note: We used to filter self-edges here, but self-edges from Alias represent valid identity checks
			// (e.g., team:first#member accessing team:first via member). Removing the filter allows these
			// identity relationships to propagate through arrows correctly.

			rightPathCount++

			// For each right resource, query the left side.
			// For schema arrows (relation->permission), we need to query with BOTH:
			// 1. The specific relation from the path
			// 2. The same resource with ellipsis (to match stored relationships with ellipsis subjects)
			// For subrelation arrows (relation: type#subrelation), we only query with the specific relation.
			rightResourceAsSubject := rightPath.ResourceOAR()
			leftPathCount := 0

			if a.isSchemaArrow {
				// Schema arrow: query with both specific relation and ellipsis
				rightResourceWithEllipsis := ObjectAndRelation{
					ObjectType: rightResourceAsSubject.ObjectType,
					ObjectID:   rightResourceAsSubject.ObjectID,
					Relation:   tuple.Ellipsis,
				}

				ctx.TraceStep(a, "iterating left side for right resource %s:%s#%s (and ellipsis, schema arrow)",
					rightResourceAsSubject.ObjectType, rightResourceAsSubject.ObjectID, rightResourceAsSubject.Relation)

				// Query with specific relation
				leftSeqSpecific, err := ctx.IterResources(a.left, rightResourceAsSubject, filterResourceType)
				if err != nil {
					yield(Path{}, err)
					return
				}

				// Query with ellipsis
				leftSeqEllipsis, err := ctx.IterResources(a.left, rightResourceWithEllipsis, filterResourceType)
				if err != nil {
					yield(Path{}, err)
					return
				}

				// Process both sequences
				count1, ok := processRightPathSequence(leftSeqSpecific, rightPath, yield)
				leftPathCount += count1
				totalResultPaths += count1
				if !ok {
					return
				}

				count2, ok := processRightPathSequence(leftSeqEllipsis, rightPath, yield)
				leftPathCount += count2
				totalResultPaths += count2
				if !ok {
					return
				}

				ctx.TraceStep(a, "left side returned %d paths for right subject %s:%s#%s (and ellipsis, schema arrow)", leftPathCount, rightResourceAsSubject.ObjectType, rightResourceAsSubject.ObjectID, rightResourceAsSubject.Relation)
			} else {
				// Subrelation arrow: query with the left iterator's expected subrelation
				// Get the expected subject types from the left iterator
				leftSubjectTypes, err := a.left.SubjectTypes()
				if err != nil {
					yield(Path{}, err)
					return
				}

				// Use the expected subrelation from the left iterator
				// (for subrelation arrows, there should be exactly one expected type)
				if len(leftSubjectTypes) > 0 && leftSubjectTypes[0].Type == rightResourceAsSubject.ObjectType {
					expectedSubject := ObjectAndRelation{
						ObjectType: rightResourceAsSubject.ObjectType,
						ObjectID:   rightResourceAsSubject.ObjectID,
						Relation:   leftSubjectTypes[0].Subrelation,
					}

					ctx.TraceStep(a, "iterating left side for right resource %s:%s#%s (subrelation arrow, using expected subrelation %s)",
						rightResourceAsSubject.ObjectType, rightResourceAsSubject.ObjectID, rightResourceAsSubject.Relation, expectedSubject.Relation)

					leftSeq, err := ctx.IterResources(a.left, expectedSubject, filterResourceType)
					if err != nil {
						yield(Path{}, err)
						return
					}

					count, ok := processRightPathSequence(leftSeq, rightPath, yield)
					leftPathCount += count
					totalResultPaths += count
					if !ok {
						return
					}

					ctx.TraceStep(a, "left side returned %d paths for right subject %s:%s#%s (subrelation arrow)", leftPathCount, expectedSubject.ObjectType, expectedSubject.ObjectID, expectedSubject.Relation)
				}
			}
		}

		ctx.TraceStep(a, "arrow IterSubjects completed: %d right paths, %d total result paths", rightPathCount, totalResultPaths)
	}, nil
}

func (a *ArrowIterator) Clone() Iterator {
	return &ArrowIterator{
		canonicalKey:  a.canonicalKey,
		left:          a.left.Clone(),
		right:         a.right.Clone(),
		direction:     a.direction,     // preserve direction
		isSchemaArrow: a.isSchemaArrow, // preserve arrow type
	}
}

func (a *ArrowIterator) Explain() Explain {
	var kind string
	switch a.direction {
	case rightToLeft:
		kind = "RTL"
	case leftToRight:
		kind = "LTR"
	}
	return Explain{
		Name:       "Arrow",
		Info:       fmt.Sprintf("Arrow(%s)", kind),
		SubExplain: []Explain{a.left.Explain(), a.right.Explain()},
	}
}

func (a *ArrowIterator) Subiterators() []Iterator {
	return []Iterator{a.left, a.right}
}

func (a *ArrowIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &ArrowIterator{
		canonicalKey:  a.canonicalKey,
		left:          newSubs[0],
		right:         newSubs[1],
		direction:     a.direction,
		isSchemaArrow: a.isSchemaArrow,
	}, nil
}

func (a *ArrowIterator) CanonicalKey() CanonicalKey {
	return a.canonicalKey
}

func (a *ArrowIterator) ResourceType() ([]ObjectType, error) {
	// Arrow's resources come from the left side
	return a.left.ResourceType()
}

func (a *ArrowIterator) SubjectTypes() ([]ObjectType, error) {
	// Arrow's subjects come from the right side
	return a.right.SubjectTypes()
}
