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

func (a *ArrowIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	// There are three major strategies:
	// - IterSubjects on the left, Check on the right
	// - IterResources on the right, Check on the left
	// - IterSubjects on left, IterResources on right, and intersect the two iterators here (especially if they are known to be sorted)
	//
	// But for now, we cover the first two. When BatchedArrows is set the
	// per-element Check loop is replaced by a single CheckMany call so the
	// executor (e.g. DispatchExecutor) can collapse fanout into one RPC.
	switch a.direction {
	case leftToRight:
		if ctx.BatchedArrows {
			return a.checkLeftToRightBatch(ctx, resource, subject)
		}
		return a.checkLeftToRight(ctx, resource, subject)
	case rightToLeft:
		if ctx.BatchedArrows {
			return a.checkRightToLeftBatch(ctx, resource, subject)
		}
		return a.checkRightToLeft(ctx, resource, subject)
	default:
		return nil, spiceerrors.MustBugf("unknown arrow direction: %d", a.direction)
	}
}

// checkLeftToRight implements the left-to-right strategy:
// IterSubjects on left for the resource, then Check on right for each left subject.
// Returns the first found combined path (OR-merged if multiple left paths lead to the same subject).
func (a *ArrowIterator) checkLeftToRight(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow check (left-to-right) for resource %s:%s", resource.ObjectType, resource.ObjectID)
	}

	subit, err := ctx.IterSubjects(a.left, resource, NoObjectFilter())
	if err != nil {
		return nil, err
	}

	var result *Path
	leftPathCount := 0
	for leftPath, err := range subit {
		if err != nil {
			return nil, err
		}
		leftPathCount++

		// If the left side returned a wildcard (e.g., folder:*), we can't use it as a
		// resource for the right side. Instead, invert: call IterResources on the right
		// with the target subject to find any intermediate of the matching type.
		if leftPath.Subject.ObjectID == tuple.PublicWildcard {
			if ctx.shouldTrace() {
				ctx.TraceStep(a, "left returned wildcard %s:*, using IterResources inversion", leftPath.Subject.ObjectType)
			}
			rightSeq, err := ctx.IterResources(a.right, subject, ObjectType{Type: leftPath.Subject.ObjectType})
			if err != nil {
				return nil, err
			}
			for rightPath, err := range rightSeq {
				if err != nil {
					return nil, err
				}
				// Any matching intermediate means the wildcard left is satisfied.
				combined := combineArrowPaths(leftPath, rightPath)
				if combined.Caveat == nil {
					return combined, nil
				}
				result, err = result.MergeOr(combined)
				if err != nil {
					return nil, err
				}
			}
			continue
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(a, "checking right side for left subject %s:%s", leftPath.Subject.ObjectType, leftPath.Subject.ObjectID)
		}

		rightPath, err := ctx.Check(a.right, GetObject(leftPath.Subject), subject)
		if err != nil {
			return nil, err
		}

		if rightPath == nil {
			continue
		}

		combined := combineArrowPaths(leftPath, rightPath)
		if combined.Caveat == nil {
			return combined, nil
		}

		result, err = result.MergeOr(combined)
		if err != nil {
			return nil, err
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow (left-to-right) completed: %d left paths, found=%v", leftPathCount, result != nil)
	}
	return result, nil
}

// checkRightToLeft implements the right-to-left strategy:
// IterResources on right to get candidate intermediates, then Check on left for the resource.
func (a *ArrowIterator) checkRightToLeft(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow check (right-to-left) for resource %s:%s, subject %s:%s",
			resource.ObjectType, resource.ObjectID, subject.ObjectType, subject.ObjectID)
	}

	// Start from the right side with the target subject to get candidate intermediates
	rightSeq, err := ctx.IterResources(a.right, subject, NoObjectFilter())
	if err != nil {
		return nil, err
	}

	var result *Path
	rightPathCount := 0
	for rightPath, err := range rightSeq {
		if err != nil {
			return nil, err
		}
		rightPathCount++

		// rightPath.Resource is an intermediate object from the right side.
		// Check if our input resource connects to this intermediate via the left side.
		// Use tuple.Ellipsis as the relation since the left side stores subjects with "..."
		// (the canonical relation for direct membership with no subrelation).
		intermediateAsSubject := ObjectAndRelation{
			ObjectType: rightPath.Resource.ObjectType,
			ObjectID:   rightPath.Resource.ObjectID,
			Relation:   tuple.Ellipsis,
		}

		leftPath, err := ctx.Check(a.left, resource, intermediateAsSubject)
		if err != nil {
			return nil, err
		}

		if leftPath == nil {
			continue
		}

		combined := combineArrowPaths(leftPath, rightPath)
		if combined.Caveat == nil {
			return combined, nil
		}

		result, err = result.MergeOr(combined)
		if err != nil {
			return nil, err
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(a, "left matched intermediate %s:%s", intermediateAsSubject.ObjectType, intermediateAsSubject.ObjectID)
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow (right-to-left) completed: %d right paths, found=%v", rightPathCount, result != nil)
	}
	return result, nil
}

// checkLeftToRightBatch is the batched variant of checkLeftToRight: it drains
// all left subjects first, then issues a single CheckManyResources call against
// the right side. Wildcards still fall back to the per-element IterResources
// inversion path because they cannot be used as a concrete resource on the right.
func (a *ArrowIterator) checkLeftToRightBatch(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow check (left-to-right, batched) for resource %s:%s", resource.ObjectType, resource.ObjectID)
	}

	subit, err := ctx.IterSubjects(a.left, resource, NoObjectFilter())
	if err != nil {
		return nil, err
	}
	leftPaths, err := CollectAll(subit)
	if err != nil {
		return nil, err
	}

	var result *Path
	concreteLeft := make([]*Path, 0, len(leftPaths))
	concreteRes := make([]Object, 0, len(leftPaths))
	for _, leftPath := range leftPaths {
		// Wildcards: invert to IterResources, like the unbatched path.
		if leftPath.Subject.ObjectID == tuple.PublicWildcard {
			rightSeq, err := ctx.IterResources(a.right, subject, ObjectType{Type: leftPath.Subject.ObjectType})
			if err != nil {
				return nil, err
			}
			for rightPath, err := range rightSeq {
				if err != nil {
					return nil, err
				}
				combined := combineArrowPaths(leftPath, rightPath)
				if combined.Caveat == nil {
					return combined, nil
				}
				result, err = result.MergeOr(combined)
				if err != nil {
					return nil, err
				}
			}
			continue
		}
		concreteLeft = append(concreteLeft, leftPath)
		concreteRes = append(concreteRes, GetObject(leftPath.Subject))
	}

	if len(concreteRes) > 0 {
		rightPaths, err := ctx.CheckManyResources(a.right, concreteRes, subject)
		if err != nil {
			return nil, err
		}
		for i, rightPath := range rightPaths {
			if rightPath == nil {
				continue
			}
			combined := combineArrowPaths(concreteLeft[i], rightPath)
			if combined.Caveat == nil {
				return combined, nil
			}
			result, err = result.MergeOr(combined)
			if err != nil {
				return nil, err
			}
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow (left-to-right, batched) completed: %d concrete, found=%v", len(concreteLeft), result != nil)
	}
	return result, nil
}

// checkRightToLeftBatch is the batched variant of checkRightToLeft: it drains
// all right resources first, then issues a single CheckManySubjects call against
// the left side using the right resources (with ellipsis relation) as subjects.
func (a *ArrowIterator) checkRightToLeftBatch(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow check (right-to-left, batched) for resource %s:%s, subject %s:%s",
			resource.ObjectType, resource.ObjectID, subject.ObjectType, subject.ObjectID)
	}

	rightSeq, err := ctx.IterResources(a.right, subject, NoObjectFilter())
	if err != nil {
		return nil, err
	}
	rightPaths, err := CollectAll(rightSeq)
	if err != nil {
		return nil, err
	}
	if len(rightPaths) == 0 {
		return nil, nil
	}

	intermediates := make([]ObjectAndRelation, len(rightPaths))
	for i, rightPath := range rightPaths {
		intermediates[i] = ObjectAndRelation{
			ObjectType: rightPath.Resource.ObjectType,
			ObjectID:   rightPath.Resource.ObjectID,
			Relation:   tuple.Ellipsis,
		}
	}

	leftPaths, err := ctx.CheckManySubjects(a.left, resource, intermediates)
	if err != nil {
		return nil, err
	}

	var result *Path
	for i, leftPath := range leftPaths {
		if leftPath == nil {
			continue
		}
		combined := combineArrowPaths(leftPath, rightPaths[i])
		if combined.Caveat == nil {
			return combined, nil
		}
		result, err = result.MergeOr(combined)
		if err != nil {
			return nil, err
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(a, "arrow (right-to-left, batched) completed: %d right paths, found=%v", len(rightPaths), result != nil)
	}
	return result, nil
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
	fixedPath *Path,
	isLeft bool,
	yield func(*Path, error) bool,
) (int, bool) {
	count := 0
	for iteratedPath, err := range seq {
		if err != nil {
			yield(nil, err)
			return count, false
		}

		var combinedPath *Path
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
	leftPath *Path,
	yield func(*Path, error) bool,
) (int, bool) {
	return processPathSequence(seq, leftPath, true, yield)
}

// processRightPathSequence processes a sequence where the fixed path is on the right side.
// Returns (count, ok) where count is paths processed and ok indicates whether to continue.
func processRightPathSequence(
	seq PathSeq,
	rightPath *Path,
	yield func(*Path, error) bool,
) (int, bool) {
	return processPathSequence(seq, rightPath, false, yield)
}

// combineArrowPaths combines a left path and right path into a single path for arrow operations.
// The combined path uses the resource and relation from the left path, the subject from the right path,
// and combines caveats from both sides using AND logic.
func combineArrowPaths(leftPath, rightPath *Path) *Path {
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

	return &Path{
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
	return func(yield func(*Path, error) bool) {
		if ctx.shouldTrace() {
			ctx.TraceStep(a, "iterating subjects for resource %s:%s", resource.ObjectType, resource.ObjectID)
		}

		// Get all subjects from the left side
		leftSeq, err := ctx.IterSubjects(a.left, resource, NoObjectFilter())
		if err != nil {
			yield(nil, err)
			return
		}

		leftPathCount := 0
		totalResultPaths := 0
		for leftPath, err := range leftSeq {
			if err != nil {
				yield(nil, err)
				return
			}
			leftPathCount++

			// If the left side returned a wildcard (e.g., folder:*), we can't use it
			// as a resource for the right side. Skip it — we can't enumerate all
			// concrete subjects reachable through all intermediates of this type without
			// a store-wide query. This matches the traditional dispatch path, which also
			// doesn't expand wildcard tupleset entries through arrows.
			// (The Check path handles this correctly via IterResources inversion.)
			if leftPath.Subject.ObjectID == tuple.PublicWildcard {
				if ctx.shouldTrace() {
					ctx.TraceStep(a, "left returned wildcard %s:*, skipping (cannot follow arrow through wildcard)", leftPath.Subject.ObjectType)
				}
				continue
			}

			// For each left subject, get subjects from right side
			leftSubjectAsResource := GetObject(leftPath.Subject)
			if ctx.shouldTrace() {
				ctx.TraceStep(a, "iterating right side for left subject %s:%s", leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID)
			}

			rightSeq, err := ctx.IterSubjects(a.right, leftSubjectAsResource, filterSubjectType)
			if err != nil {
				yield(nil, err)
				return
			}

			// Process right subjects and combine with left path
			count, ok := processLeftPathSequence(rightSeq, leftPath, yield)
			totalResultPaths += count
			if !ok {
				return
			}

			if ctx.shouldTrace() {
				ctx.TraceStep(a, "right side returned %d paths for left subject %s:%s", count, leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID)
			}
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(a, "arrow IterSubjects completed: %d left paths, %d total result paths", leftPathCount, totalResultPaths)
		}
	}, nil
}

func (a *ArrowIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	// Arrow: resource -> left subjects -> right subjects
	// Get resources from right side, then for each, get resources from left side
	return func(yield func(*Path, error) bool) {
		if ctx.shouldTrace() {
			ctx.TraceStep(a, "iterating resources for subject %s:%s", subject.ObjectType, subject.ObjectID)
		}

		// Get all resources from the right side
		rightSeq, err := ctx.IterResources(a.right, subject, NoObjectFilter())
		if err != nil {
			yield(nil, err)
			return
		}

		rightPathCount := 0
		totalResultPaths := 0
		for rightPath, err := range rightSeq {
			if err != nil {
				yield(nil, err)
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

				if ctx.shouldTrace() {
					ctx.TraceStep(a, "iterating left side for right resource %s:%s#%s (and ellipsis, schema arrow)",
						rightResourceAsSubject.ObjectType, rightResourceAsSubject.ObjectID, rightResourceAsSubject.Relation)
				}

				// Query with specific relation
				leftSeqSpecific, err := ctx.IterResources(a.left, rightResourceAsSubject, filterResourceType)
				if err != nil {
					yield(nil, err)
					return
				}

				// Query with ellipsis
				leftSeqEllipsis, err := ctx.IterResources(a.left, rightResourceWithEllipsis, filterResourceType)
				if err != nil {
					yield(nil, err)
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

				if ctx.shouldTrace() {
					ctx.TraceStep(a, "left side returned %d paths for right subject %s:%s#%s (and ellipsis, schema arrow)", leftPathCount, rightResourceAsSubject.ObjectType, rightResourceAsSubject.ObjectID, rightResourceAsSubject.Relation)
				}
			} else {
				// Subrelation arrow: query with the left iterator's expected subrelation
				// Get the expected subject types from the left iterator
				leftSubjectTypes, err := a.left.SubjectTypes()
				if err != nil {
					yield(nil, err)
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

					if ctx.shouldTrace() {
						ctx.TraceStep(a, "iterating left side for right resource %s:%s#%s (subrelation arrow, using expected subrelation %s)",
							rightResourceAsSubject.ObjectType, rightResourceAsSubject.ObjectID, rightResourceAsSubject.Relation, expectedSubject.Relation)
					}

					leftSeq, err := ctx.IterResources(a.left, expectedSubject, filterResourceType)
					if err != nil {
						yield(nil, err)
						return
					}

					count, ok := processRightPathSequence(leftSeq, rightPath, yield)
					leftPathCount += count
					totalResultPaths += count
					if !ok {
						return
					}

					if ctx.shouldTrace() {
						ctx.TraceStep(a, "left side returned %d paths for right subject %s:%s#%s (subrelation arrow)", leftPathCount, expectedSubject.ObjectType, expectedSubject.ObjectID, expectedSubject.Relation)
					}
				}
			}
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(a, "arrow IterSubjects completed: %d right paths, %d total result paths", rightPathCount, totalResultPaths)
		}
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
