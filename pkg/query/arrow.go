package query

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// arrowDirection specifies which direction to execute the arrow check
type arrowDirection int

const (
	// leftToRight executes IterSubjects on left, Check on right
	leftToRight arrowDirection = iota
	// rightToLeft executes IterResources on right, Check on left
	rightToLeft
)

// Arrow is an iterator that represents the set of paths that
// follow from a walk in the graph.
//
// Ex: `folder->owner` and `left->right`
type Arrow struct {
	id        string
	left      Iterator
	right     Iterator
	direction arrowDirection // execution direction
}

var _ Iterator = &Arrow{}

func NewArrow(left, right Iterator) *Arrow {
	return &Arrow{
		id:        uuid.NewString(),
		left:      left,
		right:     right,
		direction: leftToRight,
	}
}

func (a *Arrow) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
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
func (a *Arrow) checkLeftToRight(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
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

				rightPathCount := 0
				for checkPath, err := range checkit {
					if err != nil {
						yield(Path{}, err)
						return
					}
					rightPathCount++

					combinedPath := combineArrowPaths(path, checkPath)

					totalResultPaths++
					if !yield(combinedPath, nil) {
						return
					}
				}

				ctx.TraceStep(a, "right side returned %d paths for subject %s:%s", rightPathCount, path.Subject.ObjectType, path.Subject.ObjectID)
			}

			ctx.TraceStep(a, "left side returned %d paths for resource %s:%s", leftPathCount, resource.ObjectType, resource.ObjectID)
		}

		ctx.TraceStep(a, "arrow completed with %d total result paths", totalResultPaths)
	}, nil
}

// checkRightToLeft implements the right-to-left strategy:
// IterResources on right to get candidate resources, then Check on left
func (a *Arrow) checkRightToLeft(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
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

			leftPathCount := 0
			for leftPath, err := range leftSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}
				leftPathCount++

				combinedPath := combineArrowPaths(leftPath, rightPath)

				totalResultPaths++
				if !yield(combinedPath, nil) {
					return
				}
			}

			ctx.TraceStep(a, "left side returned %d paths for intermediate %s:%s",
				leftPathCount, intermediateAsSubject.ObjectType, intermediateAsSubject.ObjectID)
		}

		ctx.TraceStep(a, "arrow check (right-to-left) completed: %d right paths, %d total result paths",
			rightPathCount, totalResultPaths)
	}, nil
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

func (a *Arrow) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
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

			rightPathCount := 0
			for rightPath, err := range rightSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}
				rightPathCount++

				combinedPath := combineArrowPaths(leftPath, rightPath)

				totalResultPaths++
				if !yield(combinedPath, nil) {
					return
				}
			}

			ctx.TraceStep(a, "right side returned %d paths for left subject %s:%s", rightPathCount, leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID)
		}

		ctx.TraceStep(a, "arrow IterSubjects completed: %d left paths, %d total result paths", leftPathCount, totalResultPaths)
	}, nil
}

func (a *Arrow) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
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

			// Filter out self-edges where the right resource (object + relation) matches the original subject
			// (these are circular references that shouldn't be propagated through arrows).
			// We need to check both the object (type+ID) AND the relation to identify true self-edges.
			if rightPath.Resource.Equals(GetObject(subject)) && rightPath.Relation == subject.Relation {
				continue
			}

			rightPathCount++

			// For each right resource, get resources from left side
			// TODO: see if WithEllipses is correct here
			rightResourceAsSubject := rightPath.Resource.WithEllipses()
			ctx.TraceStep(a, "iterating left side for right resource %s:%s", rightResourceAsSubject.ObjectType, rightResourceAsSubject.ObjectID)

			leftSeq, err := ctx.IterResources(a.left, rightResourceAsSubject, filterResourceType)
			if err != nil {
				yield(Path{}, err)
				return
			}

			leftPathCount := 0
			for leftPath, err := range leftSeq {
				if err != nil {
					yield(Path{}, err)
					return
				}
				leftPathCount++

				combinedPath := combineArrowPaths(leftPath, rightPath)

				totalResultPaths++
				if !yield(combinedPath, nil) {
					return
				}
			}

			ctx.TraceStep(a, "left side returned %d paths for right subject %s:%s", leftPathCount, rightResourceAsSubject.ObjectType, rightResourceAsSubject.ObjectID)
		}

		ctx.TraceStep(a, "arrow IterSubjects completed: %d right paths, %d total result paths", rightPathCount, totalResultPaths)
	}, nil
}

func (a *Arrow) Clone() Iterator {
	return &Arrow{
		id:        uuid.NewString(),
		left:      a.left.Clone(),
		right:     a.right.Clone(),
		direction: a.direction, // preserve direction
	}
}

func (a *Arrow) Explain() Explain {
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

func (a *Arrow) Subiterators() []Iterator {
	return []Iterator{a.left, a.right}
}

func (a *Arrow) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &Arrow{
		id:        uuid.NewString(),
		left:      newSubs[0],
		right:     newSubs[1],
		direction: a.direction,
	}, nil
}

func (a *Arrow) ID() string {
	return a.id
}

func (a *Arrow) ResourceType() ([]ObjectType, error) {
	// Arrow's resources come from the left side
	return a.left.ResourceType()
}

func (a *Arrow) SubjectTypes() ([]ObjectType, error) {
	// Arrow's subjects come from the right side
	return a.right.SubjectTypes()
}
