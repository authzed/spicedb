package query

import (
	"github.com/google/uuid"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// IntersectionArrowIterator is an iterator that represents the set of relations that
// follow from a walk in the graph where ALL subjects on the left must satisfy
// the right side condition.
//
// Ex: `group.all(member)` - user must be member of ALL groups
type IntersectionArrowIterator struct {
	id           string
	left         Iterator
	right        Iterator
	canonicalKey CanonicalKey
}

var _ Iterator = &IntersectionArrowIterator{}

func NewIntersectionArrowIterator(left, right Iterator) *IntersectionArrowIterator {
	return &IntersectionArrowIterator{
		id:    uuid.NewString(),
		left:  left,
		right: right,
	}
}

func (ia *IntersectionArrowIterator) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	return func(yield func(Path, error) bool) {
		for _, resource := range resources {
			ctx.TraceStep(ia, "processing resource %s:%s", resource.ObjectType, resource.ObjectID)

			subit, err := ctx.IterSubjects(ia.left, resource, NoObjectFilter())
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
					Expiration: combineExpiration(path.Expiration, checkPath.Expiration),
					Integrity:  combineIntegrity(path.Integrity, checkPath.Integrity),
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

				// Combine expiration and integrity from all results
				combinedExpiration := firstResult.Expiration
				combinedIntegrity := firstResult.Integrity
				for i := 1; i < len(validResults); i++ {
					combinedExpiration = combineExpiration(combinedExpiration, validResults[i].Expiration)
					combinedIntegrity = combineIntegrity(combinedIntegrity, validResults[i].Integrity)
				}

				finalResult := Path{
					Resource:   resource,
					Relation:   "",
					Subject:    subject,
					Caveat:     intersectionCaveat,
					Expiration: combinedExpiration,
					Integrity:  combinedIntegrity,
					Metadata:   firstResult.Metadata,
				}

				if !yield(finalResult, nil) {
					return
				}
			}
		}
	}, nil
}

func (ia *IntersectionArrowIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	// IntersectionArrow: ALL left subjects must satisfy the right side
	ctx.TraceStep(ia, "iterating subjects for resource %s:%s", resource.ObjectType, resource.ObjectID)

	// Get all left subjects
	leftSeq, err := ctx.IterSubjects(ia.left, resource, NoObjectFilter())
	if err != nil {
		return nil, err
	}

	leftPaths, err := CollectAll(leftSeq)
	if err != nil {
		return nil, err
	}

	ctx.TraceStep(ia, "left side returned %d subjects", len(leftPaths))

	if len(leftPaths) == 0 {
		ctx.TraceStep(ia, "no left subjects, returning empty")
		return EmptyPathSeq(), nil
	}

	// For intersection arrow, we need ALL left subjects to satisfy the right side
	// Track all valid results
	var validResults []Path
	unsatisfied := false

	for _, leftPath := range leftPaths {
		leftSubjectAsResource := GetObject(leftPath.Subject)
		ctx.TraceStep(ia, "checking right side for left subject %s:%s", leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID)

		rightSeq, err := ctx.IterSubjects(ia.right, leftSubjectAsResource, filterSubjectType)
		if err != nil {
			return nil, err
		}

		rightPaths, err := CollectAll(rightSeq)
		if err != nil {
			return nil, err
		}

		if len(rightPaths) == 0 {
			ctx.TraceStep(ia, "left subject %s:%s did NOT satisfy right side", leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID)
			unsatisfied = true
			break
		}

		ctx.TraceStep(ia, "left subject %s:%s satisfied with %d right subjects", leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID, len(rightPaths))

		// Collect all valid combinations
		for _, rightPath := range rightPaths {
			// Combine caveats from left and right with AND logic
			combinedCaveat := caveats.And(leftPath.Caveat, rightPath.Caveat)

			combinedPath := Path{
				Resource:   leftPath.Resource,
				Relation:   leftPath.Relation,
				Subject:    rightPath.Subject,
				Caveat:     combinedCaveat,
				Expiration: combineExpiration(leftPath.Expiration, rightPath.Expiration),
				Integrity:  combineIntegrity(leftPath.Integrity, rightPath.Integrity),
				Metadata:   make(map[string]any),
			}
			validResults = append(validResults, combinedPath)
		}
	}

	if unsatisfied {
		ctx.TraceStep(ia, "intersection arrow FAILED - not all left subjects satisfied")
		return EmptyPathSeq(), nil
	}

	ctx.TraceStep(ia, "intersection arrow SUCCESS - returning %d final subjects", len(validResults))

	return func(yield func(Path, error) bool) {
		for _, path := range validResults {
			if !yield(path, nil) {
				return
			}
		}
	}, nil
}

func (ia *IntersectionArrowIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	// IntersectionArrow: ALL left subjects must satisfy the right side
	ctx.TraceStep(ia, "iterating resources for subject %s:%s", subject.ObjectType, subject.ObjectID)

	// Get all right resources
	rightSeq, err := ctx.IterResources(ia.right, subject, NoObjectFilter())
	if err != nil {
		return nil, err
	}

	rightPaths, err := CollectAll(rightSeq)
	if err != nil {
		return nil, err
	}

	ctx.TraceStep(ia, "right side returned %d resources", len(rightPaths))

	if len(rightPaths) == 0 {
		ctx.TraceStep(ia, "no right resources, returning empty")
		return EmptyPathSeq(), nil
	}

	// seenResources is used to avoid rechecking resources that we've already seen
	seenResources := mapz.NewSet[string]()
	validResults := make([]Path, 0)

	for _, rightPath := range rightPaths {
		rightResourceAsSubject := rightPath.Resource.WithEllipses()
		ctx.TraceStep(ia, "looking up left resources for right resource %s:%s", rightResourceAsSubject.ObjectType, rightResourceAsSubject.ObjectID)

		leftSeq, err := ctx.IterResources(ia.left, rightResourceAsSubject, filterResourceType)
		if err != nil {
			return nil, err
		}

		leftPaths, err := CollectAll(leftSeq)
		if err != nil {
			return nil, err
		}

		ctx.TraceStep(ia, "right subject %s:%s returned %d left resources", rightPath.Resource.ObjectType, rightPath.Resource.ObjectID, len(leftPaths))

		// Make a list of the leftPaths that we haven't seen
		// before that we need to check
		leftResources := make([]Object, 0, len(leftPaths))
		for _, path := range leftPaths {
			resource := path.Resource
			key := resource.Key()
			if notSeen := seenResources.Add(key); notSeen {
				leftResources = append(leftResources, path.Resource)
			}
		}

		// Now that we have all of the potential left resources, we need to check
		// them individually against the original subject to ensure that all of their
		// subjects satisfy the intersection arrow constraint.
		checkSeq, err := ia.CheckImpl(ctx, leftResources, subject)
		if err != nil {
			return nil, err
		}

		// The remaining values are a part of the resource set
		for path, err := range checkSeq {
			if err != nil {
				return nil, err
			}
			validResults = append(validResults, path)
		}
	}

	ctx.TraceStep(ia, "intersection arrow SUCCESS - returning %d final resources", len(validResults))

	return func(yield func(Path, error) bool) {
		for _, path := range validResults {
			if !yield(path, nil) {
				return
			}
		}
	}, nil
}

func (ia *IntersectionArrowIterator) Clone() Iterator {
	return &IntersectionArrowIterator{
		id:    uuid.NewString(),
		left:  ia.left.Clone(),
		right: ia.right.Clone(),
	}
}

func (ia *IntersectionArrowIterator) Explain() Explain {
	return Explain{
		Name:       "IntersectionArrow",
		Info:       "IntersectionArrow",
		SubExplain: []Explain{ia.left.Explain(), ia.right.Explain()},
	}
}

func (ia *IntersectionArrowIterator) Subiterators() []Iterator {
	return []Iterator{ia.left, ia.right}
}

func (ia *IntersectionArrowIterator) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &IntersectionArrowIterator{id: uuid.NewString(), left: newSubs[0], right: newSubs[1]}, nil
}

func (ia *IntersectionArrowIterator) ID() string {
	return ia.id
}

func (ia *IntersectionArrowIterator) ResourceType() ([]ObjectType, error) {
	// IntersectionArrow's resources come from the left side
	return ia.left.ResourceType()
}

func (ia *IntersectionArrowIterator) SubjectTypes() ([]ObjectType, error) {
	// IntersectionArrow's subjects come from the right side
	return ia.right.SubjectTypes()
}
