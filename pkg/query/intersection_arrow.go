package query

import (
	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// IntersectionArrowIterator is an iterator that represents the set of relations that
// follow from a walk in the graph where ALL subjects on the left must satisfy
// the right side condition.
//
// Ex: `group.all(member)` - user must be member of ALL groups
type IntersectionArrowIterator struct {
	left         Iterator
	right        Iterator
	canonicalKey CanonicalKey
}

var _ Iterator = &IntersectionArrowIterator{}

func NewIntersectionArrowIterator(left, right Iterator) *IntersectionArrowIterator {
	return &IntersectionArrowIterator{
		left:  left,
		right: right,
	}
}

func (ia *IntersectionArrowIterator) CheckImpl(ctx *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.shouldTrace() {
		ctx.TraceStep(ia, "processing resource %s:%s", resource.ObjectType, resource.ObjectID)
	}

	subit, err := ctx.IterSubjects(ia.left, resource, NoObjectFilter())
	if err != nil {
		return nil, err
	}

	// For intersection arrow:
	// 1. Enumerate all left subjects that actually exist for this resource
	// 2. For each, check if it satisfies the right side
	// 3. Only return a path if ALL existing left subjects satisfy the right condition
	// 4. Combine all (leftCaveat AND rightCaveat) pairs with AND logic

	validResults := make([]*Path, 0)
	if ctx.BatchedArrows {
		var (
			leftPaths    []*Path
			concreteRes  []Object
			concreteIdxs []int
			wildcardIdxs []int
		)
		for path, err := range subit {
			if err != nil {
				return nil, err
			}
			leftPaths = append(leftPaths, path)
			if path.Subject.ObjectID == tuple.PublicWildcard {
				wildcardIdxs = append(wildcardIdxs, len(leftPaths)-1)
			} else {
				concreteIdxs = append(concreteIdxs, len(leftPaths)-1)
				concreteRes = append(concreteRes, GetObject(path.Subject))
			}
		}

		checkResults := make([]*Path, len(leftPaths))
		if len(concreteRes) > 0 {
			batched, err := ctx.CheckManyResources(ia.right, concreteRes, subject)
			if err != nil {
				return nil, err
			}
			for i, idx := range concreteIdxs {
				checkResults[idx] = batched[i]
			}
		}
		// Wildcards still go through IterResources inversion individually.
		for _, idx := range wildcardIdxs {
			lp := leftPaths[idx]
			if ctx.shouldTrace() {
				ctx.TraceStep(ia, "left returned wildcard %s:*, using IterResources inversion", lp.Subject.ObjectType)
			}
			rightSeq, err := ctx.IterResources(ia.right, subject, ObjectType{Type: lp.Subject.ObjectType})
			if err != nil {
				return nil, err
			}
			for rp, err := range rightSeq {
				if err != nil {
					return nil, err
				}
				checkResults[idx] = rp
				break
			}
		}

		for i, leftPath := range leftPaths {
			checkPath := checkResults[i]
			if checkPath == nil {
				if ctx.shouldTrace() {
					ctx.TraceStep(ia, "left subject %s:%s did NOT connect on the right side",
						leftPath.Subject.ObjectType, leftPath.Subject.ObjectID)
				}
				return nil, nil
			}
			combinedCaveat := caveats.And(leftPath.Caveat, checkPath.Caveat)
			validResults = append(validResults, &Path{
				Resource:   leftPath.Resource,
				Relation:   leftPath.Relation,
				Subject:    checkPath.Subject,
				Caveat:     combinedCaveat,
				Expiration: combineExpiration(leftPath.Expiration, checkPath.Expiration),
				Integrity:  combineIntegrity(leftPath.Integrity, checkPath.Integrity),
				Metadata:   checkPath.Metadata,
			})
		}
	} else {
		for path, err := range subit {
			if err != nil {
				return nil, err
			}

			// If the left side returned a wildcard, use IterResources inversion on the right
			// to check if the subject appears in any resource of the matching type.
			var checkPath *Path
			if path.Subject.ObjectID == tuple.PublicWildcard {
				if ctx.shouldTrace() {
					ctx.TraceStep(ia, "left returned wildcard %s:*, using IterResources inversion", path.Subject.ObjectType)
				}
				rightSeq, err := ctx.IterResources(ia.right, subject, ObjectType{Type: path.Subject.ObjectType})
				if err != nil {
					return nil, err
				}
				for rp, err := range rightSeq {
					if err != nil {
						return nil, err
					}
					checkPath = rp
					break // any match suffices
				}
			} else {
				checkPath, err = ctx.Check(ia.right, GetObject(path.Subject), subject)
				if err != nil {
					return nil, err
				}
			}

			if checkPath == nil {
				if ctx.shouldTrace() {
					ctx.TraceStep(ia, "left subject %s:%s did NOT connect on the right side",
						path.Subject.ObjectType, path.Subject.ObjectID)
				}
				// One left subject failed — intersection fails entirely
				return nil, nil
			}

			if ctx.shouldTrace() {
				ctx.TraceStep(ia, "left subject %s:%s connects with the right side",
					path.Subject.ObjectType, path.Subject.ObjectID)
			}

			// Combine this path's left caveat with the right caveat
			combinedCaveat := caveats.And(path.Caveat, checkPath.Caveat)

			combinedPath := &Path{
				Resource:   path.Resource,
				Relation:   path.Relation,
				Subject:    checkPath.Subject,
				Caveat:     combinedCaveat,
				Expiration: combineExpiration(path.Expiration, checkPath.Expiration),
				Integrity:  combineIntegrity(path.Integrity, checkPath.Integrity),
				Metadata:   checkPath.Metadata,
			}
			validResults = append(validResults, combinedPath)
		}
	}

	if len(validResults) == 0 {
		// No left subjects existed at all — nothing to intersect
		return nil, nil
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(ia, "intersection SUCCESS - combining %d results", len(validResults))
	}

	// AND together all per-subject combined caveats
	var intersectionCaveat *core.CaveatExpression
	for i, result := range validResults {
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

	// Combine expiration and integrity from all results
	firstResult := validResults[0]
	combinedExpiration := firstResult.Expiration
	combinedIntegrity := firstResult.Integrity
	for i := 1; i < len(validResults); i++ {
		combinedExpiration = combineExpiration(combinedExpiration, validResults[i].Expiration)
		combinedIntegrity = combineIntegrity(combinedIntegrity, validResults[i].Integrity)
	}

	return &Path{
		Resource:   resource,
		Relation:   "",
		Subject:    subject,
		Caveat:     intersectionCaveat,
		Expiration: combinedExpiration,
		Integrity:  combinedIntegrity,
		Metadata:   firstResult.Metadata,
	}, nil
}

func (ia *IntersectionArrowIterator) IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	// IntersectionArrow: ALL left subjects must satisfy the right side
	if ctx.shouldTrace() {
		ctx.TraceStep(ia, "iterating subjects for resource %s:%s", resource.ObjectType, resource.ObjectID)
	}

	// Get all left subjects
	leftSeq, err := ctx.IterSubjects(ia.left, resource, NoObjectFilter())
	if err != nil {
		return nil, err
	}

	leftPaths, err := CollectAll(leftSeq)
	if err != nil {
		return nil, err
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(ia, "left side returned %d subjects", len(leftPaths))
	}

	if len(leftPaths) == 0 {
		if ctx.shouldTrace() {
			ctx.TraceStep(ia, "no left subjects, returning empty")
		}
		return EmptyPathSeq(), nil
	}

	// For intersection arrow, we need ALL left subjects to satisfy the right side
	// Track all valid results
	var validResults []*Path
	unsatisfied := false

	for _, leftPath := range leftPaths {
		// If the left side returned a wildcard, we can't use it as a resource for the
		// right side. Skip it — same reasoning as ArrowIterator.
		if leftPath.Subject.ObjectID == tuple.PublicWildcard {
			if ctx.shouldTrace() {
				ctx.TraceStep(ia, "left returned wildcard %s:*, skipping (cannot follow arrow through wildcard)", leftPath.Subject.ObjectType)
			}
			continue
		}

		leftSubjectAsResource := GetObject(leftPath.Subject)
		if ctx.shouldTrace() {
			ctx.TraceStep(ia, "checking right side for left subject %s:%s", leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID)
		}

		rightSeq, err := ctx.IterSubjects(ia.right, leftSubjectAsResource, filterSubjectType)
		if err != nil {
			return nil, err
		}

		rightPaths, err := CollectAll(rightSeq)
		if err != nil {
			return nil, err
		}

		if len(rightPaths) == 0 {
			if ctx.shouldTrace() {
				ctx.TraceStep(ia, "left subject %s:%s did NOT satisfy right side", leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID)
			}
			unsatisfied = true
			break
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(ia, "left subject %s:%s satisfied with %d right subjects", leftSubjectAsResource.ObjectType, leftSubjectAsResource.ObjectID, len(rightPaths))
		}

		// Collect all valid combinations
		for _, rightPath := range rightPaths {
			// Combine caveats from left and right with AND logic
			combinedCaveat := caveats.And(leftPath.Caveat, rightPath.Caveat)

			combinedPath := &Path{
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
		if ctx.shouldTrace() {
			ctx.TraceStep(ia, "intersection arrow FAILED - not all left subjects satisfied")
		}
		return EmptyPathSeq(), nil
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(ia, "intersection arrow SUCCESS - returning %d final subjects", len(validResults))
	}

	return func(yield func(*Path, error) bool) {
		for _, path := range validResults {
			if !yield(path, nil) {
				return
			}
		}
	}, nil
}

func (ia *IntersectionArrowIterator) IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	// IntersectionArrow: ALL left subjects must satisfy the right side
	if ctx.shouldTrace() {
		ctx.TraceStep(ia, "iterating resources for subject %s:%s", subject.ObjectType, subject.ObjectID)
	}

	// Get all right resources
	rightSeq, err := ctx.IterResources(ia.right, subject, NoObjectFilter())
	if err != nil {
		return nil, err
	}

	rightPaths, err := CollectAll(rightSeq)
	if err != nil {
		return nil, err
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(ia, "right side returned %d resources", len(rightPaths))
	}

	if len(rightPaths) == 0 {
		if ctx.shouldTrace() {
			ctx.TraceStep(ia, "no right resources, returning empty")
		}
		return EmptyPathSeq(), nil
	}

	// seenResources is used to avoid rechecking resources that we've already seen
	seenResources := mapz.NewSet[string]()
	validResults := make([]*Path, 0)

	for _, rightPath := range rightPaths {
		rightResourceAsSubject := rightPath.Resource.WithEllipses()
		if ctx.shouldTrace() {
			ctx.TraceStep(ia, "looking up left resources for right resource %s:%s", rightResourceAsSubject.ObjectType, rightResourceAsSubject.ObjectID)
		}

		leftSeq, err := ctx.IterResources(ia.left, rightResourceAsSubject, filterResourceType)
		if err != nil {
			return nil, err
		}

		leftPaths, err := CollectAll(leftSeq)
		if err != nil {
			return nil, err
		}

		if ctx.shouldTrace() {
			ctx.TraceStep(ia, "right subject %s:%s returned %d left resources", rightPath.Resource.ObjectType, rightPath.Resource.ObjectID, len(leftPaths))
		}

		// Check each unseen left resource individually against the original subject to ensure
		// that all of their subjects satisfy the intersection arrow constraint.
		for _, leftPath := range leftPaths {
			leftResource := leftPath.Resource
			key := leftResource.Key()
			if notSeen := seenResources.Add(key); !notSeen {
				continue
			}

			checkPath, err := ia.CheckImpl(ctx, leftResource, subject)
			if err != nil {
				return nil, err
			}
			if checkPath != nil {
				validResults = append(validResults, checkPath)
			}
		}
	}

	if ctx.shouldTrace() {
		ctx.TraceStep(ia, "intersection arrow SUCCESS - returning %d final resources", len(validResults))
	}

	return func(yield func(*Path, error) bool) {
		for _, path := range validResults {
			if !yield(path, nil) {
				return
			}
		}
	}, nil
}

func (ia *IntersectionArrowIterator) Clone() Iterator {
	return &IntersectionArrowIterator{
		canonicalKey: ia.canonicalKey,
		left:         ia.left.Clone(),
		right:        ia.right.Clone(),
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
	return &IntersectionArrowIterator{canonicalKey: ia.canonicalKey, left: newSubs[0], right: newSubs[1]}, nil
}

func (ia *IntersectionArrowIterator) CanonicalKey() CanonicalKey {
	return ia.canonicalKey
}

func (ia *IntersectionArrowIterator) ResourceType() ([]ObjectType, error) {
	// IntersectionArrow's resources come from the left side
	return ia.left.ResourceType()
}

func (ia *IntersectionArrowIterator) SubjectTypes() ([]ObjectType, error) {
	// IntersectionArrow's subjects come from the right side
	return ia.right.SubjectTypes()
}
