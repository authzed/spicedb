package query

import (
	"github.com/authzed/spicedb/internal/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Arrow is an iterator that represents the set of paths that
// follow from a walk in the graph.
//
// Ex: `folder->owner` and `left->right`
type Arrow struct {
	left  Iterator
	right Iterator
}

var _ Iterator = &Arrow{}

func NewArrow(left, right Iterator) *Arrow {
	return &Arrow{
		left:  left,
		right: right,
	}
}

func (a *Arrow) CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	// TODO -- the ordering, directionality, batching, everything can depend on other statistics.
	//
	// There are three major strategies:
	// - IterSubjects on the left, Check on the right (as per this implementation)
	// - IterResources on the right, Check on the left
	// - IterSubjects on left, IterResources on right, and intersect the two iterators here (especially if they are known to be sorted)
	//
	// But for now, this is a proof-of-concept, so the first one, one-by-one (no batching).
	// This is going to be the crux of a lot of statistics optimizations -- statistics often
	// don't restructure the tree, but can affect the best way to evaluate the tree, sometimes dynamically.

	return func(yield func(Path, error) bool) {
		ctx.TraceStep(a, "processing %d resources", len(resources))

		totalResultPaths := 0
		for resourceIdx, resource := range resources {
			ctx.TraceStep(a, "processing resource %d: %s:%s", resourceIdx, resource.ObjectType, resource.ObjectID)

			subit, err := ctx.IterSubjects(a.left, resource)
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

					// Combine caveats from both sides using Path-based approach
					// For arrow operations (left->right), both conditions must be satisfied (AND logic)
					var combinedCaveat *core.CaveatExpression
					switch {
					case path.Caveat != nil && checkPath.Caveat != nil:
						// Both sides have caveats - create combined caveat expression
						combinedCaveat = caveats.And(path.Caveat, checkPath.Caveat)
					case path.Caveat != nil:
						// Only left side has caveat
						combinedCaveat = path.Caveat
					case checkPath.Caveat != nil:
						// Only right side has caveat
						combinedCaveat = checkPath.Caveat
					}
					// else both are nil, combinedCaveat remains nil

					// Create combined path with resource from left and subject from right
					combinedPath := Path{
						Resource:   path.Resource,
						Relation:   path.Relation,
						Subject:    checkPath.Subject,
						Caveat:     combinedCaveat,
						Expiration: checkPath.Expiration,
						Integrity:  checkPath.Integrity,
						Metadata:   make(map[string]any),
					}

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

func (a *Arrow) IterSubjectsImpl(ctx *Context, resource Object) (PathSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (a *Arrow) IterResourcesImpl(ctx *Context, subject ObjectAndRelation) (PathSeq, error) {
	return nil, spiceerrors.MustBugf("unimplemented")
}

func (a *Arrow) Clone() Iterator {
	return &Arrow{
		left:  a.left.Clone(),
		right: a.right.Clone(),
	}
}

func (a *Arrow) Explain() Explain {
	return Explain{
		Name:       "Arrow",
		Info:       "Arrow",
		SubExplain: []Explain{a.left.Explain(), a.right.Explain()},
	}
}

func (a *Arrow) Subiterators() []Iterator {
	return []Iterator{a.left, a.right}
}

func (a *Arrow) ReplaceSubiterators(newSubs []Iterator) (Iterator, error) {
	return &Arrow{left: newSubs[0], right: newSubs[1]}, nil
}
