package query

import (
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

	return func(yield func(*Path, error) bool) {
		for _, resource := range resources {
			subit, err := a.left.IterSubjectsImpl(ctx, resource)
			if err != nil {
				yield(nil, err)
				return
			}
			for path, err := range subit {
				if err != nil {
					yield(nil, err)
					return
				}
				checkResources := []Object{GetObject(path.Subject)}
				checkit, err := a.right.CheckImpl(ctx, checkResources, subject)
				if err != nil {
					yield(nil, err)
					return
				}
				for checkPath, err := range checkit {
					if err != nil {
						yield(nil, err)
						return
					}

					// Create combined path with resource from left and subject from right
					combinedPath := &Path{
						Resource:   path.Resource,
						Relation:   path.Relation,
						Subject:    checkPath.Subject,
						Caveat:     checkPath.Caveat,
						Expiration: checkPath.Expiration,
						Integrity:  checkPath.Integrity,
						Metadata:   make(map[string]any),
					}
					if !yield(combinedPath, nil) {
						return
					}
				}
			}
		}
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
		Info:       "Arrow",
		SubExplain: []Explain{a.left.Explain(), a.right.Explain()},
	}
}
