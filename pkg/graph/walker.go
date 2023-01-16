package graph

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// WalkHandler is a function invoked for each node in the rewrite tree. If it returns non-nil,
// that value is returned from the walk. Otherwise, the walk continues.
type WalkHandler func(childOneof *core.SetOperation_Child) interface{}

// WalkRewrite walks a userset rewrite tree, invoking the handler found on each node of the tree
// until the handler returns a non-nil value, which is in turn returned from this function. Returns
// nil if no valid value was found. If the rewrite is nil, returns nil.
func WalkRewrite(rewrite *core.UsersetRewrite, handler WalkHandler) (interface{}, error) {
	if rewrite == nil {
		return nil, nil
	}

	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return walkRewriteChildren(rw.Union, handler)
	case *core.UsersetRewrite_Intersection:
		return walkRewriteChildren(rw.Intersection, handler)
	case *core.UsersetRewrite_Exclusion:
		return walkRewriteChildren(rw.Exclusion, handler)
	default:
		return nil, spiceerrors.MustBugf("unknown type of rewrite operation in walker: %T", rw)
	}
}

// HasThis returns true if there exists a `_this` node anywhere within the given rewrite. If
// the rewrite is nil, returns false.
func HasThis(rewrite *core.UsersetRewrite) (bool, error) {
	result, err := WalkRewrite(rewrite, func(childOneof *core.SetOperation_Child) interface{} {
		switch childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			return true
		default:
			return nil
		}
	})
	return result != nil && result.(bool), err
}

func walkRewriteChildren(so *core.SetOperation, handler WalkHandler) (interface{}, error) {
	for _, childOneof := range so.Child {
		vle := handler(childOneof)
		if vle != nil {
			return vle, nil
		}

		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_UsersetRewrite:
			rvle, err := WalkRewrite(child.UsersetRewrite, handler)
			if err != nil {
				return nil, err
			}

			if rvle != nil {
				return rvle, nil
			}
		}
	}

	return nil, nil
}
