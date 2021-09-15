package graph

import (
	v0 "github.com/authzed/spicedb/internal/proto/authzed/api/v0"
)

// WalkHandler is a function invoked for each node in the rewrite tree. If it returns non-nil,
// that value is returned from the walk. Otherwise, the walk continues.
type WalkHandler func(childOneof *v0.SetOperation_Child) interface{}

// WalkRewrite walks a userset rewrite tree, invoking the handler found on each node of the tree
// until the handler returns a non-nil value, which is in turn returned from this function. Returns
// nil if no valid value was found. If the rewrite is nil, returns nil.
func WalkRewrite(rewrite *v0.UsersetRewrite, handler WalkHandler) interface{} {
	if rewrite == nil {
		return nil
	}

	switch rw := rewrite.RewriteOperation.(type) {
	case *v0.UsersetRewrite_Union:
		return walkRewriteChildren(rw.Union, handler)
	case *v0.UsersetRewrite_Intersection:
		return walkRewriteChildren(rw.Intersection, handler)
	case *v0.UsersetRewrite_Exclusion:
		return walkRewriteChildren(rw.Exclusion, handler)
	}
	return nil
}

// HasThis returns true if there exists a `_this` node anywhere within the given rewrite. If
// the rewrite is nil, returns false.
func HasThis(rewrite *v0.UsersetRewrite) bool {
	result := WalkRewrite(rewrite, func(childOneof *v0.SetOperation_Child) interface{} {
		switch childOneof.ChildType.(type) {
		case *v0.SetOperation_Child_XThis:
			return true
		default:
			return nil
		}
	})
	return result != nil && result.(bool)
}

func walkRewriteChildren(so *v0.SetOperation, handler WalkHandler) interface{} {
	for _, childOneof := range so.Child {
		vle := handler(childOneof)
		if vle != nil {
			return vle
		}

		switch child := childOneof.ChildType.(type) {
		case *v0.SetOperation_Child_UsersetRewrite:
			rvle := WalkRewrite(child.UsersetRewrite, handler)
			if rvle != nil {
				return rvle
			}
		}
	}

	return nil
}
