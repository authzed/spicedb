package graph

import (
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

// WalkHandler is a function invoked for each node in the rewrite tree. If it returns non-nil,
// that value is returned from the walk. Otherwise, the walk continues.
type WalkHandler func(childOneof *pb.SetOperation_Child) interface{}

// WalkRewrite walks a userset rewrite tree, invoking the handler found on each node of the tree
// until the handler returns a non-nil value, which is in turn returned from this function. Returns
// nil if no valid value was found. If the rewrite is nil, returns nil.
func WalkRewrite(rewrite *pb.UsersetRewrite, handler WalkHandler) interface{} {
	if rewrite == nil {
		return nil
	}

	switch rw := rewrite.RewriteOperation.(type) {
	case *pb.UsersetRewrite_Union:
		return walkRewriteChildren(rw.Union, handler)
	case *pb.UsersetRewrite_Intersection:
		return walkRewriteChildren(rw.Intersection, handler)
	case *pb.UsersetRewrite_Exclusion:
		return walkRewriteChildren(rw.Exclusion, handler)
	}
	return nil
}

// HasThis returns true if there exists a `_this` node anywhere within the given rewrite. If
// the rewrite is nil, returns false.
func HasThis(rewrite *pb.UsersetRewrite) bool {
	result := WalkRewrite(rewrite, func(childOneof *pb.SetOperation_Child) interface{} {
		switch childOneof.ChildType.(type) {
		case *pb.SetOperation_Child_XThis:
			return true
		default:
			return nil
		}
	})
	return result != nil && result.(bool)
}

func walkRewriteChildren(so *pb.SetOperation, handler WalkHandler) interface{} {
	for _, childOneof := range so.Child {
		vle := handler(childOneof)
		if vle != nil {
			return vle
		}

		switch child := childOneof.ChildType.(type) {
		case *pb.SetOperation_Child_UsersetRewrite:
			rvle := WalkRewrite(child.UsersetRewrite, handler)
			if rvle != nil {
				return rvle
			}
		}
	}

	return nil
}
