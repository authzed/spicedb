package graph

import (
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// WalkHandler is a function invoked for each node in the rewrite tree. If it returns non-nil,
// that value is returned from the walk. Otherwise, the walk continues.
type WalkHandler func(childOneof *core.SetOperation_Child) interface{}

// WalkRewrite walks a userset rewrite tree, invoking the handler found on each node of the tree
// until the handler returns a non-nil value, which is in turn returned from this function. Returns
// nil if no valid value was found. If the rewrite is nil, returns nil.
func WalkRewrite(rewrite *core.UsersetRewrite, handler WalkHandler) interface{} {
	if rewrite == nil {
		return nil
	}

	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return walkRewriteChildren(rw.Union, handler)
	case *core.UsersetRewrite_Intersection:
		return walkRewriteChildren(rw.Intersection, handler)
	case *core.UsersetRewrite_Exclusion:
		return walkRewriteChildren(rw.Exclusion, handler)
	default:
		panic(fmt.Sprintf("unknown type of rewrite operation in walker: %T", rw))
	}
}

// HasThis returns true if there exists a `_this` node anywhere within the given rewrite. If
// the rewrite is nil, returns false.
func HasThis(rewrite *core.UsersetRewrite) bool {
	result := WalkRewrite(rewrite, func(childOneof *core.SetOperation_Child) interface{} {
		switch childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			return true
		default:
			return nil
		}
	})
	return result != nil && result.(bool)
}

func walkRewriteChildren(so *core.SetOperation, handler WalkHandler) interface{} {
	for _, childOneof := range so.Child {
		vle := handler(childOneof)
		if vle != nil {
			return vle
		}

		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_UsersetRewrite:
			rvle := WalkRewrite(child.UsersetRewrite, handler)
			if rvle != nil {
				return rvle
			}
		}
	}

	return nil
}

type OperationPath = []uint32

// FindOperation finds the operation at the given path in the rewrite, if any.
func FindOperation[T any](rewrite *core.UsersetRewrite, opPath OperationPath) *T {
	if rewrite == nil {
		return nil
	}

	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return findRewriteOperation[T](rw.Union, opPath).(*T)
	case *core.UsersetRewrite_Intersection:
		return findRewriteOperation[T](rw.Intersection, opPath).(*T)
	case *core.UsersetRewrite_Exclusion:
		return findRewriteOperation[T](rw.Exclusion, opPath).(*T)
	default:
		panic(fmt.Sprintf("unknown type of rewrite operation in FindOperation: %T", rw))
	}
}

func findRewriteOperation[T any](so *core.SetOperation, opPath OperationPath) any {
	if len(opPath) == 0 {
		panic("invalid operations path")
	}

	if opPath[0] >= uint32(len(so.Child)) {
		panic("invalid operations path")
	}

	childOneof := so.Child[opPath[0]]
	switch child := childOneof.ChildType.(type) {
	case *core.SetOperation_Child_UsersetRewrite:
		return FindOperation[T](child.UsersetRewrite, opPath[1:])

	default:
		if len(opPath) > 1 {
			panic("invalid operations path")
		}
	}

	switch child := childOneof.ChildType.(type) {
	case *core.SetOperation_Child_XThis:
		return child.XThis

	case *core.SetOperation_Child_ComputedUserset:
		return child.ComputedUserset

	case *core.SetOperation_Child_TupleToUserset:
		return child.TupleToUserset

	case *core.SetOperation_Child_XNil:
		return child.XNil

	default:
		panic(fmt.Sprintf("unknown set operation child `%T` in findRewriteOperation", child))
	}
}
