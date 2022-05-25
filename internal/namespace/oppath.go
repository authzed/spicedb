package namespace

import (
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

/**
 * decorateRelationOpPaths decorates all SetOperations found within the given relation's rewrite
 * (if any) with a path indicating each operation's position in the tree of operations.
 */
func decorateRelationOpPaths(relation *core.Relation) error {
	rewrite := relation.GetUsersetRewrite()
	if rewrite == nil {
		return nil
	}

	return decorateRelationRewritePath(rewrite, []uint32{})
}

func decorateRelationRewritePath(rewrite *core.UsersetRewrite, parentPath []uint32) error {
	switch rw := rewrite.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return decorateRelationRewriteChildrenWithPath(rw.Union, parentPath)
	case *core.UsersetRewrite_Intersection:
		return decorateRelationRewriteChildrenWithPath(rw.Intersection, parentPath)
	case *core.UsersetRewrite_Exclusion:
		return decorateRelationRewriteChildrenWithPath(rw.Exclusion, parentPath)
	default:
		return fmt.Errorf("unknown type of rewrite operation in rewrite path annotator: %T", rw)
	}
}

func decorateRelationRewriteChildrenWithPath(so *core.SetOperation, parentPath []uint32) error {
	for index, childOneof := range so.Child {
		if len(childOneof.OperationPath) > 0 {
			return nil
		}

		newPath := append(parentPath, uint32(index))
		childOneof.OperationPath = newPath

		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_UsersetRewrite:
			if err := decorateRelationRewritePath(child.UsersetRewrite, newPath); err != nil {
				return err
			}
		}
	}
	return nil
}
