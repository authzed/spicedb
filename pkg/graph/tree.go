package graph

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// Leaf constructs a RelationTupleTreeNode leaf.
func Leaf(start *core.ObjectAndRelation, children ...*core.User) *core.RelationTupleTreeNode {
	return &core.RelationTupleTreeNode{
		NodeType: &core.RelationTupleTreeNode_LeafNode{
			LeafNode: &core.DirectUserset{
				Users: children,
			},
		},
		Expanded: start,
	}
}

func setResult(
	op core.SetOperationUserset_Operation,
	start *core.ObjectAndRelation,
	children []*core.RelationTupleTreeNode,
) *core.RelationTupleTreeNode {
	return &core.RelationTupleTreeNode{
		NodeType: &core.RelationTupleTreeNode_IntermediateNode{
			IntermediateNode: &core.SetOperationUserset{
				Operation:  op,
				ChildNodes: children,
			},
		},
		Expanded: start,
	}
}

// Union constructs a RelationTupleTreeNode union operation.
func Union(start *core.ObjectAndRelation, children ...*core.RelationTupleTreeNode) *core.RelationTupleTreeNode {
	return setResult(core.SetOperationUserset_UNION, start, children)
}

// Intersection constructs a RelationTupleTreeNode intersection operation.
func Intersection(start *core.ObjectAndRelation, children ...*core.RelationTupleTreeNode) *core.RelationTupleTreeNode {
	return setResult(core.SetOperationUserset_INTERSECTION, start, children)
}

// Exclusion constructs a RelationTupleTreeNode exclusion operation.
func Exclusion(start *core.ObjectAndRelation, children ...*core.RelationTupleTreeNode) *core.RelationTupleTreeNode {
	return setResult(core.SetOperationUserset_EXCLUSION, start, children)
}
