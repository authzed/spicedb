package graph

import (
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
)

// Leaf constructs a RelationTupleTreeNode leaf.
func Leaf(start *v0.ObjectAndRelation, children ...*v0.User) *v0.RelationTupleTreeNode {
	return &v0.RelationTupleTreeNode{
		NodeType: &v0.RelationTupleTreeNode_LeafNode{
			LeafNode: &v0.DirectUserset{
				Users: children,
			},
		},
		Expanded: start,
	}
}

func setResult(
	op v0.SetOperationUserset_Operation,
	start *v0.ObjectAndRelation,
	children []*v0.RelationTupleTreeNode,
) *v0.RelationTupleTreeNode {
	return &v0.RelationTupleTreeNode{
		NodeType: &v0.RelationTupleTreeNode_IntermediateNode{
			IntermediateNode: &v0.SetOperationUserset{
				Operation:  op,
				ChildNodes: children,
			},
		},
		Expanded: start,
	}
}

// Union constructs a RelationTupleTreeNode union operation.
func Union(start *v0.ObjectAndRelation, children ...*v0.RelationTupleTreeNode) *v0.RelationTupleTreeNode {
	return setResult(v0.SetOperationUserset_UNION, start, children)
}

// Intersection constructs a RelationTupleTreeNode intersection operation.
func Intersection(start *v0.ObjectAndRelation, children ...*v0.RelationTupleTreeNode) *v0.RelationTupleTreeNode {
	return setResult(v0.SetOperationUserset_INTERSECTION, start, children)
}

// Exclusion constructs a RelationTupleTreeNode exclusion operation.
func Exclusion(start *v0.ObjectAndRelation, children ...*v0.RelationTupleTreeNode) *v0.RelationTupleTreeNode {
	return setResult(v0.SetOperationUserset_EXCLUSION, start, children)
}
