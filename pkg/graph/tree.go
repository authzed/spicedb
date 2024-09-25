package graph

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Leaf constructs a RelationTupleTreeNode leaf.
func Leaf(start *tuple.ObjectAndRelation, subjects ...*core.DirectSubject) *core.RelationTupleTreeNode {
	var startONR *core.ObjectAndRelation
	if start != nil {
		startONR = start.ToCoreONR()
	}

	return &core.RelationTupleTreeNode{
		NodeType: &core.RelationTupleTreeNode_LeafNode{
			LeafNode: &core.DirectSubjects{
				Subjects: subjects,
			},
		},
		Expanded:         startONR,
		CaveatExpression: nil, // Set by caller if necessary
	}
}

func setResult(
	op core.SetOperationUserset_Operation,
	start *tuple.ObjectAndRelation,
	children []*core.RelationTupleTreeNode,
) *core.RelationTupleTreeNode {
	var startONR *core.ObjectAndRelation
	if start != nil {
		startONR = start.ToCoreONR()
	}

	return &core.RelationTupleTreeNode{
		NodeType: &core.RelationTupleTreeNode_IntermediateNode{
			IntermediateNode: &core.SetOperationUserset{
				Operation:  op,
				ChildNodes: children,
			},
		},
		Expanded:         startONR,
		CaveatExpression: nil, // Set by caller if necessary
	}
}

// Union constructs a RelationTupleTreeNode union operation.
func Union(start *tuple.ObjectAndRelation, children ...*core.RelationTupleTreeNode) *core.RelationTupleTreeNode {
	return setResult(core.SetOperationUserset_UNION, start, children)
}

// Intersection constructs a RelationTupleTreeNode intersection operation.
func Intersection(start *tuple.ObjectAndRelation, children ...*core.RelationTupleTreeNode) *core.RelationTupleTreeNode {
	return setResult(core.SetOperationUserset_INTERSECTION, start, children)
}

// Exclusion constructs a RelationTupleTreeNode exclusion operation.
func Exclusion(start *tuple.ObjectAndRelation, children ...*core.RelationTupleTreeNode) *core.RelationTupleTreeNode {
	return setResult(core.SetOperationUserset_EXCLUSION, start, children)
}
