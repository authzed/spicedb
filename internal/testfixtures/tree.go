package testfixtures

import (
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

func Leaf(start *pb.ObjectAndRelation, children ...*pb.User) *pb.RelationTupleTreeNode {
	return &pb.RelationTupleTreeNode{
		NodeType: &pb.RelationTupleTreeNode_LeafNode{
			LeafNode: &pb.DirectUserset{
				Users: children,
			},
		},
		Expanded: start,
	}
}

func setResult(
	op pb.SetOperationUserset_Operation,
	start *pb.ObjectAndRelation,
	children []*pb.RelationTupleTreeNode,
) *pb.RelationTupleTreeNode {
	return &pb.RelationTupleTreeNode{
		NodeType: &pb.RelationTupleTreeNode_IntermediateNode{
			IntermediateNode: &pb.SetOperationUserset{
				Operation:  op,
				ChildNodes: children,
			},
		},
		Expanded: start,
	}
}

func U(start *pb.ObjectAndRelation, children ...*pb.RelationTupleTreeNode) *pb.RelationTupleTreeNode {
	return setResult(pb.SetOperationUserset_UNION, start, children)
}

func I(start *pb.ObjectAndRelation, children ...*pb.RelationTupleTreeNode) *pb.RelationTupleTreeNode {
	return setResult(pb.SetOperationUserset_INTERSECTION, start, children)
}

func E(start *pb.ObjectAndRelation, children ...*pb.RelationTupleTreeNode) *pb.RelationTupleTreeNode {
	return setResult(pb.SetOperationUserset_EXCLUSION, start, children)
}
