package graph

import (
	"fmt"

	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
)

func Simplify(node *pb.RelationTupleTreeNode) []*pb.User {
	switch typed := node.NodeType.(type) {
	case *pb.RelationTupleTreeNode_IntermediateNode:
		switch typed.IntermediateNode.Operation {
		case pb.SetOperationUserset_UNION:
			return SimplifyUnion(typed.IntermediateNode.ChildNodes)
		case pb.SetOperationUserset_INTERSECTION:
			return SimplifyIntersection(typed.IntermediateNode.ChildNodes)
		case pb.SetOperationUserset_EXCLUSION:
			return SimplifyExclusion(typed.IntermediateNode.ChildNodes)
		}
	case *pb.RelationTupleTreeNode_LeafNode:
		var toReturn UserSet = make(map[string]struct{})
		for _, usr := range typed.LeafNode.Users {
			toReturn.Add(usr)
		}
		return toReturn.ToSlice()
	}
	return nil
}

func SimplifyUnion(children []*pb.RelationTupleTreeNode) []*pb.User {
	var toReturn UserSet = make(map[string]struct{})
	for _, child := range children {
		toReturn.Add(Simplify(child)...)
	}
	return toReturn.ToSlice()
}

func SimplifyIntersection(children []*pb.RelationTupleTreeNode) []*pb.User {
	firstChildChildren := Simplify(children[0])

	if len(children) == 1 {
		return firstChildChildren
	}

	var inOthers UserSet = make(map[string]struct{})
	inOthers.Add(SimplifyIntersection(children[1:])...)

	maxChildren := len(firstChildChildren)
	if len(inOthers) < maxChildren {
		maxChildren = len(inOthers)
	}

	toReturn := make([]*pb.User, 0, maxChildren)
	for _, child := range firstChildChildren {
		if inOthers.Contains(child) {
			toReturn = append(toReturn, child)
		}
	}

	return toReturn
}

func SimplifyExclusion(children []*pb.RelationTupleTreeNode) []*pb.User {
	firstChildChildren := Simplify(children[0])

	if len(children) == 1 || len(firstChildChildren) == 0 {
		return firstChildChildren
	}

	var toReturn UserSet = make(map[string]struct{})
	toReturn.Add(firstChildChildren...)
	for _, child := range children[1:] {
		toReturn.Remove(Simplify(child)...)
	}

	return toReturn.ToSlice()
}

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

func Union(start *pb.ObjectAndRelation, children ...*pb.RelationTupleTreeNode) *pb.RelationTupleTreeNode {
	return setResult(pb.SetOperationUserset_UNION, start, children)
}

func Intersection(start *pb.ObjectAndRelation, children ...*pb.RelationTupleTreeNode) *pb.RelationTupleTreeNode {
	return setResult(pb.SetOperationUserset_INTERSECTION, start, children)
}

func Exclusion(start *pb.ObjectAndRelation, children ...*pb.RelationTupleTreeNode) *pb.RelationTupleTreeNode {
	return setResult(pb.SetOperationUserset_EXCLUSION, start, children)
}

type UserSet map[string]struct{}

func (us UserSet) Add(users ...*pb.User) {
	for _, usr := range users {
		us[toKey(usr)] = struct{}{}
	}
}

func (us UserSet) Contains(usr *pb.User) bool {
	_, ok := us[toKey(usr)]
	return ok
}

func (us UserSet) Remove(users ...*pb.User) {
	for _, usr := range users {
		delete(us, toKey(usr))
	}
}

func (us UserSet) ToSlice() []*pb.User {
	toReturn := make([]*pb.User, 0, len(us))
	for key, _ := range us {
		toReturn = append(toReturn, fromKey(key))
	}
	return toReturn
}

func toKey(usr *pb.User) string {
	return fmt.Sprintf("%s %s %s", usr.GetUserset().Namespace, usr.GetUserset().ObjectId, usr.GetUserset().Relation)
}

func fromKey(key string) *pb.User {
	userset := &pb.ObjectAndRelation{}
	fmt.Sscanf(key, "%s %s %s", &userset.Namespace, &userset.ObjectId, &userset.Relation)
	return &pb.User{
		UserOneof: &pb.User_Userset{Userset: userset},
	}
}
