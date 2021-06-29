package graph

import (
	"fmt"

	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

func Simplify(node *v0.RelationTupleTreeNode) []*v0.User {
	switch typed := node.NodeType.(type) {
	case *v0.RelationTupleTreeNode_IntermediateNode:
		switch typed.IntermediateNode.Operation {
		case v0.SetOperationUserset_UNION:
			return SimplifyUnion(typed.IntermediateNode.ChildNodes)
		case v0.SetOperationUserset_INTERSECTION:
			return SimplifyIntersection(typed.IntermediateNode.ChildNodes)
		case v0.SetOperationUserset_EXCLUSION:
			return SimplifyExclusion(typed.IntermediateNode.ChildNodes)
		}
	case *v0.RelationTupleTreeNode_LeafNode:
		var toReturn UserSet = make(map[string]struct{})
		for _, usr := range typed.LeafNode.Users {
			toReturn.Add(usr)
		}
		return toReturn.ToSlice()
	}
	return nil
}

func SimplifyUnion(children []*v0.RelationTupleTreeNode) []*v0.User {
	var toReturn UserSet = make(map[string]struct{})
	for _, child := range children {
		toReturn.Add(Simplify(child)...)
	}
	return toReturn.ToSlice()
}

func SimplifyIntersection(children []*v0.RelationTupleTreeNode) []*v0.User {
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

	toReturn := make([]*v0.User, 0, maxChildren)
	for _, child := range firstChildChildren {
		if inOthers.Contains(child) {
			toReturn = append(toReturn, child)
		}
	}

	return toReturn
}

func SimplifyExclusion(children []*v0.RelationTupleTreeNode) []*v0.User {
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

func Union(start *v0.ObjectAndRelation, children ...*v0.RelationTupleTreeNode) *v0.RelationTupleTreeNode {
	return setResult(v0.SetOperationUserset_UNION, start, children)
}

func Intersection(start *v0.ObjectAndRelation, children ...*v0.RelationTupleTreeNode) *v0.RelationTupleTreeNode {
	return setResult(v0.SetOperationUserset_INTERSECTION, start, children)
}

func Exclusion(start *v0.ObjectAndRelation, children ...*v0.RelationTupleTreeNode) *v0.RelationTupleTreeNode {
	return setResult(v0.SetOperationUserset_EXCLUSION, start, children)
}

type UserSet map[string]struct{}

func (us UserSet) Add(users ...*v0.User) {
	for _, usr := range users {
		us[toKey(usr)] = struct{}{}
	}
}

func (us UserSet) Contains(usr *v0.User) bool {
	_, ok := us[toKey(usr)]
	return ok
}

func (us UserSet) Remove(users ...*v0.User) {
	for _, usr := range users {
		delete(us, toKey(usr))
	}
}

func (us UserSet) ToSlice() []*v0.User {
	toReturn := make([]*v0.User, 0, len(us))
	for key := range us {
		toReturn = append(toReturn, fromKey(key))
	}
	return toReturn
}

func toKey(usr *v0.User) string {
	return fmt.Sprintf("%s %s %s", usr.GetUserset().Namespace, usr.GetUserset().ObjectId, usr.GetUserset().Relation)
}

func fromKey(key string) *v0.User {
	userset := &v0.ObjectAndRelation{}
	fmt.Sscanf(key, "%s %s %s", &userset.Namespace, &userset.ObjectId, &userset.Relation)
	return &v0.User{
		UserOneof: &v0.User_Userset{Userset: userset},
	}
}
