package graph

import (
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	"github.com/authzed/spicedb/pkg/tuple"
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

// Simplify simplifes a relation tuple tree node into the set of users that would pass a Check.
func Simplify(node *v0.RelationTupleTreeNode) SubjectSet {
	switch typed := node.NodeType.(type) {
	case *v0.RelationTupleTreeNode_IntermediateNode:
		switch typed.IntermediateNode.Operation {
		case v0.SetOperationUserset_UNION:
			return simplifyUnion(typed.IntermediateNode.ChildNodes)
		case v0.SetOperationUserset_INTERSECTION:
			return simplifyIntersection(typed.IntermediateNode.ChildNodes)
		case v0.SetOperationUserset_EXCLUSION:
			return simplifyExclusion(typed.IntermediateNode.ChildNodes)
		}
	case *v0.RelationTupleTreeNode_LeafNode:
		toReturn := NewSubjectSet()
		for _, usr := range typed.LeafNode.Users {
			toReturn.Add(usr.GetUserset())
		}
		return toReturn
	}
	return nil
}

func simplifyUnion(children []*v0.RelationTupleTreeNode) SubjectSet {
	toReturn := NewSubjectSet()
	for _, child := range children {
		toReturn.AddFrom(Simplify(child))
	}
	return toReturn
}

func simplifyIntersection(children []*v0.RelationTupleTreeNode) SubjectSet {
	firstChildChildren := Simplify(children[0])
	if len(children) == 1 {
		return firstChildChildren
	}

	return firstChildChildren.Intersect(simplifyIntersection(children[1:]))
}

func simplifyExclusion(children []*v0.RelationTupleTreeNode) SubjectSet {
	firstChildChildren := Simplify(children[0])
	if len(children) == 1 || len(firstChildChildren) == 0 {
		return firstChildChildren
	}

	toReturn := NewSubjectSet()
	toReturn.AddFrom(firstChildChildren)
	for _, child := range children[1:] {
		toReturn = toReturn.Exclude(Simplify(child))
	}

	return toReturn
}

func isWildcard(subject *v0.ObjectAndRelation) bool {
	return subject.ObjectId == tuple.PublicWildcard
}

// SubjectSet is a set of subjects.
type SubjectSet map[string]struct{}

// NewSubjectSet returns a new subject set, optionally populated with the given subjects to start.
func NewSubjectSet(subjects ...*v0.ObjectAndRelation) SubjectSet {
	var toReturn SubjectSet = make(map[string]struct{})
	toReturn.Add(subjects...)
	return toReturn
}

// AddFrom adds the subjects found in the other set to this set.
func (ss SubjectSet) AddFrom(otherSet SubjectSet) {
	for subStr := range otherSet {
		ss[subStr] = struct{}{}
	}
}

// RemoveFrom removes all the subjects found in the other set from this set. Handles wildcards
// as well.
func (ss SubjectSet) RemoveFrom(otherSet SubjectSet) {
	for subStr := range otherSet {
		ss.Remove(fromKey(subStr))
	}
}

// Add adds the given subjects to the subject set.
func (ss SubjectSet) Add(subjects ...*v0.ObjectAndRelation) {
	for _, sub := range subjects {
		ss[toKey(sub)] = struct{}{}
	}
}

// Contains indicates whether the subject set contains the given user *directly*. Note that this
// will *not* match wildcards (use Matches for that).
func (ss SubjectSet) Contains(subject *v0.ObjectAndRelation) bool {
	_, ok := ss[toKey(subject)]
	return ok
}

// Remove removes the given subject(s) from the set. If the subject is a wildcard, all matching
// subjects will be removed.
func (ss SubjectSet) Remove(subjects ...*v0.ObjectAndRelation) {
	for _, subject := range subjects {
		delete(ss, toKey(subject))

		// Delete any entries matching the wildcard, if applicable.
		if isWildcard(subject) {
			// remove any matching types.
			for key := range ss {
				current := fromKey(key)
				if current.Namespace == subject.Namespace {
					delete(ss, key)
				}
			}
		}
	}
}

// WithType returns all subjects in the set with the given object type.
func (ss SubjectSet) WithType(objectType string) []*v0.ObjectAndRelation {
	toReturn := make([]*v0.ObjectAndRelation, 0, len(ss))
	for key := range ss {
		current := fromKey(key)
		if current.Namespace == objectType {
			toReturn = append(toReturn, current)
		}
	}
	return toReturn
}

// Exclude excludes the members of the other set from this set, including handling of wildcards.
func (ss SubjectSet) Exclude(otherSet SubjectSet) SubjectSet {
	newSet := NewSubjectSet()
	newSet.AddFrom(ss)
	newSet.RemoveFrom(otherSet)
	return newSet
}

// Intersect returns a new SubjectSet that is the intersection of this set with the other specified.
// Handles wildcard subjects automatically.
func (ss SubjectSet) Intersect(otherSet SubjectSet) SubjectSet {
	newSet := NewSubjectSet()
	for key := range ss {
		current := fromKey(key)

		// Add directly if shared by both.
		if otherSet.Contains(current) {
			newSet.Add(current)
		}

		// If the current is a wildcard, Add any matching.
		if isWildcard(current) {
			newSet.Add(otherSet.WithType(current.Namespace)...)
		}
	}

	for key := range otherSet {
		// If the current is a wildcard, Add any matching.
		current := fromKey(key)
		if isWildcard(current) {
			newSet.Add(ss.WithType(current.Namespace)...)
		}
	}

	return newSet
}

// ToSlice converts the SubjectSet into a slice of subjects.
func (ss SubjectSet) ToSlice() []*v0.ObjectAndRelation {
	toReturn := make([]*v0.ObjectAndRelation, 0, len(ss))
	for key := range ss {
		toReturn = append(toReturn, fromKey(key))
	}
	return toReturn
}

func toKey(subject *v0.ObjectAndRelation) string {
	return fmt.Sprintf("%s %s %s", subject.Namespace, subject.ObjectId, subject.Relation)
}

func fromKey(key string) *v0.ObjectAndRelation {
	subject := &v0.ObjectAndRelation{}
	fmt.Sscanf(key, "%s %s %s", &subject.Namespace, &subject.ObjectId, &subject.Relation)
	return subject
}
