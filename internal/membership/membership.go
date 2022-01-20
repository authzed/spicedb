package membership

import (
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	"github.com/authzed/spicedb/pkg/tuple"
)

// Set represents the set of membership for one or more ONRs, based on expansion
// trees.
type Set struct {
	// objectsAndRelations is a map from an ONR (as a string) to the subjects found for that ONR.
	objectsAndRelations map[string]FoundSubjects
}

// SubjectsByONR returns a map from ONR (as a string) to the FoundSubjects for that ONR.
func (ms *Set) SubjectsByONR() map[string]FoundSubjects {
	return ms.objectsAndRelations
}

// NewMembershipSet constructs a new membership set.
//
// NOTE: This is designed solely for the developer API and should *not* be used in any performance
// sensitive code.
func NewMembershipSet() *Set {
	return &Set{
		objectsAndRelations: map[string]FoundSubjects{},
	}
}

// AddExpansion adds the expansion of an ONR to the membership set. Returns false if the ONR was already added.
//
// NOTE: The expansion tree *should* be the fully recursive expansion.
func (ms *Set) AddExpansion(onr *v0.ObjectAndRelation, expansion *v0.RelationTupleTreeNode) (FoundSubjects, bool, error) {
	onrString := tuple.StringONR(onr)
	existing, ok := ms.objectsAndRelations[onrString]
	if ok {
		return existing, false, nil
	}

	tss, err := populateFoundSubjects(onr, expansion)
	if err != nil {
		return FoundSubjects{}, false, err
	}

	fs := tss.ToFoundSubjects()
	ms.objectsAndRelations[onrString] = fs
	return fs, true, nil
}

// AccessibleExpansionSubjects returns a TrackingSubjectSet representing the set of accessible subjects in the expansion.
func AccessibleExpansionSubjects(treeNode *v0.RelationTupleTreeNode) (TrackingSubjectSet, error) {
	return populateFoundSubjects(treeNode.Expanded, treeNode)
}

func populateFoundSubjects(rootONR *v0.ObjectAndRelation, treeNode *v0.RelationTupleTreeNode) (TrackingSubjectSet, error) {
	resource := rootONR
	if treeNode.Expanded != nil {
		resource = treeNode.Expanded
	}

	switch typed := treeNode.NodeType.(type) {
	case *v0.RelationTupleTreeNode_IntermediateNode:
		switch typed.IntermediateNode.Operation {
		case v0.SetOperationUserset_UNION:
			toReturn := NewTrackingSubjectSet()
			for _, child := range typed.IntermediateNode.ChildNodes {
				tss, err := populateFoundSubjects(resource, child)
				if err != nil {
					return nil, err
				}

				toReturn.AddFrom(tss)
			}
			return toReturn, nil

		case v0.SetOperationUserset_INTERSECTION:
			if len(typed.IntermediateNode.ChildNodes) == 0 {
				return nil, fmt.Errorf("found intersection with no children")
			}

			firstChildSet, err := populateFoundSubjects(rootONR, typed.IntermediateNode.ChildNodes[0])
			if err != nil {
				return nil, err
			}

			toReturn := NewTrackingSubjectSet()
			toReturn.AddFrom(firstChildSet)

			for _, child := range typed.IntermediateNode.ChildNodes[1:] {
				childSet, err := populateFoundSubjects(rootONR, child)
				if err != nil {
					return nil, err
				}
				toReturn = toReturn.Intersect(childSet)
			}
			return toReturn, nil

		case v0.SetOperationUserset_EXCLUSION:
			if len(typed.IntermediateNode.ChildNodes) == 0 {
				return nil, fmt.Errorf("found exclusion with no children")
			}

			firstChildSet, err := populateFoundSubjects(rootONR, typed.IntermediateNode.ChildNodes[0])
			if err != nil {
				return nil, err
			}

			toReturn := NewTrackingSubjectSet()
			toReturn.AddFrom(firstChildSet)

			for _, child := range typed.IntermediateNode.ChildNodes[1:] {
				childSet, err := populateFoundSubjects(rootONR, child)
				if err != nil {
					return nil, err
				}
				toReturn = toReturn.Exclude(childSet)
			}

			return toReturn, nil

		default:
			panic("unknown expand operation")
		}

	case *v0.RelationTupleTreeNode_LeafNode:
		toReturn := NewTrackingSubjectSet()
		for _, user := range typed.LeafNode.Users {
			fs := NewFoundSubject(user.GetUserset())
			toReturn.Add(fs)
			fs.relationships.Add(resource)
		}
		return toReturn, nil

	default:
		panic("unknown TreeNode type")
	}
}
