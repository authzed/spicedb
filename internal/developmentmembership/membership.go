package developmentmembership

import (
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/spiceerrors"
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
func (ms *Set) AddExpansion(onr tuple.ObjectAndRelation, expansion *core.RelationTupleTreeNode) (FoundSubjects, bool, error) {
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
func AccessibleExpansionSubjects(treeNode *core.RelationTupleTreeNode) (*TrackingSubjectSet, error) {
	return populateFoundSubjects(tuple.FromCoreObjectAndRelation(treeNode.Expanded), treeNode)
}

func populateFoundSubjects(rootONR tuple.ObjectAndRelation, treeNode *core.RelationTupleTreeNode) (*TrackingSubjectSet, error) {
	resource := rootONR
	if treeNode.Expanded != nil {
		resource = tuple.FromCoreObjectAndRelation(treeNode.Expanded)
	}

	switch typed := treeNode.NodeType.(type) {
	case *core.RelationTupleTreeNode_IntermediateNode:
		switch typed.IntermediateNode.Operation {
		case core.SetOperationUserset_UNION:
			toReturn := NewTrackingSubjectSet()
			for _, child := range typed.IntermediateNode.ChildNodes {
				tss, err := populateFoundSubjects(resource, child)
				if err != nil {
					return nil, err
				}

				err = toReturn.AddFrom(tss)
				if err != nil {
					return nil, err
				}
			}

			toReturn.ApplyParentCaveatExpression(treeNode.CaveatExpression)
			return toReturn, nil

		case core.SetOperationUserset_INTERSECTION:
			if len(typed.IntermediateNode.ChildNodes) == 0 {
				return nil, fmt.Errorf("found intersection with no children")
			}

			firstChildSet, err := populateFoundSubjects(rootONR, typed.IntermediateNode.ChildNodes[0])
			if err != nil {
				return nil, err
			}

			toReturn := NewTrackingSubjectSet()
			err = toReturn.AddFrom(firstChildSet)
			if err != nil {
				return nil, err
			}

			for _, child := range typed.IntermediateNode.ChildNodes[1:] {
				childSet, err := populateFoundSubjects(rootONR, child)
				if err != nil {
					return nil, err
				}

				updated, err := toReturn.Intersect(childSet)
				if err != nil {
					return nil, err
				}

				toReturn = updated
			}

			toReturn.ApplyParentCaveatExpression(treeNode.CaveatExpression)
			return toReturn, nil

		case core.SetOperationUserset_EXCLUSION:
			if len(typed.IntermediateNode.ChildNodes) == 0 {
				return nil, fmt.Errorf("found exclusion with no children")
			}

			firstChildSet, err := populateFoundSubjects(rootONR, typed.IntermediateNode.ChildNodes[0])
			if err != nil {
				return nil, err
			}

			toReturn := NewTrackingSubjectSet()
			err = toReturn.AddFrom(firstChildSet)
			if err != nil {
				return nil, err
			}

			for _, child := range typed.IntermediateNode.ChildNodes[1:] {
				childSet, err := populateFoundSubjects(rootONR, child)
				if err != nil {
					return nil, err
				}
				toReturn = toReturn.Exclude(childSet)
			}

			toReturn.ApplyParentCaveatExpression(treeNode.CaveatExpression)
			return toReturn, nil

		default:
			return nil, spiceerrors.MustBugf("unknown expand operation")
		}

	case *core.RelationTupleTreeNode_LeafNode:
		toReturn := NewTrackingSubjectSet()
		for _, subject := range typed.LeafNode.Subjects {
			fs := NewFoundSubject(subject)
			err := toReturn.Add(fs)
			if err != nil {
				return nil, err
			}

			fs.resources.Add(resource)
		}

		toReturn.ApplyParentCaveatExpression(treeNode.CaveatExpression)
		return toReturn, nil

	default:
		return nil, spiceerrors.MustBugf("unknown TreeNode type")
	}
}
