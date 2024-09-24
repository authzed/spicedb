package namespace

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// ExpressionChangeType defines the type of expression changes.
type ExpressionChangeType string

const (
	// ExpressionUnchanged indicates that the expression was unchanged.
	ExpressionUnchanged ExpressionChangeType = "expression-unchanged"

	// ExpressionOperationChanged indicates that the operation type of the expression was changed.
	ExpressionOperationChanged ExpressionChangeType = "operation-changed"

	// ExpressionChildrenChanged indicates that the children of the expression were changed.
	ExpressionChildrenChanged ExpressionChangeType = "children-changed"

	// ExpressionOperationExpanded indicates that the operation type of the expression was expanded
	// from a union of a single child to multiple children under a union, intersection or another
	// operation.
	ExpressionOperationExpanded ExpressionChangeType = "operation-expanded"
)

// ExpressionDiff holds the diff between two expressions.
type ExpressionDiff struct {
	existing *core.UsersetRewrite
	updated  *core.UsersetRewrite
	change   ExpressionChangeType

	childDiffs []*OperationDiff
}

// Existing returns the existing expression, if any.
func (ed *ExpressionDiff) Existing() *core.UsersetRewrite {
	return ed.existing
}

// Updated returns the updated expression, if any.
func (ed *ExpressionDiff) Updated() *core.UsersetRewrite {
	return ed.updated
}

// Change returns the type of change that occurred.
func (ed *ExpressionDiff) Change() ExpressionChangeType {
	return ed.change
}

// ChildDiffs returns the child diffs, if any.
func (ed *ExpressionDiff) ChildDiffs() []*OperationDiff {
	return ed.childDiffs
}

// SetOperationChangeType defines the type of set operation changes.
type SetOperationChangeType string

const (
	// OperationUnchanged indicates that the set operation was unchanged.
	OperationUnchanged SetOperationChangeType = "operation-changed"

	// OperationAdded indicates that a set operation was added.
	OperationAdded SetOperationChangeType = "operation-added"

	// OperationRemoved indicates that a set operation was removed.
	OperationRemoved SetOperationChangeType = "operation-removed"

	// OperationTypeChanged indicates that the type of set operation was changed.
	OperationTypeChanged SetOperationChangeType = "operation-type-changed"

	// OperationComputedUsersetChanged indicates that the computed userset of the operation was changed.
	OperationComputedUsersetChanged SetOperationChangeType = "operation-computed-userset-changed"

	// OperationTuplesetChanged indicates that the tupleset of the operation was changed.
	OperationTuplesetChanged SetOperationChangeType = "operation-tupleset-changed"

	// OperationChildExpressionChanged indicates that the child expression of the operation was changed.
	OperationChildExpressionChanged SetOperationChangeType = "operation-child-expression-changed"
)

// OperationDiff holds the diff between two set operations.
type OperationDiff struct {
	existing      *core.SetOperation_Child
	updated       *core.SetOperation_Child
	change        SetOperationChangeType
	childExprDiff *ExpressionDiff
}

// Existing returns the existing set operation, if any.
func (od *OperationDiff) Existing() *core.SetOperation_Child {
	return od.existing
}

// Updated returns the updated set operation, if any.
func (od *OperationDiff) Updated() *core.SetOperation_Child {
	return od.updated
}

// Change returns the type of change that occurred.
func (od *OperationDiff) Change() SetOperationChangeType {
	return od.change
}

// ChildExpressionDiff returns the child expression diff, if any.
func (od *OperationDiff) ChildExpressionDiff() *ExpressionDiff {
	return od.childExprDiff
}

// DiffExpressions diffs two expressions.
func DiffExpressions(existing *core.UsersetRewrite, updated *core.UsersetRewrite) (*ExpressionDiff, error) {
	// Check for a difference in the operation type.
	var existingType string
	var existingOperation *core.SetOperation
	var updatedType string
	var updatedOperation *core.SetOperation

	switch t := existing.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		existingType = "union"
		existingOperation = t.Union

	case *core.UsersetRewrite_Intersection:
		existingType = "intersection"
		existingOperation = t.Intersection

	case *core.UsersetRewrite_Exclusion:
		existingType = "exclusion"
		existingOperation = t.Exclusion

	default:
		return nil, spiceerrors.MustBugf("unknown operation type %T", existing.RewriteOperation)
	}

	switch t := updated.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		updatedType = "union"
		updatedOperation = t.Union

	case *core.UsersetRewrite_Intersection:
		updatedType = "intersection"
		updatedOperation = t.Intersection

	case *core.UsersetRewrite_Exclusion:
		updatedType = "exclusion"
		updatedOperation = t.Exclusion

	default:
		return nil, spiceerrors.MustBugf("unknown operation type %T", updated.RewriteOperation)
	}

	childChangeKind := ExpressionChildrenChanged
	if existingType != updatedType {
		// If the expression has changed from a union with a single child, then
		// treat this as a special case, since there wasn't really an operation
		// before.
		if existingType != "union" || len(existingOperation.Child) != 1 {
			return &ExpressionDiff{
				existing: existing,
				updated:  updated,
				change:   ExpressionOperationChanged,
			}, nil
		}

		childChangeKind = ExpressionOperationExpanded
	}

	childDiffs := make([]*OperationDiff, 0, abs(len(updatedOperation.Child)-len(existingOperation.Child)))
	if len(existingOperation.Child) < len(updatedOperation.Child) {
		for _, updatedChild := range updatedOperation.Child[len(existingOperation.Child):] {
			childDiffs = append(childDiffs, &OperationDiff{
				change:  OperationAdded,
				updated: updatedChild,
			})
		}
	}

	if len(existingOperation.Child) > len(updatedOperation.Child) {
		for _, existingChild := range existingOperation.Child[len(updatedOperation.Child):] {
			childDiffs = append(childDiffs, &OperationDiff{
				change:   OperationRemoved,
				existing: existingChild,
			})
		}
	}

	for i := 0; i < len(existingOperation.Child) && i < len(updatedOperation.Child); i++ {
		childDiff, err := compareChildren(existingOperation.Child[i], updatedOperation.Child[i])
		if err != nil {
			return nil, err
		}

		if childDiff.change != OperationUnchanged {
			childDiffs = append(childDiffs, childDiff)
		}
	}

	if len(childDiffs) > 0 {
		return &ExpressionDiff{
			existing:   existing,
			updated:    updated,
			change:     childChangeKind,
			childDiffs: childDiffs,
		}, nil
	}

	return &ExpressionDiff{
		existing: existing,
		updated:  updated,
		change:   ExpressionUnchanged,
	}, nil
}

func abs(i int) int {
	if i < 0 {
		return -i
	}
	return i
}

func compareChildren(existing *core.SetOperation_Child, updated *core.SetOperation_Child) (*OperationDiff, error) {
	existingType, err := typeOfSetOperationChild(existing)
	if err != nil {
		return nil, err
	}

	updatedType, err := typeOfSetOperationChild(updated)
	if err != nil {
		return nil, err
	}

	if existingType != updatedType {
		return &OperationDiff{
			existing: existing,
			updated:  updated,
			change:   OperationTypeChanged,
		}, nil
	}

	switch existingType {
	case "usersetrewrite":
		childDiff, err := DiffExpressions(existing.GetUsersetRewrite(), updated.GetUsersetRewrite())
		if err != nil {
			return nil, err
		}

		if childDiff.change != ExpressionUnchanged {
			return &OperationDiff{
				existing:      existing,
				updated:       updated,
				change:        OperationChildExpressionChanged,
				childExprDiff: childDiff,
			}, nil
		}

		return &OperationDiff{
			existing: existing,
			updated:  updated,
			change:   OperationUnchanged,
		}, nil

	case "computed":
		if existing.GetComputedUserset().Relation != updated.GetComputedUserset().Relation {
			return &OperationDiff{
				existing: existing,
				updated:  updated,
				change:   OperationComputedUsersetChanged,
			}, nil
		}

		return &OperationDiff{
			existing: existing,
			updated:  updated,
			change:   OperationUnchanged,
		}, nil

	case "_this":
		return &OperationDiff{
			existing: existing,
			updated:  updated,
			change:   OperationUnchanged,
		}, nil

	case "nil":
		return &OperationDiff{
			existing: existing,
			updated:  updated,
			change:   OperationUnchanged,
		}, nil

	case "ttu":
		existingTTU := existing.GetTupleToUserset()
		updatedTTU := updated.GetTupleToUserset()

		if existingTTU.GetComputedUserset().Relation != updatedTTU.GetComputedUserset().Relation {
			return &OperationDiff{
				existing: existing,
				updated:  updated,
				change:   OperationComputedUsersetChanged,
			}, nil
		}

		if existingTTU.Tupleset.Relation != updatedTTU.Tupleset.Relation {
			return &OperationDiff{
				existing: existing,
				updated:  updated,
				change:   OperationTuplesetChanged,
			}, nil
		}

		return &OperationDiff{
			existing: existing,
			updated:  updated,
			change:   OperationUnchanged,
		}, nil

	case "anyttu":
		fallthrough

	case "intersectionttu":
		existingTTU := existing.GetFunctionedTupleToUserset()
		updatedTTU := updated.GetFunctionedTupleToUserset()

		if existingTTU.GetComputedUserset().Relation != updatedTTU.GetComputedUserset().Relation {
			return &OperationDiff{
				existing: existing,
				updated:  updated,
				change:   OperationComputedUsersetChanged,
			}, nil
		}

		if existingTTU.Tupleset.Relation != updatedTTU.Tupleset.Relation {
			return &OperationDiff{
				existing: existing,
				updated:  updated,
				change:   OperationTuplesetChanged,
			}, nil
		}

		return &OperationDiff{
			existing: existing,
			updated:  updated,
			change:   OperationUnchanged,
		}, nil

	default:
		return nil, spiceerrors.MustBugf("unknown child type %s", existingType)
	}
}

func typeOfSetOperationChild(child *core.SetOperation_Child) (string, error) {
	switch t := child.ChildType.(type) {
	case *core.SetOperation_Child_XThis:
		return "_this", nil

	case *core.SetOperation_Child_ComputedUserset:
		return "computed", nil

	case *core.SetOperation_Child_UsersetRewrite:
		return "usersetrewrite", nil

	case *core.SetOperation_Child_TupleToUserset:
		return "ttu", nil

	case *core.SetOperation_Child_FunctionedTupleToUserset:
		switch t.FunctionedTupleToUserset.Function {
		case core.FunctionedTupleToUserset_FUNCTION_UNSPECIFIED:
			return "", spiceerrors.MustBugf("function type unspecified")

		case core.FunctionedTupleToUserset_FUNCTION_ANY:
			return "anyttu", nil

		case core.FunctionedTupleToUserset_FUNCTION_ALL:
			return "intersectionttu", nil

		default:
			return "", spiceerrors.MustBugf("unknown function type %v", t.FunctionedTupleToUserset.Function)
		}

	case *core.SetOperation_Child_XNil:
		return "nil", nil

	default:
		return "", spiceerrors.MustBugf("unknown child type %T", t)
	}
}
