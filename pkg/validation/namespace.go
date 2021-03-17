package validation

import (
	"errors"
	"fmt"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const errInvalidNamespace = "invalid namespace: %w"

var (
	ErrUnknownRewriteOperation      = errors.New("unknown rewrite operation")
	ErrMissingChildren              = errors.New("set operation has no children")
	ErrUnknownSetChildType          = errors.New("unknown set operation child type")
	ErrNilDefinition                = errors.New("required definition was undefined/nil")
	ErrTupleToUsersetBadRelation    = errors.New("tuple to userset bad relation")
	ErrTupleToUsersetMissingUserset = errors.New("tuple to userset missing computed userset")
	ErrComputedUsersetObject        = errors.New("unknown computed userset object type")
)

// NamespaceConfig validates the provided namespace configuration.
func NamespaceConfig(ns *pb.NamespaceDefinition) error {
	if ns == nil {
		return fmt.Errorf(errInvalidNamespace, ErrNilDefinition)
	}

	if err := NamespaceName(ns.Name); err != nil {
		return fmt.Errorf(errInvalidNamespace, err)
	}

	for _, relation := range ns.Relation {
		if err := RelationName(relation.Name); err != nil {
			return fmt.Errorf(errInvalidNamespace, err)
		}

		if relation.UsersetRewrite != nil {
			if err := usersetRewrite(relation.UsersetRewrite); err != nil {
				return fmt.Errorf(errInvalidNamespace, err)
			}
		}
	}

	return nil
}

func usersetRewrite(rewrite *pb.UsersetRewrite) error {
	if rewrite == nil {
		return ErrNilDefinition
	}

	switch rw := rewrite.RewriteOperation.(type) {
	case *pb.UsersetRewrite_Union:
		return setOperation(rw.Union)
	case *pb.UsersetRewrite_Intersection:
		return setOperation(rw.Intersection)
	case *pb.UsersetRewrite_Exclusion:
		return setOperation(rw.Exclusion)
	default:
		return ErrUnknownRewriteOperation
	}
}

func setOperation(op *pb.SetOperation) error {
	if op == nil {
		return ErrNilDefinition
	}

	if len(op.Child) == 0 {
		return ErrMissingChildren
	}

	for _, child := range op.Child {
		if child == nil {
			return ErrNilDefinition
		}

		switch ch := child.ChildType.(type) {
		case *pb.SetOperation_Child_UsersetRewrite:
			if err := usersetRewrite(ch.UsersetRewrite); err != nil {
				return err
			}
		case *pb.SetOperation_Child_TupleToUserset:
			if err := tupleToUserset(ch.TupleToUserset); err != nil {
				return err
			}
		case *pb.SetOperation_Child_ComputedUserset:
			if err := computedUserset(ch.ComputedUserset); err != nil {
				return err
			}
		case *pb.SetOperation_Child_XThis:
			// Intentionally blank
		default:
			return ErrUnknownSetChildType
		}
	}

	return nil
}

func tupleToUserset(ttu *pb.TupleToUserset) error {
	if ttu == nil {
		return ErrNilDefinition
	}

	if ttu.Tupleset == nil || RelationName(ttu.Tupleset.Relation) != nil {
		return ErrTupleToUsersetBadRelation
	}

	if ttu.ComputedUserset == nil {
		return ErrTupleToUsersetMissingUserset
	}

	return computedUserset(ttu.ComputedUserset)
}

func computedUserset(cu *pb.ComputedUserset) error {
	if cu == nil {
		return ErrNilDefinition
	}

	if err := RelationName(cu.Relation); err != nil {
		return err
	}

	if cu.Object != pb.ComputedUserset_TUPLE_OBJECT && cu.Object != pb.ComputedUserset_TUPLE_USERSET_OBJECT {
		return ErrComputedUsersetObject
	}

	return nil
}
