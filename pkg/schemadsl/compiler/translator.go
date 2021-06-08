package compiler

import (
	"fmt"

	"github.com/jzelinskie/stringz"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
)

type translationContext struct {
	objectTypePrefix string
}

func (tc translationContext) NamespacePath(namespaceName string) string {
	return tc.objectTypePrefix + "/" + namespaceName
}

const Ellipsis = "..."

func translate(root *dslNode, tctx translationContext) ([]*pb.NamespaceDefinition, error) {
	definitions := []*pb.NamespaceDefinition{}
	for _, definitionNode := range root.GetChildren() {
		definition, err := translateDefinition(definitionNode, tctx)
		if err != nil {
			return []*pb.NamespaceDefinition{}, err
		}

		definitions = append(definitions, definition)
	}

	return definitions, nil
}

func translateDefinition(defNode *dslNode, tctx translationContext) (*pb.NamespaceDefinition, error) {
	definitionName, err := defNode.GetString(dslshape.NodeDefinitionPredicateName)
	if err != nil {
		return nil, defNode.Errorf("invalid definition name: %w", err)
	}

	relationsAndPermissions := []*pb.Relation{}
	for _, relationOrPermissionNode := range defNode.GetChildren() {
		relationOrPermission, err := translateRelationOrPermission(relationOrPermissionNode, tctx)
		if err != nil {
			return nil, err
		}

		relationsAndPermissions = append(relationsAndPermissions, relationOrPermission)
	}

	if len(relationsAndPermissions) == 0 {
		return namespace.Namespace(tctx.NamespacePath(definitionName)), nil
	}

	return namespace.Namespace(tctx.NamespacePath(definitionName), relationsAndPermissions...), nil
}

func translateRelationOrPermission(relOrPermNode *dslNode, tctx translationContext) (*pb.Relation, error) {
	switch relOrPermNode.GetType() {
	case dslshape.NodeTypeRelation:
		return translateRelation(relOrPermNode, tctx)

	case dslshape.NodeTypePermission:
		return translatePermission(relOrPermNode, tctx)

	default:
		return nil, relOrPermNode.Errorf("unknown definition top-level node type %s", relOrPermNode.GetType())
	}
}

func translateRelation(relationNode *dslNode, tctx translationContext) (*pb.Relation, error) {
	relationName, err := relationNode.GetString(dslshape.NodePredicateName)
	if err != nil {
		return nil, relationNode.Errorf("invalid relation name: %w", err)
	}

	allowedDirectTypes := []*pb.RelationReference{}
	for _, typeRef := range relationNode.List(dslshape.NodeRelationPredicateAllowedTypes) {
		relReferences, err := translateTypeReference(typeRef, tctx)
		if err != nil {
			return nil, err
		}

		allowedDirectTypes = append(allowedDirectTypes, relReferences...)
	}

	return namespace.Relation(relationName, nil, allowedDirectTypes...), nil
}

func translatePermission(permissionNode *dslNode, tctx translationContext) (*pb.Relation, error) {
	permissionName, err := permissionNode.GetString(dslshape.NodePredicateName)
	if err != nil {
		return nil, permissionNode.Errorf("invalid permission name: %w", err)
	}

	expressionNode, err := permissionNode.Lookup(dslshape.NodePermissionPredicateComputeExpression)
	if err != nil {
		return nil, permissionNode.Errorf("invalid permission expression: %w", err)
	}

	rewrite, err := translateExpression(expressionNode, tctx)
	if err != nil {
		return nil, err
	}

	return namespace.Relation(permissionName, rewrite), nil
}

func translateBinary(expressionNode *dslNode, tctx translationContext) (*pb.SetOperation_Child, *pb.SetOperation_Child, error) {
	leftChild, err := expressionNode.Lookup(dslshape.NodeExpressionPredicateLeftExpr)
	if err != nil {
		return nil, nil, err
	}

	rightChild, err := expressionNode.Lookup(dslshape.NodeExpressionPredicateRightExpr)
	if err != nil {
		return nil, nil, err
	}

	leftOperation, err := translateExpressionOperation(leftChild, tctx)
	if err != nil {
		return nil, nil, err
	}

	rightOperation, err := translateExpressionOperation(rightChild, tctx)
	if err != nil {
		return nil, nil, err
	}

	return leftOperation, rightOperation, nil
}

func translateExpression(expressionNode *dslNode, tctx translationContext) (*pb.UsersetRewrite, error) {
	switch expressionNode.GetType() {
	case dslshape.NodeTypeUnionExpression:
		leftOperation, rightOperation, err := translateBinary(expressionNode, tctx)
		if err != nil {
			return nil, err
		}
		return namespace.Union(leftOperation, rightOperation), nil

	case dslshape.NodeTypeIntersectExpression:
		leftOperation, rightOperation, err := translateBinary(expressionNode, tctx)
		if err != nil {
			return nil, err
		}
		return namespace.Intersection(leftOperation, rightOperation), nil

	case dslshape.NodeTypeExclusionExpression:
		leftOperation, rightOperation, err := translateBinary(expressionNode, tctx)
		if err != nil {
			return nil, err
		}
		return namespace.Exclusion(leftOperation, rightOperation), nil

	default:
		op, err := translateExpressionOperation(expressionNode, tctx)
		if err != nil {
			return nil, err
		}

		return namespace.Union(op), nil
	}
}

func translateExpressionOperation(expressionOpNode *dslNode, tctx translationContext) (*pb.SetOperation_Child, error) {
	switch expressionOpNode.GetType() {
	case dslshape.NodeTypeIdentifier:
		referencedRelationName, err := expressionOpNode.GetString(dslshape.NodeIdentiferPredicateValue)
		if err != nil {
			return nil, err
		}

		return namespace.ComputedUserset(referencedRelationName), nil

	case dslshape.NodeTypeArrowExpression:
		leftChild, err := expressionOpNode.Lookup(dslshape.NodeExpressionPredicateLeftExpr)
		if err != nil {
			return nil, err
		}

		rightChild, err := expressionOpNode.Lookup(dslshape.NodeExpressionPredicateRightExpr)
		if err != nil {
			return nil, err
		}

		if leftChild.GetType() != dslshape.NodeTypeIdentifier {
			return nil, leftChild.Errorf("Nested arrows not yet supported")
		}

		tuplesetRelation, err := leftChild.GetString(dslshape.NodeIdentiferPredicateValue)
		if err != nil {
			return nil, err
		}

		usersetRelation, err := rightChild.GetString(dslshape.NodeIdentiferPredicateValue)
		if err != nil {
			return nil, err
		}

		return namespace.TupleToUserset(tuplesetRelation, usersetRelation), nil

	case dslshape.NodeTypeUnionExpression:
		fallthrough

	case dslshape.NodeTypeIntersectExpression:
		fallthrough

	case dslshape.NodeTypeExclusionExpression:
		rewrite, err := translateExpression(expressionOpNode, tctx)
		if err != nil {
			return nil, err
		}
		return namespace.Rewrite(rewrite), nil

	default:
		return nil, expressionOpNode.Errorf("unknown expression node type %s", expressionOpNode.GetType())
	}
}

func translateTypeReference(typeRefNode *dslNode, tctx translationContext) ([]*pb.RelationReference, error) {
	switch typeRefNode.GetType() {
	case dslshape.NodeTypeTypeReference:
		references := []*pb.RelationReference{}
		for _, subRefNode := range typeRefNode.List(dslshape.NodeTypeReferencePredicateType) {
			subReferences, err := translateTypeReference(subRefNode, tctx)
			if err != nil {
				return []*pb.RelationReference{}, err
			}

			references = append(references, subReferences...)
		}
		return references, nil

	case dslshape.NodeTypeSpecificTypeReference:
		ref, err := translateSpecificTypeReference(typeRefNode, tctx)
		if err != nil {
			return []*pb.RelationReference{}, err
		}
		return []*pb.RelationReference{ref}, nil

	default:
		return nil, typeRefNode.Errorf("unknown type ref node type %s", typeRefNode.GetType())
	}
}

func translateSpecificTypeReference(typeRefNode *dslNode, tctx translationContext) (*pb.RelationReference, error) {
	typePath, err := typeRefNode.GetString(dslshape.NodeSpecificReferencePredicateType)
	if err != nil {
		return nil, typeRefNode.Errorf("invalid type name: %w", err)
	}

	var typePrefix, typeName string
	if err := stringz.SplitExact(typePath, "/", &typePrefix, &typeName); err != nil {
		typePrefix = tctx.objectTypePrefix
		typeName = typePath
	}

	relationName := Ellipsis
	if typeRefNode.Has(dslshape.NodeSpecificReferencePredicateRelation) {
		relationName, err = typeRefNode.GetString(dslshape.NodeSpecificReferencePredicateRelation)
		if err != nil {
			return nil, typeRefNode.Errorf("invalid type relation: %w", err)
		}
	}

	return &pb.RelationReference{
		Namespace: fmt.Sprintf("%s/%s", typePrefix, typeName),
		Relation:  relationName,
	}, nil
}
