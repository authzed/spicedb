package compiler

import (
	"fmt"

	"github.com/jzelinskie/stringz"

	"github.com/authzed/spicedb/pkg/namespace"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
)

type translationContext struct {
	objectTypePrefix *string
}

func (tctx translationContext) NamespacePath(namespaceName string) (string, error) {
	var prefix, name string
	if err := stringz.SplitExact(namespaceName, "/", &prefix, &name); err != nil {
		if tctx.objectTypePrefix == nil {
			return "", fmt.Errorf("found reference `%s` without prefix", namespaceName)
		}
		prefix = *tctx.objectTypePrefix
		name = namespaceName
	}
	return stringz.Join("/", prefix, name), nil
}

const Ellipsis = "..."

func translate(tctx translationContext, root *dslNode) ([]*v0.NamespaceDefinition, error) {
	definitions := []*v0.NamespaceDefinition{}
	for _, definitionNode := range root.GetChildren() {
		definition, err := translateDefinition(tctx, definitionNode)
		if err != nil {
			return []*v0.NamespaceDefinition{}, err
		}

		definitions = append(definitions, definition)
	}

	return definitions, nil
}

func translateDefinition(tctx translationContext, defNode *dslNode) (*v0.NamespaceDefinition, error) {
	definitionName, err := defNode.GetString(dslshape.NodeDefinitionPredicateName)
	if err != nil {
		return nil, defNode.Errorf("invalid definition name: %w", err)
	}

	relationsAndPermissions := []*v0.Relation{}
	for _, relationOrPermissionNode := range defNode.GetChildren() {
		relationOrPermission, err := translateRelationOrPermission(tctx, relationOrPermissionNode)
		if err != nil {
			return nil, err
		}

		relationsAndPermissions = append(relationsAndPermissions, relationOrPermission)
	}

	nspath, err := tctx.NamespacePath(definitionName)
	if err != nil {
		return nil, defNode.Errorf("%w", err)
	}

	if len(relationsAndPermissions) == 0 {
		return namespace.Namespace(nspath), nil
	}

	return namespace.Namespace(nspath, relationsAndPermissions...), nil
}

func translateRelationOrPermission(tctx translationContext, relOrPermNode *dslNode) (*v0.Relation, error) {
	switch relOrPermNode.GetType() {
	case dslshape.NodeTypeRelation:
		return translateRelation(tctx, relOrPermNode)

	case dslshape.NodeTypePermission:
		return translatePermission(tctx, relOrPermNode)

	default:
		return nil, relOrPermNode.Errorf("unknown definition top-level node type %s", relOrPermNode.GetType())
	}
}

func translateRelation(tctx translationContext, relationNode *dslNode) (*v0.Relation, error) {
	relationName, err := relationNode.GetString(dslshape.NodePredicateName)
	if err != nil {
		return nil, relationNode.Errorf("invalid relation name: %w", err)
	}

	allowedDirectTypes := []*v0.RelationReference{}
	for _, typeRef := range relationNode.List(dslshape.NodeRelationPredicateAllowedTypes) {
		relReferences, err := translateTypeReference(tctx, typeRef)
		if err != nil {
			return nil, err
		}

		allowedDirectTypes = append(allowedDirectTypes, relReferences...)
	}

	return namespace.Relation(relationName, nil, allowedDirectTypes...), nil
}

func translatePermission(tctx translationContext, permissionNode *dslNode) (*v0.Relation, error) {
	permissionName, err := permissionNode.GetString(dslshape.NodePredicateName)
	if err != nil {
		return nil, permissionNode.Errorf("invalid permission name: %w", err)
	}

	expressionNode, err := permissionNode.Lookup(dslshape.NodePermissionPredicateComputeExpression)
	if err != nil {
		return nil, permissionNode.Errorf("invalid permission expression: %w", err)
	}

	rewrite, err := translateExpression(tctx, expressionNode)
	if err != nil {
		return nil, err
	}

	return namespace.Relation(permissionName, rewrite), nil
}

func translateBinary(tctx translationContext, expressionNode *dslNode) (*v0.SetOperation_Child, *v0.SetOperation_Child, error) {
	leftChild, err := expressionNode.Lookup(dslshape.NodeExpressionPredicateLeftExpr)
	if err != nil {
		return nil, nil, err
	}

	rightChild, err := expressionNode.Lookup(dslshape.NodeExpressionPredicateRightExpr)
	if err != nil {
		return nil, nil, err
	}

	leftOperation, err := translateExpressionOperation(tctx, leftChild)
	if err != nil {
		return nil, nil, err
	}

	rightOperation, err := translateExpressionOperation(tctx, rightChild)
	if err != nil {
		return nil, nil, err
	}

	return leftOperation, rightOperation, nil
}

func translateExpression(tctx translationContext, expressionNode *dslNode) (*v0.UsersetRewrite, error) {
	switch expressionNode.GetType() {
	case dslshape.NodeTypeUnionExpression:
		leftOperation, rightOperation, err := translateBinary(tctx, expressionNode)
		if err != nil {
			return nil, err
		}
		return namespace.Union(leftOperation, rightOperation), nil

	case dslshape.NodeTypeIntersectExpression:
		leftOperation, rightOperation, err := translateBinary(tctx, expressionNode)
		if err != nil {
			return nil, err
		}
		return namespace.Intersection(leftOperation, rightOperation), nil

	case dslshape.NodeTypeExclusionExpression:
		leftOperation, rightOperation, err := translateBinary(tctx, expressionNode)
		if err != nil {
			return nil, err
		}
		return namespace.Exclusion(leftOperation, rightOperation), nil

	default:
		op, err := translateExpressionOperation(tctx, expressionNode)
		if err != nil {
			return nil, err
		}

		return namespace.Union(op), nil
	}
}

func translateExpressionOperation(tctx translationContext, expressionOpNode *dslNode) (*v0.SetOperation_Child, error) {
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
		rewrite, err := translateExpression(tctx, expressionOpNode)
		if err != nil {
			return nil, err
		}
		return namespace.Rewrite(rewrite), nil

	default:
		return nil, expressionOpNode.Errorf("unknown expression node type %s", expressionOpNode.GetType())
	}
}

func translateTypeReference(tctx translationContext, typeRefNode *dslNode) ([]*v0.RelationReference, error) {
	switch typeRefNode.GetType() {
	case dslshape.NodeTypeTypeReference:
		references := []*v0.RelationReference{}
		for _, subRefNode := range typeRefNode.List(dslshape.NodeTypeReferencePredicateType) {
			subReferences, err := translateTypeReference(tctx, subRefNode)
			if err != nil {
				return []*v0.RelationReference{}, err
			}

			references = append(references, subReferences...)
		}
		return references, nil

	case dslshape.NodeTypeSpecificTypeReference:
		ref, err := translateSpecificTypeReference(tctx, typeRefNode)
		if err != nil {
			return []*v0.RelationReference{}, err
		}
		return []*v0.RelationReference{ref}, nil

	default:
		return nil, typeRefNode.Errorf("unknown type ref node type %s", typeRefNode.GetType())
	}
}

func translateSpecificTypeReference(tctx translationContext, typeRefNode *dslNode) (*v0.RelationReference, error) {
	typePath, err := typeRefNode.GetString(dslshape.NodeSpecificReferencePredicateType)
	if err != nil {
		return nil, typeRefNode.Errorf("invalid type name: %w", err)
	}

	nspath, err := tctx.NamespacePath(typePath)
	if err != nil {
		return nil, typeRefNode.Errorf("%w", err)
	}

	relationName := Ellipsis
	if typeRefNode.Has(dslshape.NodeSpecificReferencePredicateRelation) {
		relationName, err = typeRefNode.GetString(dslshape.NodeSpecificReferencePredicateRelation)
		if err != nil {
			return nil, typeRefNode.Errorf("invalid type relation: %w", err)
		}
	}

	return &v0.RelationReference{
		Namespace: nspath,
		Relation:  relationName,
	}, nil
}
