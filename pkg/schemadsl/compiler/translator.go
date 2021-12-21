package compiler

import (
	"bufio"
	"fmt"
	"strings"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/jzelinskie/stringz"

	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
)

type translationContext struct {
	objectTypePrefix *string
}

func (tctx translationContext) namespacePath(namespaceName string) (string, error) {
	var prefix, name string
	if err := stringz.SplitExact(namespaceName, "/", &prefix, &name); err != nil {
		if tctx.objectTypePrefix == nil {
			return "", fmt.Errorf("found reference `%s` without prefix", namespaceName)
		}
		prefix = *tctx.objectTypePrefix
		name = namespaceName
	}

	if prefix == "" {
		return name, nil
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
		if relationOrPermissionNode.GetType() == dslshape.NodeTypeComment {
			continue
		}

		relationOrPermission, err := translateRelationOrPermission(tctx, relationOrPermissionNode)
		if err != nil {
			return nil, err
		}

		relationsAndPermissions = append(relationsAndPermissions, relationOrPermission)
	}

	nspath, err := tctx.namespacePath(definitionName)
	if err != nil {
		return nil, defNode.Errorf("%w", err)
	}

	if len(relationsAndPermissions) == 0 {
		ns := namespace.Namespace(nspath)
		ns.Metadata = addComments(ns.Metadata, defNode)

		err = ns.Validate()
		if err != nil {
			return nil, defNode.Errorf("error in object definition %s: %w", nspath, err)
		}

		return ns, nil
	}

	ns := namespace.Namespace(nspath, relationsAndPermissions...)
	ns.Metadata = addComments(ns.Metadata, defNode)

	err = ns.Validate()
	if err != nil {
		return nil, defNode.Errorf("error in object definition %s: %w", nspath, err)
	}

	return ns, nil
}

func addComments(mdmsg *v0.Metadata, dslNode *dslNode) *v0.Metadata {
	for _, child := range dslNode.GetChildren() {
		if child.GetType() == dslshape.NodeTypeComment {
			value, err := child.GetString(dslshape.NodeCommentPredicateValue)
			if err == nil {
				mdmsg, _ = namespace.AddComment(mdmsg, normalizeComment(value))
			}
		}
	}
	return mdmsg
}

func normalizeComment(value string) string {
	var lines []string
	scanner := bufio.NewScanner(strings.NewReader(value))
	for scanner.Scan() {
		trimmed := strings.TrimSpace(scanner.Text())
		lines = append(lines, trimmed)
	}
	return strings.Join(lines, "\n")
}

func translateRelationOrPermission(tctx translationContext, relOrPermNode *dslNode) (*v0.Relation, error) {
	switch relOrPermNode.GetType() {
	case dslshape.NodeTypeRelation:
		rel, err := translateRelation(tctx, relOrPermNode)
		if err != nil {
			return nil, err
		}
		rel.Metadata = addComments(rel.Metadata, relOrPermNode)
		return rel, err

	case dslshape.NodeTypePermission:
		rel, err := translatePermission(tctx, relOrPermNode)
		if err != nil {
			return nil, err
		}
		rel.Metadata = addComments(rel.Metadata, relOrPermNode)
		return rel, err

	default:
		return nil, relOrPermNode.Errorf("unknown definition top-level node type %s", relOrPermNode.GetType())
	}
}

func translateRelation(tctx translationContext, relationNode *dslNode) (*v0.Relation, error) {
	relationName, err := relationNode.GetString(dslshape.NodePredicateName)
	if err != nil {
		return nil, relationNode.Errorf("invalid relation name: %w", err)
	}

	allowedDirectTypes := []*v0.AllowedRelation{}
	for _, typeRef := range relationNode.List(dslshape.NodeRelationPredicateAllowedTypes) {
		allowedRelations, err := translateAllowedRelations(tctx, typeRef)
		if err != nil {
			return nil, err
		}

		allowedDirectTypes = append(allowedDirectTypes, allowedRelations...)
	}

	relation := namespace.Relation(relationName, nil, allowedDirectTypes...)
	err = relation.Validate()
	if err != nil {
		return nil, relationNode.Errorf("error in relation %s: %w", relationName, err)
	}

	return relation, nil
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

	permission := namespace.Relation(permissionName, rewrite)
	err = permission.Validate()
	if err != nil {
		return nil, permissionNode.Errorf("error in permission %s: %w", permissionName, err)
	}

	return permission, nil
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

func translateAllowedRelations(tctx translationContext, typeRefNode *dslNode) ([]*v0.AllowedRelation, error) {
	switch typeRefNode.GetType() {
	case dslshape.NodeTypeTypeReference:
		references := []*v0.AllowedRelation{}
		for _, subRefNode := range typeRefNode.List(dslshape.NodeTypeReferencePredicateType) {
			subReferences, err := translateAllowedRelations(tctx, subRefNode)
			if err != nil {
				return []*v0.AllowedRelation{}, err
			}

			references = append(references, subReferences...)
		}
		return references, nil

	case dslshape.NodeTypeSpecificTypeReference:
		ref, err := translateSpecificTypeReference(tctx, typeRefNode)
		if err != nil {
			return []*v0.AllowedRelation{}, err
		}
		return []*v0.AllowedRelation{ref}, nil

	default:
		return nil, typeRefNode.Errorf("unknown type ref node type %s", typeRefNode.GetType())
	}
}

func translateSpecificTypeReference(tctx translationContext, typeRefNode *dslNode) (*v0.AllowedRelation, error) {
	typePath, err := typeRefNode.GetString(dslshape.NodeSpecificReferencePredicateType)
	if err != nil {
		return nil, typeRefNode.Errorf("invalid type name: %w", err)
	}

	nspath, err := tctx.namespacePath(typePath)
	if err != nil {
		return nil, typeRefNode.Errorf("%w", err)
	}

	if typeRefNode.Has(dslshape.NodeSpecificReferencePredicateWildcard) {
		ref := &v0.AllowedRelation{
			Namespace: nspath,
			RelationOrWildcard: &v0.AllowedRelation_PublicWildcard_{
				PublicWildcard: &v0.AllowedRelation_PublicWildcard{},
			},
		}

		err = ref.Validate()
		if err != nil {
			return nil, typeRefNode.Errorf("invalid type relation: %w", err)
		}

		return ref, nil
	}

	relationName := Ellipsis
	if typeRefNode.Has(dslshape.NodeSpecificReferencePredicateRelation) {
		relationName, err = typeRefNode.GetString(dslshape.NodeSpecificReferencePredicateRelation)
		if err != nil {
			return nil, typeRefNode.Errorf("invalid type relation: %w", err)
		}
	}

	ref := &v0.AllowedRelation{
		Namespace: nspath,
		RelationOrWildcard: &v0.AllowedRelation_Relation{
			Relation: relationName,
		},
	}

	err = ref.Validate()
	if err != nil {
		return nil, typeRefNode.Errorf("invalid type relation: %w", err)
	}

	return ref, nil
}
