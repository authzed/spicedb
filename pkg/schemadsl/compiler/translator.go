package compiler

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/authzed/spicedb/pkg/caveats"

	"github.com/authzed/spicedb/pkg/util"

	"github.com/jzelinskie/stringz"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/namespace"
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

type translationContext struct {
	objectTypePrefix *string
	mapper           input.PositionMapper
	schemaString     string
}

func (tctx translationContext) prefixedPath(definitionName string) (string, error) {
	var prefix, name string
	if err := stringz.SplitExact(definitionName, "/", &prefix, &name); err != nil {
		if tctx.objectTypePrefix == nil {
			return "", fmt.Errorf("found reference `%s` without prefix", definitionName)
		}
		prefix = *tctx.objectTypePrefix
		name = definitionName
	}

	if prefix == "" {
		return name, nil
	}

	return stringz.Join("/", prefix, name), nil
}

const Ellipsis = "..."

func translate(tctx translationContext, root *dslNode) (*CompiledSchema, error) {
	orderedDefinitions := make([]SchemaDefinition, 0, len(root.GetChildren()))
	var objectDefinitions []*core.NamespaceDefinition
	var caveatDefinitions []*core.CaveatDefinition

	names := util.NewSet[string]()

	for _, definitionNode := range root.GetChildren() {
		var definition SchemaDefinition

		switch definitionNode.GetType() {
		case dslshape.NodeTypeCaveatDefinition:
			def, err := translateCaveatDefinition(tctx, definitionNode)
			if err != nil {
				return nil, err
			}

			definition = def
			caveatDefinitions = append(caveatDefinitions, def)

		case dslshape.NodeTypeDefinition:
			def, err := translateObjectDefinition(tctx, definitionNode)
			if err != nil {
				return nil, err
			}

			definition = def
			objectDefinitions = append(objectDefinitions, def)
		}

		if !names.Add(definition.GetName()) {
			return nil, definitionNode.ErrorWithSourcef(definition.GetName(), "found name reused between multiple definitions and/or caveats: %s", definition.GetName())
		}

		orderedDefinitions = append(orderedDefinitions, definition)
	}

	return &CompiledSchema{
		CaveatDefinitions:  caveatDefinitions,
		ObjectDefinitions:  objectDefinitions,
		OrderedDefinitions: orderedDefinitions,
	}, nil
}

func translateCaveatDefinition(tctx translationContext, defNode *dslNode) (*core.CaveatDefinition, error) {
	definitionName, err := defNode.GetString(dslshape.NodeCaveatDefinitionPredicateName)
	if err != nil {
		return nil, defNode.ErrorWithSourcef(definitionName, "invalid definition name: %w", err)
	}

	// parameters
	paramNodes := defNode.List(dslshape.NodeCaveatDefinitionPredicateParameters)
	if len(paramNodes) == 0 {
		return nil, defNode.ErrorWithSourcef(definitionName, "caveat `%s` must have at least one parameter defined", definitionName)
	}

	env := caveats.NewEnvironment()
	parameters := make(map[string]caveattypes.VariableType, len(paramNodes))
	for _, paramNode := range paramNodes {
		paramName, err := paramNode.GetString(dslshape.NodeCaveatParameterPredicateName)
		if err != nil {
			return nil, paramNode.ErrorWithSourcef(paramName, "invalid parameter name: %w", err)
		}

		if _, ok := parameters[paramName]; ok {
			return nil, paramNode.ErrorWithSourcef(paramName, "duplicate parameter `%s` defined on caveat `%s`", paramName, definitionName)
		}

		typeRefNode, err := paramNode.Lookup(dslshape.NodeCaveatParameterPredicateType)
		if err != nil {
			return nil, paramNode.ErrorWithSourcef(paramName, "invalid type for parameter: %w", err)
		}

		translatedType, err := translateCaveatTypeReference(tctx, typeRefNode)
		if err != nil {
			return nil, paramNode.ErrorWithSourcef(paramName, "invalid type for caveat parameter `%s` on caveat `%s`: %w", paramName, definitionName, err)
		}

		parameters[paramName] = *translatedType
		err = env.AddVariable(paramName, *translatedType)
		if err != nil {
			return nil, paramNode.ErrorWithSourcef(paramName, "invalid type for caveat parameter `%s` on caveat `%s`: %w", paramName, definitionName, err)
		}
	}

	caveatPath, err := tctx.prefixedPath(definitionName)
	if err != nil {
		return nil, defNode.Errorf("%w", err)
	}

	// caveat expression.
	expressionStringNode, err := defNode.Lookup(dslshape.NodeCaveatDefinitionPredicateExpession)
	if err != nil {
		return nil, defNode.ErrorWithSourcef(definitionName, "invalid expression: %w", err)
	}

	expressionString, err := expressionStringNode.GetString(dslshape.NodeCaveatExpressionPredicateExpression)
	if err != nil {
		return nil, defNode.ErrorWithSourcef(expressionString, "invalid expression: %w", err)
	}

	rnge, err := expressionStringNode.Range(tctx.mapper)
	if err != nil {
		return nil, defNode.ErrorWithSourcef(expressionString, "invalid expression: %w", err)
	}

	source, err := caveats.NewSource(expressionString, rnge.Start(), caveatPath)
	if err != nil {
		return nil, defNode.ErrorWithSourcef(expressionString, "invalid expression: %w", err)
	}

	compiled, err := caveats.CompileCaveatWithSource(env, caveatPath, source)
	if err != nil {
		return nil, expressionStringNode.ErrorWithSourcef(expressionString, "invalid expression for caveat `%s`: %w", definitionName, err)
	}

	def, err := namespace.CompiledCaveatDefinition(env, caveatPath, compiled)
	if err != nil {
		return nil, err
	}

	def.Metadata = addComments(def.Metadata, defNode)
	def.SourcePosition = getSourcePosition(defNode, tctx.mapper)
	return def, nil
}

func translateCaveatTypeReference(tctx translationContext, typeRefNode *dslNode) (*caveattypes.VariableType, error) {
	typeName, err := typeRefNode.GetString(dslshape.NodeCaveatTypeReferencePredicateType)
	if err != nil {
		return nil, typeRefNode.ErrorWithSourcef(typeName, "invalid type name: %w", err)
	}

	childTypeNodes := typeRefNode.List(dslshape.NodeCaveatTypeReferencePredicateChildTypes)
	childTypes := make([]caveattypes.VariableType, 0, len(childTypeNodes))
	for _, childTypeNode := range childTypeNodes {
		translated, err := translateCaveatTypeReference(tctx, childTypeNode)
		if err != nil {
			return nil, err
		}
		childTypes = append(childTypes, *translated)
	}

	constructedType, err := caveattypes.BuildType(typeName, childTypes)
	if err != nil {
		return nil, typeRefNode.ErrorWithSourcef(typeName, "%w", err)
	}

	return constructedType, nil
}

func translateObjectDefinition(tctx translationContext, defNode *dslNode) (*core.NamespaceDefinition, error) {
	definitionName, err := defNode.GetString(dslshape.NodeDefinitionPredicateName)
	if err != nil {
		return nil, defNode.ErrorWithSourcef(definitionName, "invalid definition name: %w", err)
	}

	relationsAndPermissions := []*core.Relation{}
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

	nspath, err := tctx.prefixedPath(definitionName)
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
	ns.SourcePosition = getSourcePosition(defNode, tctx.mapper)

	err = ns.Validate()
	if err != nil {
		return nil, defNode.Errorf("error in object definition %s: %w", nspath, err)
	}

	return ns, nil
}

func getSourcePosition(dslNode *dslNode, mapper input.PositionMapper) *core.SourcePosition {
	if !dslNode.Has(dslshape.NodePredicateStartRune) {
		return nil
	}

	sourceRange, err := dslNode.Range(mapper)
	if err != nil {
		return nil
	}

	line, col, err := sourceRange.Start().LineAndColumn()
	if err != nil {
		return nil
	}

	return &core.SourcePosition{
		ZeroIndexedLineNumber:     uint64(line),
		ZeroIndexedColumnPosition: uint64(col),
	}
}

func addComments(mdmsg *core.Metadata, dslNode *dslNode) *core.Metadata {
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

func translateRelationOrPermission(tctx translationContext, relOrPermNode *dslNode) (*core.Relation, error) {
	switch relOrPermNode.GetType() {
	case dslshape.NodeTypeRelation:
		rel, err := translateRelation(tctx, relOrPermNode)
		if err != nil {
			return nil, err
		}
		rel.Metadata = addComments(rel.Metadata, relOrPermNode)
		rel.SourcePosition = getSourcePosition(relOrPermNode, tctx.mapper)
		return rel, err

	case dslshape.NodeTypePermission:
		rel, err := translatePermission(tctx, relOrPermNode)
		if err != nil {
			return nil, err
		}
		rel.Metadata = addComments(rel.Metadata, relOrPermNode)
		rel.SourcePosition = getSourcePosition(relOrPermNode, tctx.mapper)
		return rel, err

	default:
		return nil, relOrPermNode.Errorf("unknown definition top-level node type %s", relOrPermNode.GetType())
	}
}

func translateRelation(tctx translationContext, relationNode *dslNode) (*core.Relation, error) {
	relationName, err := relationNode.GetString(dslshape.NodePredicateName)
	if err != nil {
		return nil, relationNode.Errorf("invalid relation name: %w", err)
	}

	allowedDirectTypes := []*core.AllowedRelation{}
	for _, typeRef := range relationNode.List(dslshape.NodeRelationPredicateAllowedTypes) {
		allowedRelations, err := translateAllowedRelations(tctx, typeRef)
		if err != nil {
			return nil, err
		}

		allowedDirectTypes = append(allowedDirectTypes, allowedRelations...)
	}

	relation, err := namespace.Relation(relationName, nil, allowedDirectTypes...)
	if err != nil {
		return nil, err
	}

	err = relation.Validate()
	if err != nil {
		return nil, relationNode.Errorf("error in relation %s: %w", relationName, err)
	}

	return relation, nil
}

func translatePermission(tctx translationContext, permissionNode *dslNode) (*core.Relation, error) {
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

	permission, err := namespace.Relation(permissionName, rewrite)
	if err != nil {
		return nil, err
	}

	err = permission.Validate()
	if err != nil {
		return nil, permissionNode.Errorf("error in permission %s: %w", permissionName, err)
	}

	return permission, nil
}

func translateBinary(tctx translationContext, expressionNode *dslNode) (*core.SetOperation_Child, *core.SetOperation_Child, error) {
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

func translateExpression(tctx translationContext, expressionNode *dslNode) (*core.UsersetRewrite, error) {
	translated, err := translateExpressionDirect(tctx, expressionNode)
	if err != nil {
		return translated, err
	}

	translated.SourcePosition = getSourcePosition(expressionNode, tctx.mapper)
	return translated, nil
}

func translateExpressionDirect(tctx translationContext, expressionNode *dslNode) (*core.UsersetRewrite, error) {
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

func translateExpressionOperation(tctx translationContext, expressionOpNode *dslNode) (*core.SetOperation_Child, error) {
	translated, err := translateExpressionOperationDirect(tctx, expressionOpNode)
	if err != nil {
		return translated, err
	}

	translated.SourcePosition = getSourcePosition(expressionOpNode, tctx.mapper)
	return translated, nil
}

func translateExpressionOperationDirect(tctx translationContext, expressionOpNode *dslNode) (*core.SetOperation_Child, error) {
	switch expressionOpNode.GetType() {
	case dslshape.NodeTypeIdentifier:
		referencedRelationName, err := expressionOpNode.GetString(dslshape.NodeIdentiferPredicateValue)
		if err != nil {
			return nil, err
		}

		return namespace.ComputedUserset(referencedRelationName), nil

	case dslshape.NodeTypeNilExpression:
		return namespace.Nil(), nil

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

func translateAllowedRelations(tctx translationContext, typeRefNode *dslNode) ([]*core.AllowedRelation, error) {
	switch typeRefNode.GetType() {
	case dslshape.NodeTypeTypeReference:
		references := []*core.AllowedRelation{}
		for _, subRefNode := range typeRefNode.List(dslshape.NodeTypeReferencePredicateType) {
			subReferences, err := translateAllowedRelations(tctx, subRefNode)
			if err != nil {
				return []*core.AllowedRelation{}, err
			}

			references = append(references, subReferences...)
		}
		return references, nil

	case dslshape.NodeTypeSpecificTypeReference:
		ref, err := translateSpecificTypeReference(tctx, typeRefNode)
		if err != nil {
			return []*core.AllowedRelation{}, err
		}
		return []*core.AllowedRelation{ref}, nil

	default:
		return nil, typeRefNode.Errorf("unknown type ref node type %s", typeRefNode.GetType())
	}
}

func translateSpecificTypeReference(tctx translationContext, typeRefNode *dslNode) (*core.AllowedRelation, error) {
	typePath, err := typeRefNode.GetString(dslshape.NodeSpecificReferencePredicateType)
	if err != nil {
		return nil, typeRefNode.Errorf("invalid type name: %w", err)
	}

	nspath, err := tctx.prefixedPath(typePath)
	if err != nil {
		return nil, typeRefNode.Errorf("%w", err)
	}

	if typeRefNode.Has(dslshape.NodeSpecificReferencePredicateWildcard) {
		ref := &core.AllowedRelation{
			Namespace: nspath,
			RelationOrWildcard: &core.AllowedRelation_PublicWildcard_{
				PublicWildcard: &core.AllowedRelation_PublicWildcard{},
			},
		}

		err = addWithCaveats(tctx, typeRefNode, ref)
		if err != nil {
			return nil, typeRefNode.Errorf("invalid caveat: %w", err)
		}

		err = ref.Validate()
		if err != nil {
			return nil, typeRefNode.Errorf("invalid type relation: %w", err)
		}

		ref.SourcePosition = getSourcePosition(typeRefNode, tctx.mapper)
		return ref, nil
	}

	relationName := Ellipsis
	if typeRefNode.Has(dslshape.NodeSpecificReferencePredicateRelation) {
		relationName, err = typeRefNode.GetString(dslshape.NodeSpecificReferencePredicateRelation)
		if err != nil {
			return nil, typeRefNode.Errorf("invalid type relation: %w", err)
		}
	}

	ref := &core.AllowedRelation{
		Namespace: nspath,
		RelationOrWildcard: &core.AllowedRelation_Relation{
			Relation: relationName,
		},
	}

	err = addWithCaveats(tctx, typeRefNode, ref)
	if err != nil {
		return nil, typeRefNode.Errorf("invalid caveat: %w", err)
	}

	err = ref.Validate()
	if err != nil {
		return nil, typeRefNode.Errorf("invalid type relation: %w", err)
	}

	ref.SourcePosition = getSourcePosition(typeRefNode, tctx.mapper)
	return ref, nil
}

func addWithCaveats(tctx translationContext, typeRefNode *dslNode, ref *core.AllowedRelation) error {
	caveats := typeRefNode.List(dslshape.NodeSpecificReferencePredicateCaveat)
	if len(caveats) == 0 {
		return nil
	}

	if len(caveats) != 1 {
		return fmt.Errorf("only one caveat is currently allowed per type reference")
	}

	name, err := caveats[0].GetString(dslshape.NodeCaveatPredicateCaveat)
	if err != nil {
		return err
	}

	ref.RequiredCaveat = &core.AllowedCaveat{
		CaveatName: name,
	}
	return nil
}
