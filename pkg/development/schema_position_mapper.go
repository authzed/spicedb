package development

import (
	"context"
	"fmt"
	"strings"

	"github.com/ccoveille/go-safecast/v2"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/caveats"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// ReferenceType is the type of reference.
type ReferenceType int

const (
	ReferenceTypeUnknown ReferenceType = iota
	ReferenceTypeDefinition
	ReferenceTypeCaveat
	ReferenceTypeRelation
	ReferenceTypePermission
	ReferenceTypeCaveatParameter
	ReferenceTypeImport
	ReferenceTypePartial
)

// SchemaReference represents a reference to a schema node.
type SchemaReference struct {
	// Source is the source of the reference.
	Source input.Source

	// Position is the position of the reference in the source.
	Position input.Position

	// Text is the text of the reference.
	Text string

	// ReferenceType is the type of reference.
	ReferenceType ReferenceType

	// ReferenceMarkdown is the markdown representation of the reference.
	ReferenceMarkdown string

	// TargetSource is the source of the target node, if any.
	TargetSource *input.Source

	// TargetPosition is the position of the target node, if any.
	TargetPosition *input.Position

	// TargetSourceCode is the source code representation of the target, if any.
	TargetSourceCode string

	// TargetNamePositionOffset is the offset from the target position from where the
	// *name* of the target is found.
	TargetNamePositionOffset int
}

// SchemaPositionMapper maps source positions to references.
type SchemaPositionMapper struct {
	schema     *compiler.CompiledSchema
	typeSystem *schema.TypeSystem
}

// NewSchemaPositionMapper creates a new schema position mapper.
func NewSchemaPositionMapper(compiledSchema *compiler.CompiledSchema) (*SchemaPositionMapper, error) {
	resolver := schema.ResolverForCompiledSchema(compiledSchema)
	ts := schema.NewTypeSystem(resolver)
	return &SchemaPositionMapper{schema: compiledSchema, typeSystem: ts}, nil
}

// ReferenceAtPosition returns the reference to the schema node at the given position in the source, if any.
func (r *SchemaPositionMapper) ReferenceAtPosition(source input.Source, position input.Position) (*SchemaReference, error) {
	nodeChain, err := compiler.PositionToAstNodeChain(r.schema, source, position)
	if err != nil {
		return nil, err
	}

	if nodeChain == nil {
		return nil, nil
	}

	// Import reference.
	if importPath, ok := r.importReferenceChain(nodeChain); ok {
		importSource := input.Source(importPath)
		return &SchemaReference{
			ReferenceType: ReferenceTypeImport,
			Source:        source,
			Position:      position,
			// TODO: is this the path pointed at by the import, or is it the position of the import reference itself?
			TargetSource:   &importSource,
			TargetPosition: &input.Position{LineNumber: 0, ColumnPosition: 0},
			Text:           importPath,
		}, nil
	}

	// Partial reference.
	if partialName, ok := r.partialReferenceChain(nodeChain); ok {
		line, col := r.schema.PartialNodePosition(partialName)
		var targetPosition *input.Position
		if line >= 0 && col >= 0 {
			targetPosition = &input.Position{LineNumber: line, ColumnPosition: col}
		}

		targetSource := r.resolveTargetSource(partialName, source)
		return &SchemaReference{
			Source:                   source,
			Position:                 position,
			Text:                     partialName,
			TargetSourceCode:         "partial " + partialName,
			ReferenceType:            ReferenceTypePartial,
			TargetSource:             &targetSource,
			TargetPosition:           targetPosition,
			TargetNamePositionOffset: len("partial "),
		}, nil
	}

	relationReference := func(relation *core.Relation, def *schema.Definition) (*SchemaReference, error) {
		// NOTE: zeroes are fine here to mean "unknown"
		lineNumber, err := safecast.Convert[int](relation.SourcePosition.ZeroIndexedLineNumber)
		if err != nil {
			log.Err(err).Msg("could not cast lineNumber to uint32")
		}
		columnPosition, err := safecast.Convert[int](relation.SourcePosition.ZeroIndexedColumnPosition)
		if err != nil {
			log.Err(err).Msg("could not cast columnPosition to uint32")
		}
		relationPosition := input.Position{
			LineNumber:     lineNumber,
			ColumnPosition: columnPosition,
		}

		targetSourceCode, err := generator.GenerateRelationSource(relation, caveattypes.Default.TypeSet)
		if err != nil {
			return nil, err
		}

		targetSource := r.resolveTargetSource(relation.Name, source)

		if def.IsPermission(relation.Name) {
			return &SchemaReference{
				Source:   source,
				Position: position,
				Text:     relation.Name,

				ReferenceType:     ReferenceTypePermission,
				ReferenceMarkdown: "permission " + relation.Name,

				TargetSource:             &targetSource,
				TargetPosition:           &relationPosition,
				TargetSourceCode:         targetSourceCode,
				TargetNamePositionOffset: len("permission "),
			}, nil
		}

		return &SchemaReference{
			Source:   source,
			Position: position,
			Text:     relation.Name,

			ReferenceType:     ReferenceTypeRelation,
			ReferenceMarkdown: "relation " + relation.Name,

			TargetSource:             &targetSource,
			TargetPosition:           &relationPosition,
			TargetSourceCode:         targetSourceCode,
			TargetNamePositionOffset: len("relation "),
		}, nil
	}

	// Type reference.
	if ts, relation, ok := r.typeReferenceChain(nodeChain); ok {
		if relation != nil {
			return relationReference(relation, ts)
		}

		def := ts.Namespace()

		// NOTE: zeroes are fine here to mean "unknown"
		lineNumber, err := safecast.Convert[int](def.SourcePosition.ZeroIndexedLineNumber)
		if err != nil {
			log.Err(err).Msg("could not cast lineNumber to uint32")
		}
		columnPosition, err := safecast.Convert[int](def.SourcePosition.ZeroIndexedColumnPosition)
		if err != nil {
			log.Err(err).Msg("could not cast columnPosition to uint32")
		}

		defPosition := input.Position{
			LineNumber:     lineNumber,
			ColumnPosition: columnPosition,
		}

		docComment := ""
		comments := namespace.GetComments(def.Metadata)
		if len(comments) > 0 {
			docComment = strings.Join(comments, "\n") + "\n"
		}

		targetSourceCode := fmt.Sprintf("%sdefinition %s {\n\t// ...\n}", docComment, def.Name)
		if len(def.Relation) == 0 {
			targetSourceCode = fmt.Sprintf("%sdefinition %s {}", docComment, def.Name)
		}

		targetSource := r.resolveTargetSource(def.Name, source)
		return &SchemaReference{
			Source:   source,
			Position: position,
			Text:     def.Name,

			ReferenceType:     ReferenceTypeDefinition,
			ReferenceMarkdown: "definition " + def.Name,

			TargetSource:             &targetSource,
			TargetPosition:           &defPosition,
			TargetSourceCode:         targetSourceCode,
			TargetNamePositionOffset: len("definition "),
		}, nil
	}

	// Caveat Type reference.
	if caveatDef, ok := r.caveatTypeReferenceChain(nodeChain); ok {
		// NOTE: zeroes are fine here to mean "unknown"
		lineNumber, err := safecast.Convert[int](caveatDef.SourcePosition.ZeroIndexedLineNumber)
		if err != nil {
			log.Err(err).Msg("could not cast lineNumber to uint32")
		}
		columnPosition, err := safecast.Convert[int](caveatDef.SourcePosition.ZeroIndexedColumnPosition)
		if err != nil {
			log.Err(err).Msg("could not cast columnPosition to uint32")
		}

		defPosition := input.Position{
			LineNumber:     lineNumber,
			ColumnPosition: columnPosition,
		}

		var caveatSourceCode strings.Builder
		fmt.Fprintf(&caveatSourceCode, "caveat %s(", caveatDef.Name)
		index := 0
		for paramName, paramType := range caveatDef.ParameterTypes {
			if index > 0 {
				caveatSourceCode.WriteString(", ")
			}

			fmt.Fprintf(&caveatSourceCode, "%s %s", paramName, caveats.ParameterTypeString(paramType))
			index++
		}
		caveatSourceCode.WriteString(") {\n\t// ...\n}")

		targetSource := r.resolveTargetSource(caveatDef.Name, source)
		return &SchemaReference{
			Source:   source,
			Position: position,
			Text:     caveatDef.Name,

			ReferenceType:     ReferenceTypeCaveat,
			ReferenceMarkdown: "caveat " + caveatDef.Name,

			TargetSource:             &targetSource,
			TargetPosition:           &defPosition,
			TargetSourceCode:         caveatSourceCode.String(),
			TargetNamePositionOffset: len("caveat "),
		}, nil
	}

	// Relation reference.
	if relation, ts, ok := r.relationReferenceChain(nodeChain); ok {
		return relationReference(relation, ts)
	}

	// Caveat parameter used in expression.
	if caveatParamName, paramTypeString, caveatDef, ok := r.caveatParamChain(nodeChain, source, position); ok {
		targetSourceCode := fmt.Sprintf("%s %s", caveatParamName, paramTypeString)
		targetSource := r.resolveTargetSource(caveatDef.Name, source)
		return &SchemaReference{
			Source:   source,
			Position: position,
			Text:     caveatParamName,

			ReferenceType:     ReferenceTypeCaveatParameter,
			ReferenceMarkdown: targetSourceCode,

			TargetSource:     &targetSource,
			TargetSourceCode: targetSourceCode,
		}, nil
	}

	return nil, nil
}

func (r *SchemaPositionMapper) lookupCaveat(caveatName string) (*core.CaveatDefinition, bool) {
	c, err := r.typeSystem.GetCaveat(context.Background(), caveatName)
	if err != nil {
		return nil, false
	}

	return c, true
}

func (r *SchemaPositionMapper) lookupRelation(defName, relationName string) (*core.Relation, *schema.Definition, bool) {
	ts, err := r.typeSystem.GetDefinition(context.Background(), defName)
	if err != nil {
		return nil, nil, false
	}

	rel, ok := ts.GetRelation(relationName)
	if !ok {
		return nil, nil, false
	}

	return rel, ts, true
}

// caveatParamChain determines whether the given position within a caveat expression refers to a
// caveat parameter. It walks the node chain to find the enclosing caveat definition, tokenizes the
// CEL expression, and checks if the token at the cursor position matches a declared parameter name.
//
// For example, given the schema:
//
//	caveat my_caveat(some_param int) {
//	    some_param > 0
//	}
//
// If the cursor is on "some_param" in the expression "some_param > 0", this returns
// ("some_param", "int", true).
//
// Returns ("", "", false) if the position is not within a caveat expression or does not correspond
// to a known parameter.
func (r *SchemaPositionMapper) caveatParamChain(nodeChain *compiler.NodeChain, source input.Source, position input.Position) (string, string, *core.CaveatDefinition, bool) {
	if !nodeChain.HasHeadType(dslshape.NodeTypeCaveatExpression) {
		return "", "", nil, false
	}

	caveatDefNode := nodeChain.FindNodeOfType(dslshape.NodeTypeCaveatDefinition)
	if caveatDefNode == nil {
		return "", "", nil, false
	}

	caveatName, err := caveatDefNode.GetString(dslshape.NodeCaveatDefinitionPredicateName)
	if err != nil {
		return "", "", nil, false
	}

	caveatDef, ok := r.lookupCaveat(caveatName)
	if !ok {
		return "", "", nil, false
	}

	runePosition, err := r.schema.SourcePositionToRunePosition(source, position)
	if err != nil {
		return "", "", nil, false
	}

	exprRunePosition, err := nodeChain.Head().GetInt(dslshape.NodePredicateStartRune)
	if err != nil {
		return "", "", nil, false
	}

	if exprRunePosition > runePosition {
		return "", "", nil, false
	}

	relativeRunePosition := runePosition - exprRunePosition

	caveatExpr, err := nodeChain.Head().GetString(dslshape.NodeCaveatExpressionPredicateExpression)
	if err != nil {
		return "", "", nil, false
	}

	// Split the expression into tokens and find the associated token.
	tokens := strings.FieldsFunc(caveatExpr, splitCELToken)
	currentIndex := 0
	for _, token := range tokens {
		// Find the token's actual position in the expression starting from currentIndex.
		tokenStart := strings.Index(caveatExpr[currentIndex:], token) + currentIndex
		if tokenStart <= relativeRunePosition && tokenStart+len(token) >= relativeRunePosition {
			if paramType, ok := caveatDef.ParameterTypes[token]; ok {
				return token, caveats.ParameterTypeString(paramType), caveatDef, true
			}
		}
		currentIndex = tokenStart + len(token)
	}

	return "", "", nil, false
}

func splitCELToken(r rune) bool {
	return r == ' ' || r == '(' || r == ')' || r == '.' || r == ',' || r == '[' || r == ']' || r == '{' || r == '}' || r == ':' || r == '='
}

func (r *SchemaPositionMapper) caveatTypeReferenceChain(nodeChain *compiler.NodeChain) (*core.CaveatDefinition, bool) {
	if !nodeChain.HasHeadType(dslshape.NodeTypeCaveatReference) {
		return nil, false
	}

	caveatName, err := nodeChain.Head().GetString(dslshape.NodeCaveatPredicateCaveat)
	if err != nil {
		return nil, false
	}

	return r.lookupCaveat(caveatName)
}

func (r *SchemaPositionMapper) typeReferenceChain(nodeChain *compiler.NodeChain) (*schema.Definition, *core.Relation, bool) {
	if !nodeChain.HasHeadType(dslshape.NodeTypeSpecificTypeReference) {
		return nil, nil, false
	}

	defName, err := nodeChain.Head().GetString(dslshape.NodeSpecificReferencePredicateType)
	if err != nil {
		return nil, nil, false
	}

	def, err := r.typeSystem.GetDefinition(context.Background(), defName)
	if err != nil {
		return nil, nil, false
	}

	relationName, err := nodeChain.Head().GetString(dslshape.NodeSpecificReferencePredicateRelation)
	if err != nil {
		return def, nil, true
	}

	startingRune, err := nodeChain.Head().GetInt(dslshape.NodePredicateStartRune)
	if err != nil {
		return def, nil, true
	}

	// If hover over the definition name, return the definition.
	if nodeChain.ForRunePosition() < startingRune+len(defName) {
		return def, nil, true
	}

	relation, ok := def.GetRelation(relationName)
	if !ok {
		return nil, nil, false
	}

	return def, relation, true
}

func (r *SchemaPositionMapper) relationReferenceChain(nodeChain *compiler.NodeChain) (*core.Relation, *schema.Definition, bool) {
	if !nodeChain.HasHeadType(dslshape.NodeTypeIdentifier) {
		return nil, nil, false
	}

	if arrowExpr := nodeChain.FindNodeOfType(dslshape.NodeTypeArrowExpression); arrowExpr != nil {
		// Ensure this on the left side of the arrow.
		rightExpr, err := arrowExpr.Lookup(dslshape.NodeExpressionPredicateRightExpr)
		if err != nil {
			return nil, nil, false
		}

		if rightExpr == nodeChain.Head() {
			return nil, nil, false
		}
	}

	relationName, err := nodeChain.Head().GetString(dslshape.NodeIdentiferPredicateValue)
	if err != nil {
		return nil, nil, false
	}

	parentDefNode := nodeChain.FindNodeOfType(dslshape.NodeTypeDefinition)
	if parentDefNode == nil {
		return nil, nil, false
	}

	defName, err := parentDefNode.GetString(dslshape.NodeDefinitionPredicateName)
	if err != nil {
		return nil, nil, false
	}

	return r.lookupRelation(defName, relationName)
}

func (r *SchemaPositionMapper) importReferenceChain(nodeChain *compiler.NodeChain) (string, bool) {
	importNode := nodeChain.FindNodeOfType(dslshape.NodeTypeImport)
	if importNode == nil {
		return "", false
	}

	importPath, err := importNode.GetString(dslshape.NodeImportPredicatePath)
	if err != nil {
		return "", false
	}

	return importPath, true
}

func (r *SchemaPositionMapper) partialReferenceChain(nodeChain *compiler.NodeChain) (string, bool) {
	if !nodeChain.HasHeadType(dslshape.NodeTypePartialReference) {
		return "", false
	}

	partialName, err := nodeChain.Head().GetString(dslshape.NodePartialReferencePredicateName)
	if err != nil {
		return "", false
	}

	return partialName, true
}
