package development

import (
	"context"
	"fmt"
	"strings"

	"github.com/ccoveille/go-safecast"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/caveats"
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

// Resolver resolves references to schema nodes from source positions.
type Resolver struct {
	schema     *compiler.CompiledSchema
	typeSystem *schema.TypeSystem
}

// NewResolver creates a new resolver for the given schema.
func NewResolver(compiledSchema *compiler.CompiledSchema) (*Resolver, error) {
	resolver := schema.ResolverForCompiledSchema(*compiledSchema)
	ts := schema.NewTypeSystem(resolver)
	return &Resolver{schema: compiledSchema, typeSystem: ts}, nil
}

// ReferenceAtPosition returns the reference to the schema node at the given position in the source, if any.
func (r *Resolver) ReferenceAtPosition(source input.Source, position input.Position) (*SchemaReference, error) {
	nodeChain, err := compiler.PositionToAstNodeChain(r.schema, source, position)
	if err != nil {
		return nil, err
	}

	if nodeChain == nil {
		return nil, nil
	}

	relationReference := func(relation *core.Relation, def *schema.Definition) (*SchemaReference, error) {
		// NOTE: zeroes are fine here to mean "unknown"
		lineNumber, err := safecast.ToInt(relation.SourcePosition.ZeroIndexedLineNumber)
		if err != nil {
			log.Err(err).Msg("could not cast lineNumber to uint32")
		}
		columnPosition, err := safecast.ToInt(relation.SourcePosition.ZeroIndexedColumnPosition)
		if err != nil {
			log.Err(err).Msg("could not cast columnPosition to uint32")
		}
		relationPosition := input.Position{
			LineNumber:     lineNumber,
			ColumnPosition: columnPosition,
		}

		targetSourceCode, err := generator.GenerateRelationSource(relation)
		if err != nil {
			return nil, err
		}

		if def.IsPermission(relation.Name) {
			return &SchemaReference{
				Source:   source,
				Position: position,
				Text:     relation.Name,

				ReferenceType:     ReferenceTypePermission,
				ReferenceMarkdown: fmt.Sprintf("permission %s", relation.Name),

				TargetSource:             &source,
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
			ReferenceMarkdown: fmt.Sprintf("relation %s", relation.Name),

			TargetSource:             &source,
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
		lineNumber, err := safecast.ToInt(def.SourcePosition.ZeroIndexedLineNumber)
		if err != nil {
			log.Err(err).Msg("could not cast lineNumber to uint32")
		}
		columnPosition, err := safecast.ToInt(def.SourcePosition.ZeroIndexedColumnPosition)
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

		return &SchemaReference{
			Source:   source,
			Position: position,
			Text:     def.Name,

			ReferenceType:     ReferenceTypeDefinition,
			ReferenceMarkdown: fmt.Sprintf("definition %s", def.Name),

			TargetSource:             &source,
			TargetPosition:           &defPosition,
			TargetSourceCode:         targetSourceCode,
			TargetNamePositionOffset: len("definition "),
		}, nil
	}

	// Caveat Type reference.
	if caveatDef, ok := r.caveatTypeReferenceChain(nodeChain); ok {
		// NOTE: zeroes are fine here to mean "unknown"
		lineNumber, err := safecast.ToInt(caveatDef.SourcePosition.ZeroIndexedLineNumber)
		if err != nil {
			log.Err(err).Msg("could not cast lineNumber to uint32")
		}
		columnPosition, err := safecast.ToInt(caveatDef.SourcePosition.ZeroIndexedColumnPosition)
		if err != nil {
			log.Err(err).Msg("could not cast columnPosition to uint32")
		}

		defPosition := input.Position{
			LineNumber:     lineNumber,
			ColumnPosition: columnPosition,
		}

		var caveatSourceCode strings.Builder
		caveatSourceCode.WriteString(fmt.Sprintf("caveat %s(", caveatDef.Name))
		index := 0
		for paramName, paramType := range caveatDef.ParameterTypes {
			if index > 0 {
				caveatSourceCode.WriteString(", ")
			}

			caveatSourceCode.WriteString(fmt.Sprintf("%s %s", paramName, caveats.ParameterTypeString(paramType)))
			index++
		}
		caveatSourceCode.WriteString(") {\n\t// ...\n}")

		return &SchemaReference{
			Source:   source,
			Position: position,
			Text:     caveatDef.Name,

			ReferenceType:     ReferenceTypeCaveat,
			ReferenceMarkdown: fmt.Sprintf("caveat %s", caveatDef.Name),

			TargetSource:             &source,
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
	if caveatParamName, caveatDef, ok := r.caveatParamChain(nodeChain, source, position); ok {
		targetSourceCode := fmt.Sprintf("%s %s", caveatParamName, caveats.ParameterTypeString(caveatDef.ParameterTypes[caveatParamName]))

		return &SchemaReference{
			Source:   source,
			Position: position,
			Text:     caveatParamName,

			ReferenceType:     ReferenceTypeCaveatParameter,
			ReferenceMarkdown: targetSourceCode,

			TargetSource:     &source,
			TargetSourceCode: targetSourceCode,
		}, nil
	}

	return nil, nil
}

func (r *Resolver) lookupCaveat(caveatName string) (*core.CaveatDefinition, bool) {
	for _, caveatDef := range r.schema.CaveatDefinitions {
		if caveatDef.Name == caveatName {
			return caveatDef, true
		}
	}

	return nil, false
}

func (r *Resolver) lookupRelation(defName, relationName string) (*core.Relation, *schema.Definition, bool) {
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

func (r *Resolver) caveatParamChain(nodeChain *compiler.NodeChain, source input.Source, position input.Position) (string, *core.CaveatDefinition, bool) {
	if !nodeChain.HasHeadType(dslshape.NodeTypeCaveatExpression) {
		return "", nil, false
	}

	caveatDefNode := nodeChain.FindNodeOfType(dslshape.NodeTypeCaveatDefinition)
	if caveatDefNode == nil {
		return "", nil, false
	}

	caveatName, err := caveatDefNode.GetString(dslshape.NodeCaveatDefinitionPredicateName)
	if err != nil {
		return "", nil, false
	}

	caveatDef, ok := r.lookupCaveat(caveatName)
	if !ok {
		return "", nil, false
	}

	runePosition, err := r.schema.SourcePositionToRunePosition(source, position)
	if err != nil {
		return "", nil, false
	}

	exprRunePosition, err := nodeChain.Head().GetInt(dslshape.NodePredicateStartRune)
	if err != nil {
		return "", nil, false
	}

	if exprRunePosition > runePosition {
		return "", nil, false
	}

	relationRunePosition := runePosition - exprRunePosition

	caveatExpr, err := nodeChain.Head().GetString(dslshape.NodeCaveatExpressionPredicateExpression)
	if err != nil {
		return "", nil, false
	}

	// Split the expression into tokens and find the associated token.
	tokens := strings.FieldsFunc(caveatExpr, splitCELToken)
	currentIndex := 0
	for _, token := range tokens {
		if currentIndex <= relationRunePosition && currentIndex+len(token) >= relationRunePosition {
			if _, ok := caveatDef.ParameterTypes[token]; ok {
				return token, caveatDef, true
			}
		}
	}

	return "", caveatDef, true
}

func splitCELToken(r rune) bool {
	return r == ' ' || r == '(' || r == ')' || r == '.' || r == ',' || r == '[' || r == ']' || r == '{' || r == '}' || r == ':' || r == '='
}

func (r *Resolver) caveatTypeReferenceChain(nodeChain *compiler.NodeChain) (*core.CaveatDefinition, bool) {
	if !nodeChain.HasHeadType(dslshape.NodeTypeCaveatReference) {
		return nil, false
	}

	caveatName, err := nodeChain.Head().GetString(dslshape.NodeCaveatPredicateCaveat)
	if err != nil {
		return nil, false
	}

	return r.lookupCaveat(caveatName)
}

func (r *Resolver) typeReferenceChain(nodeChain *compiler.NodeChain) (*schema.Definition, *core.Relation, bool) {
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

func (r *Resolver) relationReferenceChain(nodeChain *compiler.NodeChain) (*core.Relation, *schema.Definition, bool) {
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
