package development

import (
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/typesystem"
)

// ReferenceType is the type of reference.
type ReferenceType int

const (
	ReferenceTypeUnknown ReferenceType = iota
	ReferenceTypeDefinition
	ReferenceTypeCaveat
	ReferenceTypeRelation
	ReferenceTypePermission
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
}

// Resolver resolves references to schema nodes from source positions.
type Resolver struct {
	schema      *compiler.CompiledSchema
	typeSystems map[string]*typesystem.TypeSystem
}

// NewResolver creates a new resolver for the given schema.
func NewResolver(schema *compiler.CompiledSchema) (*Resolver, error) {
	typeSystems := make(map[string]*typesystem.TypeSystem, len(schema.ObjectDefinitions))
	tsResolver := typesystem.ResolverForSchema(*schema)
	for _, def := range schema.ObjectDefinitions {
		ts, err := typesystem.NewNamespaceTypeSystem(def, tsResolver)
		if err != nil {
			return nil, err
		}

		typeSystems[def.Name] = ts
	}

	return &Resolver{schema: schema, typeSystems: typeSystems}, nil
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

	// Type reference.
	if ts, ok := r.typeReferenceChain(nodeChain); ok {
		def := ts.Namespace()
		defPosition := input.Position{
			LineNumber:     int(def.SourcePosition.ZeroIndexedLineNumber),
			ColumnPosition: int(def.SourcePosition.ZeroIndexedColumnPosition),
		}

		return &SchemaReference{
			Source:   source,
			Position: position,
			Text:     def.Name,

			ReferenceType:     ReferenceTypeDefinition,
			ReferenceMarkdown: fmt.Sprintf("definition %s", def.Name),

			TargetSource:   &source,
			TargetPosition: &defPosition,
		}, nil
	}

	// Caveat Type reference.
	if caveatDef, ok := r.caveatTypeReferenceChain(nodeChain); ok {
		defPosition := input.Position{
			LineNumber:     int(caveatDef.SourcePosition.ZeroIndexedLineNumber),
			ColumnPosition: int(caveatDef.SourcePosition.ZeroIndexedColumnPosition),
		}

		return &SchemaReference{
			Source:   source,
			Position: position,
			Text:     caveatDef.Name,

			ReferenceType:     ReferenceTypeCaveat,
			ReferenceMarkdown: fmt.Sprintf("caveat %s", caveatDef.Name),

			TargetSource:   &source,
			TargetPosition: &defPosition,
		}, nil
	}

	// Relation reference.
	if relation, ts, ok := r.relationReferenceChain(nodeChain); ok {
		relationPosition := input.Position{
			LineNumber:     int(relation.SourcePosition.ZeroIndexedLineNumber),
			ColumnPosition: int(relation.SourcePosition.ZeroIndexedColumnPosition),
		}

		if ts.IsPermission(relation.Name) {
			return &SchemaReference{
				Source:   source,
				Position: position,
				Text:     relation.Name,

				ReferenceType:     ReferenceTypePermission,
				ReferenceMarkdown: fmt.Sprintf("permission %s", relation.Name),

				TargetSource:   &source,
				TargetPosition: &relationPosition,
			}, nil
		}

		return &SchemaReference{
			Source:   source,
			Position: position,
			Text:     relation.Name,

			ReferenceType:     ReferenceTypeRelation,
			ReferenceMarkdown: fmt.Sprintf("relation %s", relation.Name),

			TargetSource:   &source,
			TargetPosition: &relationPosition,
		}, nil
	}

	return nil, nil
}

func (r *Resolver) lookupDefinition(defName string) (*typesystem.TypeSystem, bool) {
	ts, ok := r.typeSystems[defName]
	return ts, ok
}

func (r *Resolver) lookupCaveat(caveatName string) (*core.CaveatDefinition, bool) {
	for _, caveatDef := range r.schema.CaveatDefinitions {
		if caveatDef.Name == caveatName {
			return caveatDef, true
		}
	}

	return nil, false
}

func (r *Resolver) lookupRelation(defName, relationName string) (*core.Relation, *typesystem.TypeSystem, bool) {
	ts, ok := r.typeSystems[defName]
	if !ok {
		return nil, nil, false
	}

	rel, ok := ts.GetRelation(relationName)
	if !ok {
		return nil, nil, false
	}

	return rel, ts, true
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

func (r *Resolver) typeReferenceChain(nodeChain *compiler.NodeChain) (*typesystem.TypeSystem, bool) {
	if !nodeChain.HasHeadType(dslshape.NodeTypeSpecificTypeReference) {
		return nil, false
	}

	defName, err := nodeChain.Head().GetString(dslshape.NodeSpecificReferencePredicateType)
	if err != nil {
		return nil, false
	}

	return r.lookupDefinition(defName)
}

func (r *Resolver) relationReferenceChain(nodeChain *compiler.NodeChain) (*core.Relation, *typesystem.TypeSystem, bool) {
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
