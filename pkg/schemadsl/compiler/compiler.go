package compiler

import (
	"fmt"

	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/schemadsl/parser"
)

// InputSchema defines the input for a Compile.
type InputSchema struct {
	// Source is the source of the schema being compiled.
	Source input.InputSource

	// Schema is the contents being compiled.
	SchemaString string
}

// Compile compilers the input schema(s) into a set of namespace definition protos.
func Compile(schemas []InputSchema, objectTypePrefix *string) ([]*v0.NamespaceDefinition, error) {
	mapper := newPositionMapper(schemas)

	// Parse and translate the various schemas.
	definitions := []*v0.NamespaceDefinition{}
	for _, schema := range schemas {
		root := parser.Parse(createAstNode, schema.Source, schema.SchemaString).(*dslNode)
		errors := root.FindAll(dslshape.NodeTypeError)
		if len(errors) > 0 {
			err := errorNodeToError(errors[0], mapper)
			return []*v0.NamespaceDefinition{}, err
		}

		translatedDefs, err := translate(translationContext{
			objectTypePrefix: objectTypePrefix,
		}, root)
		if err != nil {
			return []*v0.NamespaceDefinition{}, err
		}

		definitions = append(definitions, translatedDefs...)
	}

	return definitions, nil
}

func errorNodeToError(node *dslNode, mapper input.PositionMapper) error {
	if node.GetType() != dslshape.NodeTypeError {
		return fmt.Errorf("given none error node")
	}

	errMessage, err := node.GetString(dslshape.NodePredicateErrorMessage)
	if err != nil {
		return fmt.Errorf("could not get error message for error node: %w", err)
	}

	sourceRange, err := node.Range(mapper)
	if err != nil {
		return fmt.Errorf("could not get range for error node: %w", err)
	}

	formattedRange, err := formatRange(sourceRange)
	if err != nil {
		return err
	}

	return fmt.Errorf("parse error in %s: %s", formattedRange, errMessage)
}

func formatRange(rnge input.SourceRange) (string, error) {
	startLine, startCol, err := rnge.Start().LineAndColumn()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("`%s`, line %v, column %v", rnge.Source(), startLine+1, startCol+1), nil
}
