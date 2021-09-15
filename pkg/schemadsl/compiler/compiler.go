package compiler

import (
	"errors"
	"fmt"

	v0 "github.com/authzed/spicedb/internal/proto/authzed/api/v0"
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

// ErrorWithContext defines an error which contains contextual information.
type ErrorWithContext struct {
	error
	SourceRange input.SourceRange
	Source      input.InputSource
}

type errorWithNode struct {
	error
	node *dslNode
}

// Compile compilers the input schema(s) into a set of namespace definition protos.
func Compile(schemas []InputSchema, objectTypePrefix *string) ([]*v0.NamespaceDefinition, error) {
	mapper := newPositionMapper(schemas)

	// Parse and translate the various schemas.
	definitions := []*v0.NamespaceDefinition{}
	for _, schema := range schemas {
		root := parser.Parse(createAstNode, schema.Source, schema.SchemaString).(*dslNode)
		errs := root.FindAll(dslshape.NodeTypeError)
		if len(errs) > 0 {
			err := errorNodeToError(errs[0], mapper)
			return []*v0.NamespaceDefinition{}, err
		}

		translatedDefs, err := translate(translationContext{
			objectTypePrefix: objectTypePrefix,
		}, root)
		if err != nil {
			var errorWithNode errorWithNode
			if errors.As(err, &errorWithNode) {
				err = toContextError(errorWithNode.error.Error(), errorWithNode.node, mapper)
			}

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

	return toContextError(errMessage, node, mapper)
}

func toContextError(errMessage string, node *dslNode, mapper input.PositionMapper) error {
	sourceRange, err := node.Range(mapper)
	if err != nil {
		return fmt.Errorf("could not get range for error node: %w", err)
	}

	formattedRange, err := formatRange(sourceRange)
	if err != nil {
		return err
	}

	source, err := node.GetString(dslshape.NodePredicateSource)
	if err != nil {
		return fmt.Errorf("missing source for node: %w", err)
	}

	return ErrorWithContext{
		error:       fmt.Errorf("parse error in %s: %s", formattedRange, errMessage),
		SourceRange: sourceRange,
		Source:      input.InputSource(source),
	}
}

func formatRange(rnge input.SourceRange) (string, error) {
	startLine, startCol, err := rnge.Start().LineAndColumn()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("`%s`, line %v, column %v", rnge.Source(), startLine+1, startCol+1), nil
}
