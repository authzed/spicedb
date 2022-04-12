package compiler

import (
	"errors"
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/schemadsl/parser"
)

// InputSchema defines the input for a Compile.
type InputSchema struct {
	// Source is the source of the schema being compiled.
	Source input.Source

	// Schema is the contents being compiled.
	SchemaString string
}

// ErrorWithContext defines an error which contains contextual information.
type ErrorWithContext struct {
	BaseCompilerError
	SourceRange     input.SourceRange
	Source          input.Source
	ErrorSourceCode string
}

// BaseCompilerError defines an error with contains the base message of the issue
// that occurred.
type BaseCompilerError struct {
	error
	BaseMessage string
}

type errorWithNode struct {
	error
	node *dslNode
}

// Compile compilers the input schema(s) into a set of namespace definition protos.
func Compile(schemas []InputSchema, objectTypePrefix *string) ([]*core.NamespaceDefinition, error) {
	mapper := newPositionMapper(schemas)

	// Parse and translate the various schemas.
	definitions := []*core.NamespaceDefinition{}
	for _, schema := range schemas {
		root := parser.Parse(createAstNode, schema.Source, schema.SchemaString).(*dslNode)
		errs := root.FindAll(dslshape.NodeTypeError)
		if len(errs) > 0 {
			err := errorNodeToError(errs[0], mapper)
			return []*core.NamespaceDefinition{}, err
		}

		translatedDefs, err := translate(translationContext{
			objectTypePrefix: objectTypePrefix,
			mapper:           mapper,
		}, root)
		if err != nil {
			var errorWithNode errorWithNode
			if errors.As(err, &errorWithNode) {
				err = toContextError(errorWithNode.error.Error(), "", errorWithNode.node, mapper)
			}

			return []*core.NamespaceDefinition{}, err
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

	errorSourceCode := ""
	if node.Has(dslshape.NodePredicateErrorSource) {
		es, err := node.GetString(dslshape.NodePredicateErrorSource)
		if err != nil {
			return fmt.Errorf("could not get error source for error node: %w", err)
		}

		errorSourceCode = es
	}

	return toContextError(errMessage, errorSourceCode, node, mapper)
}

func toContextError(errMessage string, errorSourceCode string, node *dslNode, mapper input.PositionMapper) error {
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
		BaseCompilerError: BaseCompilerError{
			error:       fmt.Errorf("parse error in %s: %s", formattedRange, errMessage),
			BaseMessage: errMessage,
		},
		SourceRange:     sourceRange,
		Source:          input.Source(source),
		ErrorSourceCode: errorSourceCode,
	}
}

func formatRange(rnge input.SourceRange) (string, error) {
	startLine, startCol, err := rnge.Start().LineAndColumn()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("`%s`, line %v, column %v", rnge.Source(), startLine+1, startCol+1), nil
}
