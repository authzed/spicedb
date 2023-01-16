package compiler

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

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

// SchemaDefinition represents an object or caveat definition in a schema.
type SchemaDefinition interface {
	proto.Message

	GetName() string
}

// CompiledSchema is the result of compiling a schema when there are no errors.
type CompiledSchema struct {
	// ObjectDefinitions holds the object definitions in the schema.
	ObjectDefinitions []*core.NamespaceDefinition

	// CaveatDefinitions holds the caveat definitions in the schema.
	CaveatDefinitions []*core.CaveatDefinition

	// OrderedDefinitions holds the object and caveat definitions in the schema, in the
	// order in which they were found.
	OrderedDefinitions []SchemaDefinition
}

// Compile compilers the input schema into a set of namespace definition protos.
func Compile(schema InputSchema, objectTypePrefix *string) (*CompiledSchema, error) {
	mapper := newPositionMapper(schema)
	root := parser.Parse(createAstNode, schema.Source, schema.SchemaString).(*dslNode)
	errs := root.FindAll(dslshape.NodeTypeError)
	if len(errs) > 0 {
		err := errorNodeToError(errs[0], mapper)
		return nil, err
	}

	compiled, err := translate(translationContext{
		objectTypePrefix: objectTypePrefix,
		mapper:           mapper,
		schemaString:     schema.SchemaString,
	}, root)
	if err != nil {
		var errorWithNode errorWithNode
		if errors.As(err, &errorWithNode) {
			err = toContextError(errorWithNode.error.Error(), errorWithNode.errorSourceCode, errorWithNode.node, mapper)
		}

		return nil, err
	}

	return compiled, nil
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
