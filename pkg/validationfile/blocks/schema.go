package blocks

import (
	"errors"
	"fmt"

	yamlv3 "gopkg.in/yaml.v3"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// ParsedSchema is the parsed schema in a validationfile.
type ParsedSchema struct {
	// Schema is the schema found.
	Schema string

	// SourcePosition is the position of the schema in the file.
	SourcePosition spiceerrors.SourcePosition

	// CompiledSchema is the compiled schema.
	CompiledSchema *compiler.CompiledSchema
}

// UnmarshalYAML is a custom unmarshaller.
func (ps *ParsedSchema) UnmarshalYAML(node *yamlv3.Node) error {
	err := node.Decode(&ps.Schema)
	if err != nil {
		return convertYamlError(err)
	}

	empty := ""
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: ps.Schema,
	}, &empty)
	if err != nil {
		var errWithContext compiler.ErrorWithContext
		if errors.As(err, &errWithContext) {
			line, col, lerr := errWithContext.SourceRange.Start().LineAndColumn()
			if lerr != nil {
				return lerr
			}

			return spiceerrors.NewErrorWithSource(
				fmt.Errorf("error when parsing schema: %s", errWithContext.BaseMessage),
				errWithContext.ErrorSourceCode,
				uint64(line+1), // source line is 0-indexed
				uint64(col+1),  // source col is 0-indexed
			)
		}

		return fmt.Errorf("error when parsing schema: %w", err)
	}

	ps.CompiledSchema = compiled
	ps.SourcePosition = spiceerrors.SourcePosition{LineNumber: node.Line, ColumnPosition: node.Column}
	return nil
}
