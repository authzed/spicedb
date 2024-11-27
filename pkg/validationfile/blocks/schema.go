package blocks

import (
	"errors"
	"fmt"

	yamlv3 "gopkg.in/yaml.v3"

	"github.com/ccoveille/go-safecast"

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

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: ps.Schema,
	}, compiler.AllowUnprefixedObjectType())
	if err != nil {
		var errWithContext compiler.WithContextError
		if errors.As(err, &errWithContext) {
			line, col, lerr := errWithContext.SourceRange.Start().LineAndColumn()
			if lerr != nil {
				return lerr
			}

			uintLine, err := safecast.ToUint64(line)
			if err != nil {
				return err
			}
			uintCol, err := safecast.ToUint64(col)
			if err != nil {
				return err
			}

			return spiceerrors.NewWithSourceError(
				fmt.Errorf("error when parsing schema: %s", errWithContext.BaseMessage),
				errWithContext.ErrorSourceCode,
				uintLine+1, // source line is 0-indexed
				uintCol+1,  // source col is 0-indexed
			)
		}

		return fmt.Errorf("error when parsing schema: %w", err)
	}

	ps.CompiledSchema = compiled
	ps.SourcePosition = spiceerrors.SourcePosition{LineNumber: node.Line, ColumnPosition: node.Column}
	return nil
}
