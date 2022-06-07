package development

import (
	"errors"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// CompileSchema compiles a schema into its namespace definition(s), returning a developer
// error if the schema could not be compiled. The non-developer error is returned only if an
// internal errors occurred.
func CompileSchema(schema string) ([]*core.NamespaceDefinition, *devinterface.DeveloperError, error) {
	empty := ""
	namespaces, err := compiler.Compile([]compiler.InputSchema{
		{
			Source:       input.Source("schema"),
			SchemaString: schema,
		},
	}, &empty)

	var contextError compiler.ErrorWithContext
	if errors.As(err, &contextError) {
		line, col, lerr := contextError.SourceRange.Start().LineAndColumn()
		if lerr != nil {
			return []*core.NamespaceDefinition{}, nil, lerr
		}

		return []*core.NamespaceDefinition{}, &devinterface.DeveloperError{
			Message: contextError.BaseCompilerError.BaseMessage,
			Kind:    devinterface.DeveloperError_SCHEMA_ISSUE,
			Source:  devinterface.DeveloperError_SCHEMA,
			Line:    uint32(line) + 1, // 0-indexed in parser.
			Column:  uint32(col) + 1,  // 0-indexed in parser.
			Context: contextError.ErrorSourceCode,
		}, nil
	}

	if err != nil {
		return []*core.NamespaceDefinition{}, nil, err
	}

	return namespaces, nil, nil
}
