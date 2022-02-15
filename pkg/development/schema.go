package development

import (
	"errors"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// CompileSchema compiles a schema into its namespace definition(s), returning a developer
// error if the schema could not be compiled. The non-developer error is returned only if an
// internal errors occurred.
func CompileSchema(schema string) ([]*core.NamespaceDefinition, *v0.DeveloperError, error) {
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

		return []*core.NamespaceDefinition{}, &v0.DeveloperError{
			Message: contextError.BaseCompilerError.BaseMessage,
			Kind:    v0.DeveloperError_SCHEMA_ISSUE,
			Source:  v0.DeveloperError_SCHEMA,
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
