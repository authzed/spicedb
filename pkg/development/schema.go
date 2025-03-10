package development

import (
	"errors"
	"github.com/authzed/spicedb/pkg/commonschemadsl"

	"github.com/ccoveille/go-safecast"

	log "github.com/authzed/spicedb/internal/logging"
	composablecompiler "github.com/authzed/spicedb/pkg/composableschemadsl/compiler"
	composableinput "github.com/authzed/spicedb/pkg/composableschemadsl/input"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// CompileSchema compiles a schema into its caveat and namespace definition(s), returning a developer
// error if the schema could not be compiled. The non-developer error is returned only if an
// internal errors occurred.
func CompileSchema(schema string) (commonschemadsl.CompiledSchema, *devinterface.DeveloperError, error) {
	compiled, compilationErr := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schema,
	}, compiler.AllowUnprefixedObjectType())

	if compilationErr != nil {
		devError, err := handleCompilationErrors(compilationErr)
		return nil, devError, err
	}

	return compiled, nil, nil
}

// CompileComposableSchema Same as CompileSchema, but for composable schemas. Uses a different compiler.
func CompileComposableSchema(schema string) (*composablecompiler.CompiledSchema, *devinterface.DeveloperError, error) {
	compiled, compilationErr := composablecompiler.Compile(composablecompiler.InputSchema{
		Source:       composableinput.Source("schema"),
		SchemaString: schema,
	}, composablecompiler.AllowUnprefixedObjectType())

	if compilationErr != nil {
		devError, err := handleCompilationErrors(compilationErr)
		return nil, devError, err
	}

	return compiled, nil, nil
}

// Attempts to turn a compilation error into a DeveloperError that can be displayed
// to the user
func handleCompilationErrors(compilationErr error) (*devinterface.DeveloperError, error) {
	var contextError compiler.WithContextError
	if errors.As(compilationErr, &contextError) {
		line, col, lerr := contextError.SourceRange.Start().LineAndColumn()
		if lerr != nil {
			return nil, lerr
		}

		// NOTE: zeroes are fine here on failure.
		_, err := safecast.ToUint32(line)
		if err != nil {
			log.Err(err).Msg("could not cast lineNumber to uint32")
		}
		_, err := safecast.ToUint32(col)
		if err != nil {
			log.Err(err).Msg("could not cast columnPosition to uint32")
		}
		return &devinterface.DeveloperError{
			Message: contextError.BaseCompilerError.BaseMessage,
			Kind:    devinterface.DeveloperError_SCHEMA_ISSUE,
			Source:  devinterface.DeveloperError_SCHEMA,
			Context: contextError.ErrorSourceCode,
		}, nil
	}
	return nil, compilationErr
}
