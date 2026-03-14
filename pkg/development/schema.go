package development

import (
	"errors"
	"io/fs"

	"github.com/ccoveille/go-safecast/v2"

	log "github.com/authzed/spicedb/internal/logging"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// CompileOption configures schema compilation.
type CompileOption func(*compileConfig)

type compileConfig struct {
	fsys fs.FS
}

// WithSourceFS enables import resolution using the given filesystem.
// The filesystem should be rooted at the directory containing the schema file.
func WithSourceFS(fsys fs.FS) CompileOption {
	return func(cfg *compileConfig) { cfg.fsys = fsys }
}

// CompileSchema compiles a schema into its caveat and namespace definition(s), returning a developer
// error if the schema could not be compiled. The non-developer error is returned only if an
// internal errors occurred.
func CompileSchema(schema string, opts ...CompileOption) (*compiler.CompiledSchema, *devinterface.DeveloperError, error) {
	cfg := &compileConfig{}
	for _, o := range opts {
		o(cfg)
	}

	var compilerOpts []compiler.Option
	if cfg.fsys != nil {
		compilerOpts = append(compilerOpts, compiler.SourceFS(cfg.fsys))
	}

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schema,
	}, compiler.AllowUnprefixedObjectType(), compilerOpts...)

	var contextError compiler.WithContextError
	if errors.As(err, &contextError) {
		line, col, lerr := contextError.SourceRange.Start().LineAndColumn()
		if lerr != nil {
			return nil, nil, lerr
		}

		// NOTE: zeroes are fine here on failure.
		uintLine, err := safecast.Convert[uint32](line)
		if err != nil {
			log.Err(err).Msg("could not cast lineNumber to uint32")
		}
		uintColumn, err := safecast.Convert[uint32](col)
		if err != nil {
			log.Err(err).Msg("could not cast columnPosition to uint32")
		}
		return nil, &devinterface.DeveloperError{
			Message: contextError.BaseMessage,
			Kind:    devinterface.DeveloperError_SCHEMA_ISSUE,
			Source:  devinterface.DeveloperError_SCHEMA,
			Line:    uintLine + 1,   // 0-indexed in parser.
			Column:  uintColumn + 1, // 0-indexed in parser.
			Context: contextError.ErrorSourceCode,
		}, nil
	}

	if err != nil {
		return nil, nil, err
	}

	return compiled, nil, nil
}
