package compiler

import (
	"fmt"
	"io/fs"
	"path"
	"path/filepath"
	"strings"

	"github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/schemadsl/dslshape"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func importFile(fsys fs.FS, filePath string, toplevel *dslNode, mapper input.PositionMapper) (*dslNode, string, error) {
	if fsys == nil {
		return nil, "", fmt.Errorf("failed to read import in schema file %q: file system was nil", filePath)
	}
	schemaBytes, err := fs.ReadFile(fsys, filePath)
	if err != nil {
		nodeSource, _ := toplevel.GetString(dslshape.NodePredicateSource)
		errMsg := fmt.Errorf("failed to read import %q: %w", filePath, err)
		return nil, "", toContextError(errMsg.Error(), nodeSource, toplevel, mapper)
	}
	logging.Trace().Str("schema", string(schemaBytes)).Str("file", filePath).Msg("read schema from file")

	content := string(schemaBytes)
	parsedSchema, _, err := parseSchema(InputSchema{
		Source:       input.Source(filePath),
		SchemaString: content,
	})
	return parsedSchema, content, err
}

// Take a filepath and ensure that it's local to the current context.
func validateFilepath(importPath string, toplevel *dslNode, mapper input.PositionMapper) error {
	nodeSource, _ := toplevel.GetString(dslshape.NodePredicateSource)
	if strings.Contains(importPath, "..") {
		errMsg := fmt.Errorf("import path %q contains '..'; paths must stay within their directory", importPath)
		return toContextError(errMsg.Error(), nodeSource, toplevel, mapper)
	}
	// Import paths are resolved against an fs.FS, which requires slash-separated
	// relative paths without backslashes on every OS.
	if strings.Contains(importPath, "\\") {
		errMsg := fmt.Errorf("import path %q must use forward slashes ('/')", importPath)
		return toContextError(errMsg.Error(), nodeSource, toplevel, mapper)
	}
	// NOTE: This is slightly overly restrictive; it should theoretically be possible
	// to take a given filepath and figure out whether it's local to the context where
	// the compiler is being invoked, rather than whether it's local to the source
	// folder of the current context. The assumption is that that won't matter
	// right now, and we can fix it if we need to.
	cleaned := path.Clean(importPath)
	if !fs.ValidPath(cleaned) || !filepath.IsLocal(importPath) {
		errMsg := fmt.Errorf("import path %q is outside the directory where the root schema is defined", importPath)
		return toContextError(errMsg.Error(), nodeSource, toplevel, mapper)
	}
	return nil
}
