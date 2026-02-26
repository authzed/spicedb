package compiler

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

type CircularImportError struct {
	error
	filePath string
}

func importFile(fsys fs.FS, filePath string) (*dslNode, error) {
	schemaBytes, err := fs.ReadFile(fsys, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read import in schema file %q: %w", filePath, err)
	}
	log.Trace().Str("schema", string(schemaBytes)).Str("file", filePath).Msg("read schema from file")

	parsedSchema, _, err := parseSchema(InputSchema{
		Source:       input.Source(filePath),
		SchemaString: string(schemaBytes),
	})
	return parsedSchema, err
}

// Take a filepath and ensure that it's local to the current context.
func validateFilepath(path string) error {
	if strings.Contains(path, "..") {
		return fmt.Errorf("path %s contains '..'; paths must stay within their directory and this is likely an error", path)
	}
	// NOTE: This is slightly overly restrictive; it should theoretically be possible
	// to take a given filepath and figure out whether it's local to the context where
	// the compiler is being invoked, rather than whether it's local to the source
	// folder of the current context. The assumption is that that won't matter
	// right now, and we can fix it if we need to.
	if !filepath.IsLocal(path) {
		return fmt.Errorf("import path %s does not stay within its folder", path)
	}
	return nil
}
