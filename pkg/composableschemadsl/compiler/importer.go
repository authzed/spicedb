package compiler

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
)

type importContext struct {
	path                 string
	sourceFolder         string
	names                *mapz.Set[string]
	locallyVisitedFiles  *mapz.Set[string]
	globallyVisitedFiles *mapz.Set[string]
}

type CircularImportError struct {
	error
	filePath string
}

func importFile(importContext importContext) (*CompiledSchema, error) {
	if err := validateFilepath(importContext.path); err != nil {
		return nil, err
	}
	filePath := path.Join(importContext.sourceFolder, importContext.path)

	newSourceFolder := filepath.Dir(filePath)

	currentLocallyVisitedFiles := importContext.locallyVisitedFiles.Copy()

	if ok := currentLocallyVisitedFiles.Add(filePath); !ok {
		// If we've already visited the file on this particular branch walk, it's
		// a circular import issue.
		return nil, &CircularImportError{
			error:    fmt.Errorf("circular import detected: %s has been visited on this branch", filePath),
			filePath: filePath,
		}
	}

	if ok := importContext.globallyVisitedFiles.Add(filePath); !ok {
		// If the file has already been visited, we short-circuit the import process
		// by not reading the schema file in and compiling a schema with an empty string.
		// This prevents duplicate definitions from ending up in the output, as well
		// as preventing circular imports.
		log.Debug().Str("filepath", filePath).Msg("file %s has already been visited in another part of the walk")
		return compileImpl(InputSchema{
			Source:       input.Source(filePath),
			SchemaString: "",
		},
			compilationContext{
				existingNames:        importContext.names,
				locallyVisitedFiles:  currentLocallyVisitedFiles,
				globallyVisitedFiles: importContext.globallyVisitedFiles,
			},
			AllowUnprefixedObjectType(),
			SourceFolder(newSourceFolder),
		)
	}

	schemaBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}
	log.Trace().Str("schema", string(schemaBytes)).Str("file", filePath).Msg("read schema from file")

	return compileImpl(InputSchema{
		Source:       input.Source(filePath),
		SchemaString: string(schemaBytes),
	},
		compilationContext{
			existingNames:        importContext.names,
			locallyVisitedFiles:  currentLocallyVisitedFiles,
			globallyVisitedFiles: importContext.globallyVisitedFiles,
		},
		AllowUnprefixedObjectType(),
		SourceFolder(newSourceFolder),
	)
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
