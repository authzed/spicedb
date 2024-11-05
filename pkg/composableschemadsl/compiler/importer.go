package compiler

import (
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/pkg/composableschemadsl/input"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
)

type importContext struct {
	pathSegments         []string
	sourceFolder         string
	names                *mapz.Set[string]
	locallyVisitedFiles  *mapz.Set[string]
	globallyVisitedFiles *mapz.Set[string]
}

const SchemaFileSuffix = ".zed"

type ErrCircularImport struct {
	error
	filePath string
}

func importFile(importContext importContext) (*CompiledSchema, error) {
	relativeFilepath := constructFilePath(importContext.pathSegments)
	filePath := path.Join(importContext.sourceFolder, relativeFilepath)

	newSourceFolder := filepath.Dir(filePath)

	currentLocallyVisitedFiles := importContext.locallyVisitedFiles.Copy()

	if ok := currentLocallyVisitedFiles.Add(filePath); !ok {
		// If we've already visited the file on this particular branch walk, it's
		// a circular import issue.
		return nil, &ErrCircularImport{
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

func constructFilePath(segments []string) string {
	return path.Join(segments...) + SchemaFileSuffix
}
