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
	pathSegments []string
	sourceFolder string
	names        *mapz.Set[string]
}

const SchemaFileSuffix = ".zed"

func importFile(importContext importContext) (*CompiledSchema, error) {
	relativeFilepath := constructFilePath(importContext.pathSegments)
	filePath := path.Join(importContext.sourceFolder, relativeFilepath)

	newSourceFolder := filepath.Dir(filePath)

	var schemaBytes []byte
	schemaBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}
	log.Trace().Str("schema", string(schemaBytes)).Str("file", filePath).Msg("read schema from file")

	compiled, err := compileImpl(InputSchema{
		Source:       input.Source(filePath),
		SchemaString: string(schemaBytes),
	},
		importContext.names,
		AllowUnprefixedObjectType(),
		SourceFolder(newSourceFolder),
	)
	if err != nil {
		return nil, err
	}
	return compiled, nil
}

func constructFilePath(segments []string) string {
	return path.Join(segments...) + SchemaFileSuffix
}
