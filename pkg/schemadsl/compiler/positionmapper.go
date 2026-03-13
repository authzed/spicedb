package compiler

import (
	"strings"

	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// positionMapper converts rune positions to line/column positions and vice versa.
// It is multi-file aware: the root schema file's mapper is created at construction time,
// and imported files are registered via RegisterImportedFile during import resolution.
// This allows correct position mapping for AST nodes from imported files, whose rune
// positions are relative to their own file content rather than the root schema.
//
// This is distinct from development.SchemaPositionMapper, which is a higher-level
// construct that resolves semantic references (e.g., type references, relation references)
// to their target definitions using the compiled schema and type system.
type positionMapper struct {
	mappers  map[input.Source]input.SourcePositionMapper
	contents map[input.Source]string
}

func newPositionMapper(schema InputSchema) *positionMapper {
	pm := &positionMapper{
		mappers:  make(map[input.Source]input.SourcePositionMapper),
		contents: make(map[input.Source]string),
	}
	pm.mappers[schema.Source] = input.CreateSourcePositionMapper([]byte(schema.SchemaString))
	pm.contents[schema.Source] = schema.SchemaString
	return pm
}

// RegisterImportedFile registers an imported file's content so that rune positions
// from that file can be correctly mapped to line/col.
func (pm *positionMapper) RegisterImportedFile(source input.Source, content string) {
	pm.mappers[source] = input.CreateSourcePositionMapper([]byte(content))
	pm.contents[source] = content
}

func (pm *positionMapper) RunePositionToLineAndCol(runePosition int, source input.Source) (int, int, error) {
	return pm.mappers[source].RunePositionToLineAndCol(runePosition)
}

func (pm *positionMapper) LineAndColToRunePosition(lineNumber int, colPosition int, source input.Source) (int, error) {
	return pm.mappers[source].LineAndColToRunePosition(lineNumber, colPosition)
}

func (pm *positionMapper) TextForLine(lineNumber int, source input.Source) (string, error) {
	lines := strings.Split(pm.contents[source], "\n")
	return lines[lineNumber], nil
}
