package compiler

import (
	"strings"

	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

type positionMapper struct {
	schema InputSchema
	mapper input.SourcePositionMapper
}

func newPositionMapper(schema InputSchema) input.PositionMapper {
	return &positionMapper{
		schema: schema,
		mapper: input.CreateSourcePositionMapper([]byte(schema.SchemaString)),
	}
}

func (pm *positionMapper) RunePositionToLineAndCol(runePosition int, source input.Source) (int, int, error) {
	return pm.mapper.RunePositionToLineAndCol(runePosition)
}

func (pm *positionMapper) LineAndColToRunePosition(lineNumber int, colPosition int, source input.Source) (int, error) {
	return pm.mapper.LineAndColToRunePosition(lineNumber, colPosition)
}

func (pm *positionMapper) TextForLine(lineNumber int, source input.Source) (string, error) {
	lines := strings.Split(pm.schema.SchemaString, "\n")
	return lines[lineNumber], nil
}
