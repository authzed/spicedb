package input

import (
	"fmt"
	"strings"

	"github.com/emirpasic/gods/trees/redblacktree"
)

// SourcePositionMapper defines a helper struct for cached, faster lookup of rune position <->
// (line, column) for a specific source file.
type SourcePositionMapper struct {
	// rangeTree holds a tree that maps from rune position to a line and start position.
	rangeTree *redblacktree.Tree

	// lineMap holds a map from line number to rune positions for that line.
	lineMap map[int]inclusiveRange
}

// EmptySourcePositionMapper returns an empty source position mapper.
func EmptySourcePositionMapper() SourcePositionMapper {
	rangeTree := redblacktree.NewWith(inclusiveComparator)
	return SourcePositionMapper{rangeTree, map[int]inclusiveRange{}}
}

// CreateSourcePositionMapper returns a source position mapper for the contents of a source file.
func CreateSourcePositionMapper(contents []byte) SourcePositionMapper {
	lines := strings.Split(string(contents), "\n")
	rangeTree := redblacktree.NewWith(inclusiveComparator)
	lineMap := map[int]inclusiveRange{}

	currentStart := int(0)
	for index, line := range lines {
		lineEnd := currentStart + len(line)
		rangeTree.Put(inclusiveRange{currentStart, lineEnd}, lineAndStart{index, currentStart})
		lineMap[index] = inclusiveRange{currentStart, lineEnd}
		currentStart = lineEnd + 1
	}

	return SourcePositionMapper{rangeTree, lineMap}
}

type inclusiveRange struct {
	start int
	end   int
}

type lineAndStart struct {
	lineNumber    int
	startPosition int
}

func inclusiveComparator(a, b interface{}) int {
	i1 := a.(inclusiveRange)
	i2 := b.(inclusiveRange)

	if i1.start >= i2.start && i1.end <= i2.end {
		return 0
	}

	diff := int64(i1.start) - int64(i2.start)

	if diff < 0 {
		return -1
	}
	if diff > 0 {
		return 1
	}
	return 0
}

// RunePositionToLineAndCol returns the line number and column position of the rune position in source.
func (spm SourcePositionMapper) RunePositionToLineAndCol(runePosition int) (int, int, error) {
	ls, found := spm.rangeTree.Get(inclusiveRange{runePosition, runePosition})
	if !found {
		return 0, 0, fmt.Errorf("unknown rune position %v in source file", runePosition)
	}

	las := ls.(lineAndStart)
	return las.lineNumber, runePosition - las.startPosition, nil
}

// LineAndColToRunePosition returns the rune position of the line number and column position in source.
func (spm SourcePositionMapper) LineAndColToRunePosition(lineNumber int, colPosition int) (int, error) {
	lineRuneInfo, hasLine := spm.lineMap[lineNumber]
	if !hasLine {
		return 0, fmt.Errorf("unknown line %v in source file", lineNumber)
	}

	if colPosition > lineRuneInfo.end-lineRuneInfo.start {
		return 0, fmt.Errorf("column position %v not found on line %v in source file", colPosition, lineNumber)
	}

	return lineRuneInfo.start + colPosition, nil
}
