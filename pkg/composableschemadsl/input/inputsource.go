package input

import (
	"fmt"
)

// BytePosition represents the byte position in a piece of code.
type BytePosition int

// Position represents a position in an arbitrary source file.
type Position struct {
	// LineNumber is the 0-indexed line number.
	LineNumber int

	// ColumnPosition is the 0-indexed column position on the line.
	ColumnPosition int
}

// Source represents the path of a source file.
type Source string

// RangeForRunePosition returns a source range over this source file.
func (is Source) RangeForRunePosition(runePosition int, mapper PositionMapper) SourceRange {
	return is.RangeForRunePositions(runePosition, runePosition, mapper)
}

// PositionForRunePosition returns a source position over this source file.
func (is Source) PositionForRunePosition(runePosition int, mapper PositionMapper) SourcePosition {
	return runeIndexedPosition{is, mapper, runePosition}
}

// PositionFromLineAndColumn returns a source position at the given line and column in this source file.
func (is Source) PositionFromLineAndColumn(lineNumber int, columnPosition int, mapper PositionMapper) SourcePosition {
	return lcIndexedPosition{is, mapper, Position{lineNumber, columnPosition}}
}

// RangeForRunePositions returns a source range over this source file.
func (is Source) RangeForRunePositions(startRune int, endRune int, mapper PositionMapper) SourceRange {
	return sourceRange{is, runeIndexedPosition{is, mapper, startRune}, runeIndexedPosition{is, mapper, endRune}}
}

// RangeForLineAndColPositions returns a source range over this source file.
func (is Source) RangeForLineAndColPositions(start Position, end Position, mapper PositionMapper) SourceRange {
	return sourceRange{is, lcIndexedPosition{is, mapper, start}, lcIndexedPosition{is, mapper, end}}
}

// PositionMapper defines an interface for converting rune position <-> line+col position
// under source files.
type PositionMapper interface {
	// RunePositionToLineAndCol converts the given 0-indexed rune position under the given source file
	// into a 0-indexed line number and column position.
	RunePositionToLineAndCol(runePosition int, path Source) (int, int, error)

	// LineAndColToRunePosition converts the given 0-indexed line number and column position under the
	// given source file into a 0-indexed rune position.
	LineAndColToRunePosition(lineNumber int, colPosition int, path Source) (int, error)

	// TextForLine returns the text for the specified line number.
	TextForLine(lineNumber int, path Source) (string, error)
}

// SourceRange represents a range inside a source file.
type SourceRange interface {
	// Source is the input source for this range.
	Source() Source

	// Start is the starting position of the source range.
	Start() SourcePosition

	// End is the ending position (inclusive) of the source range. If the same as the Start,
	// this range represents a single position.
	End() SourcePosition

	// ContainsPosition returns true if the given range contains the given position.
	ContainsPosition(position SourcePosition) (bool, error)

	// AtStartPosition returns a SourceRange located only at the starting position of this range.
	AtStartPosition() SourceRange

	// String returns a (somewhat) human-readable form of the range.
	String() string
}

// SourcePosition represents a single position in a source file.
type SourcePosition interface {
	// Source is the input source for this position.
	Source() Source

	// RunePosition returns the 0-indexed rune position in the source file.
	RunePosition() (int, error)

	// LineAndColumn returns the 0-indexed line number and column position in the source file.
	LineAndColumn() (int, int, error)

	// LineText returns the text of the line for this position.
	LineText() (string, error)

	// String returns a (somewhat) human-readable form of the position.
	String() string
}

// sourceRange implements the SourceRange interface.
type sourceRange struct {
	source Source
	start  SourcePosition
	end    SourcePosition
}

func (sr sourceRange) Source() Source {
	return sr.source
}

func (sr sourceRange) Start() SourcePosition {
	return sr.start
}

func (sr sourceRange) End() SourcePosition {
	return sr.end
}

func (sr sourceRange) AtStartPosition() SourceRange {
	return sourceRange{sr.source, sr.start, sr.end}
}

func (sr sourceRange) ContainsPosition(position SourcePosition) (bool, error) {
	if position.Source() != sr.source {
		return false, nil
	}

	startRune, err := sr.start.RunePosition()
	if err != nil {
		return false, err
	}

	endRune, err := sr.end.RunePosition()
	if err != nil {
		return false, err
	}

	positionRune, err := position.RunePosition()
	if err != nil {
		return false, err
	}

	return positionRune >= startRune && positionRune <= endRune, nil
}

func (sr sourceRange) String() string {
	return fmt.Sprintf("%v -> %v", sr.start, sr.end)
}

// runeIndexedPosition implements the SourcePosition interface over a rune position.
type runeIndexedPosition struct {
	source       Source
	mapper       PositionMapper
	runePosition int
}

func (ris runeIndexedPosition) Source() Source {
	return ris.source
}

func (ris runeIndexedPosition) RunePosition() (int, error) {
	return ris.runePosition, nil
}

func (ris runeIndexedPosition) LineAndColumn() (int, int, error) {
	if ris.runePosition == 0 {
		return 0, 0, nil
	}
	if ris.mapper == nil {
		return -1, -1, fmt.Errorf("nil mapper")
	}
	return ris.mapper.RunePositionToLineAndCol(ris.runePosition, ris.source)
}

func (ris runeIndexedPosition) String() string {
	return fmt.Sprintf("%s@%v (rune)", ris.source, ris.runePosition)
}

func (ris runeIndexedPosition) LineText() (string, error) {
	lineNumber, _, err := ris.LineAndColumn()
	if err != nil {
		return "", err
	}

	return ris.mapper.TextForLine(lineNumber, ris.source)
}

// lcIndexedPosition implements the SourcePosition interface over a line and colu,n position.
type lcIndexedPosition struct {
	source     Source
	mapper     PositionMapper
	lcPosition Position
}

func (lcip lcIndexedPosition) Source() Source {
	return lcip.source
}

func (lcip lcIndexedPosition) String() string {
	return fmt.Sprintf("%s@%v:%v (line/col)", lcip.source, lcip.lcPosition.LineNumber, lcip.lcPosition.ColumnPosition)
}

func (lcip lcIndexedPosition) RunePosition() (int, error) {
	if lcip.lcPosition.LineNumber == 0 && lcip.lcPosition.ColumnPosition == 0 {
		return 0, nil
	}
	if lcip.mapper == nil {
		return -1, fmt.Errorf("nil mapper")
	}
	return lcip.mapper.LineAndColToRunePosition(lcip.lcPosition.LineNumber, lcip.lcPosition.ColumnPosition, lcip.source)
}

func (lcip lcIndexedPosition) LineAndColumn() (int, int, error) {
	return lcip.lcPosition.LineNumber, lcip.lcPosition.ColumnPosition, nil
}

func (lcip lcIndexedPosition) LineText() (string, error) {
	if lcip.mapper == nil {
		return "", fmt.Errorf("nil mapper")
	}
	return lcip.mapper.TextForLine(lcip.lcPosition.LineNumber, lcip.source)
}
