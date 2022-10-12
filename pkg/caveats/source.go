package caveats

import (
	"strings"

	"github.com/google/cel-go/common"
)

// SourcePosition is an incoming source position.
type SourcePosition interface {
	// LineAndColumn returns the 0-indexed line number and column position in the source file.
	LineAndColumn() (int, int, error)

	// RunePosition returns the 0-indexed rune position in the source file.
	RunePosition() (int, error)
}

// NewSource creates a new source for compilation into a caveat.
func NewSource(expressionString string, startPosition SourcePosition, name string) (common.Source, error) {
	startingRunePosition, err := startPosition.RunePosition()
	if err != nil {
		return nil, err
	}

	startingLine, startingCol, err := startPosition.LineAndColumn()
	if err != nil {
		return nil, err
	}

	// Synthesize a contents string with the same positioning as the caveat expression,
	// but with all tokens before the expression string replaced with whitespace.
	adjustedContents := strings.Repeat(" ", startingRunePosition-startingLine-startingCol)
	for i := 0; i < startingLine; i++ {
		adjustedContents += "\n"
	}
	adjustedContents += strings.Repeat(" ", startingCol)
	adjustedContents += expressionString

	return common.NewStringSource(adjustedContents, name), nil
}
