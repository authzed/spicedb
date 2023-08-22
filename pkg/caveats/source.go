package caveats

import (
	"strings"

	"github.com/authzed/cel-go/common"
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
	// but with all tokens before the expression string replaced with whitespace. This is
	// done to ensure that the positions produced by the CEL parser match those in the parent
	// schema/string. Otherwise, CEL would report the positions relative to only the expression
	// itself, which is not as nice for debugging.
	// NOTE: This is likely not the most efficient means of doing this, so reevaluate if/when the
	// CEL location code allows for better customization of offsets or starts returning more
	// well-typed errors that we can more easily rewrite.
	adjustedContents := strings.Repeat(" ", startingRunePosition-startingLine-startingCol)
	for i := 0; i < startingLine; i++ {
		adjustedContents += "\n"
	}
	adjustedContents += strings.Repeat(" ", startingCol)
	adjustedContents += expressionString

	return common.NewStringSource(adjustedContents, name), nil
}
