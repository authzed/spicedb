package caveats

import (
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
func NewSource(expressionString string, name string) (common.Source, error) {
	return common.NewStringSource(expressionString, name), nil
}
