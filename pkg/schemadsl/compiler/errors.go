package compiler

import (
	"strconv"

	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// BaseCompilerError defines an error with contains the base message of the issue
// that occurred.
type BaseCompilerError struct {
	error
	BaseMessage string
}

type errorWithNode struct {
	error
	node            *dslNode
	errorSourceCode string
}

// ErrorWithContext defines an error which contains contextual information.
type ErrorWithContext struct {
	BaseCompilerError
	SourceRange     input.SourceRange
	Source          input.Source
	ErrorSourceCode string
}

func (ewc ErrorWithContext) Unwrap() error {
	return ewc.BaseCompilerError
}

// DetailsMetadata returns the metadata for details for this error.
func (ewc ErrorWithContext) DetailsMetadata() map[string]string {
	startLine, startCol, err := ewc.SourceRange.Start().LineAndColumn()
	if err != nil {
		return map[string]string{}
	}

	endLine, endCol, err := ewc.SourceRange.End().LineAndColumn()
	if err != nil {
		return map[string]string{}
	}

	return map[string]string{
		"start_line_number":     strconv.Itoa(startLine),
		"start_column_position": strconv.Itoa(startCol),
		"end_line_number":       strconv.Itoa(endLine),
		"end_column_position":   strconv.Itoa(endCol),
		"source_code":           ewc.ErrorSourceCode,
	}
}
