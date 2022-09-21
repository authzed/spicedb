package spiceerrors

import "errors"

// SourcePosition is a position in the input source.
type SourcePosition struct {
	// LineNumber is the 1-indexed line number in the input source.
	LineNumber int

	// ColumnPosition is the 1-indexed column position in the input source.
	ColumnPosition int
}

// ErrorWithSource is an error that includes the source text and position
// information.
type ErrorWithSource struct {
	error

	// SourceCodeString is the input source code string for the error.
	SourceCodeString string

	// LineNumber is the (1-indexed) line number of the error, or 0 if unknown.
	LineNumber uint64

	// ColumnPosition is the (1-indexed) column position of the error, or 0 if
	// unknown.
	ColumnPosition uint64
}

// HasMetadata indicates that the error has metadata defined.
type HasMetadata interface {
	// DetailsMetadata returns the metadata for details for this error.
	DetailsMetadata() map[string]string
}

// Unwrap returns the inner, wrapped error.
func (err *ErrorWithSource) Unwrap() error {
	return err.error
}

// NewErrorWithSource creates and returns a new ErrorWithSource.
func NewErrorWithSource(err error, sourceCodeString string, oneIndexedLineNumber uint64, oneIndexedColumnPosition uint64) *ErrorWithSource {
	return &ErrorWithSource{err, sourceCodeString, oneIndexedLineNumber, oneIndexedColumnPosition}
}

// AsErrorWithSource returns the error as an ErrorWithSource, if applicable.
func AsErrorWithSource(err error) (*ErrorWithSource, bool) {
	var serr *ErrorWithSource
	if errors.As(err, &serr) {
		return serr, true
	}
	return nil, false
}
