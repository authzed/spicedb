package spiceerrors

import (
	"errors"
	"maps"
)

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

// ErrorWithAdditionalDetails is an error that includes additional details.
type ErrorWithAdditionalDetails struct {
	error

	// AdditionalDetails is a map of additional details for the error.
	AdditionalDetails map[string]string
}

func NewErrorWithAdditionalDetails(err error) *ErrorWithAdditionalDetails {
	return &ErrorWithAdditionalDetails{err, nil}
}

// Unwrap returns the inner, wrapped error.
func (err *ErrorWithAdditionalDetails) Unwrap() error {
	return err.error
}

func (err *ErrorWithAdditionalDetails) WithAdditionalDetails(key string, value string) {
	if err.AdditionalDetails == nil {
		err.AdditionalDetails = make(map[string]string)
	}
	err.AdditionalDetails[key] = value
}

func (err *ErrorWithAdditionalDetails) AddToDetails(details map[string]string) map[string]string {
	if err.AdditionalDetails != nil {
		maps.Copy(details, err.AdditionalDetails)
	}

	return details
}
