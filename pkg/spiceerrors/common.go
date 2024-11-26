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

// WithSourceError is an error that includes the source text and position
// information.
type WithSourceError struct {
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
func (err *WithSourceError) Unwrap() error {
	return err.error
}

// NewWithSourceError creates and returns a new WithSourceError.
func NewWithSourceError(err error, sourceCodeString string, oneIndexedLineNumber uint64, oneIndexedColumnPosition uint64) *WithSourceError {
	return &WithSourceError{err, sourceCodeString, oneIndexedLineNumber, oneIndexedColumnPosition}
}

// AsWithSourceError returns the error as an WithSourceError, if applicable.
func AsWithSourceError(err error) (*WithSourceError, bool) {
	var serr *WithSourceError
	if errors.As(err, &serr) {
		return serr, true
	}
	return nil, false
}

// WithAdditionalDetailsError is an error that includes additional details.
type WithAdditionalDetailsError struct {
	error

	// AdditionalDetails is a map of additional details for the error.
	AdditionalDetails map[string]string
}

func NewWithAdditionalDetailsError(err error) *WithAdditionalDetailsError {
	return &WithAdditionalDetailsError{err, nil}
}

// Unwrap returns the inner, wrapped error.
func (err *WithAdditionalDetailsError) Unwrap() error {
	return err.error
}

func (err *WithAdditionalDetailsError) WithAdditionalDetails(key string, value string) {
	if err.AdditionalDetails == nil {
		err.AdditionalDetails = make(map[string]string)
	}
	err.AdditionalDetails[key] = value
}

func (err *WithAdditionalDetailsError) AddToDetails(details map[string]string) map[string]string {
	if err.AdditionalDetails != nil {
		maps.Copy(details, err.AdditionalDetails)
	}

	return details
}
