package caveats

import (
	"strconv"

	"github.com/authzed/cel-go/cel"
	"github.com/rs/zerolog"
)

// EvaluationErr is an error in evaluation of a caveat expression.
type EvaluationErr struct {
	error
}

// MarshalZerologObject implements zerolog.LogObjectMarshaler
func (err EvaluationErr) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error)
}

// DetailsMetadata returns the metadata for details for this error.
func (err EvaluationErr) DetailsMetadata() map[string]string {
	return map[string]string{}
}

// ParameterConversionErr is an error in type conversion of a supplied parameter.
type ParameterConversionErr struct {
	error
	parameterName string
}

// MarshalZerologObject implements zerolog.LogObjectMarshaler
func (err ParameterConversionErr) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Str("parameterName", err.parameterName)
}

// DetailsMetadata returns the metadata for details for this error.
func (err ParameterConversionErr) DetailsMetadata() map[string]string {
	return map[string]string{
		"parameter_name": err.parameterName,
	}
}

// ParameterName is the name of the parameter.
func (err ParameterConversionErr) ParameterName() string {
	return err.parameterName
}

// CompilationErrors is a wrapping error for containing compilation errors for a Caveat.
type CompilationErrors struct {
	error

	issues *cel.Issues
}

// LineNumber is the 0-indexed line number for compilation error.
func (err CompilationErrors) LineNumber() int {
	return err.issues.Errors()[0].Location.Line() - 1
}

// ColumnPositionis the 0-indexed column position for compilation error.
func (err CompilationErrors) ColumnPosition() int {
	return err.issues.Errors()[0].Location.Column() - 1
}

// MarshalZerologObject implements zerolog.LogObjectMarshaler
func (err CompilationErrors) MarshalZerologObject(e *zerolog.Event) {
	e.Err(err.error).Int("lineNumber", err.LineNumber()).Int("columnPosition", err.ColumnPosition())
}

// DetailsMetadata returns the metadata for details for this error.
func (err CompilationErrors) DetailsMetadata() map[string]string {
	return map[string]string{
		"line_number":     strconv.Itoa(err.LineNumber()),
		"column_position": strconv.Itoa(err.ColumnPosition()),
	}
}
