package spiceerrors

import "time"

// TerminationError represents an error that captures contextual information to make
// available on process termination. The error will be marshalled as JSON and
// serialized into a file-path specified via CLI arguments
type TerminationError struct {
	error
	Component   string            `json:"component"`
	Timestamp   time.Time         `json:"timestamp"`
	ErrorString string            `json:"error"`
	Metadata    map[string]string `json:"metadata"`
	exitCode    int
}

// ExitCode returns the exit code to be returned on process termination
func (e TerminationError) ExitCode() int {
	return e.exitCode
}

// ErrorBuilder is a fluent-style builder for TerminationError
type ErrorBuilder struct {
	terminationErr TerminationError
}

// TerminationError returns the built termination TerminationError
func (eb *ErrorBuilder) Error() TerminationError {
	return eb.terminationErr
}

// Component specifies the component in SpiceDB that
func (eb *ErrorBuilder) Component(component string) *ErrorBuilder {
	eb.terminationErr.Component = component
	return eb
}

// Metadata adds a new key-value pair of metadata to the termination TerminationError being built
func (eb *ErrorBuilder) Metadata(key, value string) *ErrorBuilder {
	eb.terminationErr.Metadata[key] = value
	return eb
}

// ExitCode defines the ExitCode to be used upon process termination. Defaults to 1 if not specified.
func (eb *ErrorBuilder) ExitCode(exitCode int) *ErrorBuilder {
	eb.terminationErr.exitCode = exitCode
	return eb
}

// Timestamp defines the time of the error. Defaults to time.Now().UTC() if not specified.
func (eb *ErrorBuilder) Timestamp(timestamp time.Time) *ErrorBuilder {
	eb.terminationErr.Timestamp = timestamp
	return eb
}

// NewTerminationErrorBuilder returns a new ErrorBuilder for a termination.TerminationError.
func NewTerminationErrorBuilder(err error) *ErrorBuilder {
	return &ErrorBuilder{terminationErr: TerminationError{
		error:       err,
		Component:   "unspecified",
		Timestamp:   time.Now().UTC(),
		ErrorString: err.Error(),
		Metadata:    make(map[string]string, 0),
		exitCode:    1,
	}}
}
