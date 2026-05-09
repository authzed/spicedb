package errors

import "errors"

// WithSource decorates the error with a Postgres error source
func WithSource(err error, file string, line int32, function string) error {
	if err == nil {
		return nil
	}

	return &withSource{cause: err, file: file, line: line, function: function}
}

// GetSource returns the Postgres source inside the given error. If no error
// hint is an empty string returned.
func GetSource(err error) *Source {
	if s, ok := err.(*withSource); ok {
		return &Source{File: s.file, Line: s.line, Function: s.function}
	}

	if n := errors.Unwrap(err); n != nil {
		return GetSource(n)
	}

	return nil
}

type withSource struct {
	cause    error
	file     string
	line     int32
	function string
}

func (w *withSource) Error() string { return w.cause.Error() }
func (w *withSource) Unwrap() error { return w.cause }
