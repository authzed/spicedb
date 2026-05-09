package errors

import (
	"errors"
)

// WithSeverity decorates the error with a Postgres error severity
func WithSeverity(err error, severity Severity) error {
	if err == nil {
		return nil
	}

	return &withSeverity{cause: err, severity: severity}
}

// GetSeverity returns the Postgres error severity inside the given error.
func GetSeverity(err error) Severity {
	if c, ok := err.(*withSeverity); ok {
		return c.severity
	}

	if n := errors.Unwrap(err); n != nil {
		inner := GetSeverity(n)
		if inner != "" {
			return inner
		}
	}

	return ""
}

// DefaultSeverity returns the default severity (ERROR) if no valid severity
// has been defined.
func DefaultSeverity(severity Severity) Severity {
	if severity == "" {
		return LevelError
	}

	return severity
}

type withSeverity struct {
	cause    error
	severity Severity
}

func (w *withSeverity) Error() string { return w.cause.Error() }
func (w *withSeverity) Unwrap() error { return w.cause }
