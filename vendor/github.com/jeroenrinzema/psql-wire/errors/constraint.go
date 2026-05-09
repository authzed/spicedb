package errors

import "errors"

// WithConstraintName decorates the error with a Postgres error constraint
func WithConstraintName(err error, constraint string) error {
	if err == nil {
		return nil
	}

	return &withConstraint{cause: err, constraint: constraint}
}

// GetConstraintName returns the Postgres error constraint name inside the given error.
func GetConstraintName(err error) string {
	if c, ok := err.(*withConstraint); ok {
		return c.constraint
	}

	if n := errors.Unwrap(err); n != nil {
		inner := GetConstraintName(n)
		if inner != "" {
			return inner
		}
	}

	return ""
}

type withConstraint struct {
	cause      error
	constraint string
}

func (w *withConstraint) Error() string { return w.cause.Error() }
func (w *withConstraint) Unwrap() error { return w.cause }
