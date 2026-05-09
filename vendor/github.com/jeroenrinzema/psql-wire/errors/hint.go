package errors

import "errors"

// WithHint decorates the error with a Postgres error hint
func WithHint(err error, hint string) error {
	if err == nil {
		return nil
	}

	return &withHint{cause: err, hint: hint}
}

// GetHint returns the Postgres hint inside the given error. If no error
// hint is an empty string returned.
func GetHint(err error) string {
	if h, ok := err.(*withHint); ok {
		return h.hint
	}

	if n := errors.Unwrap(err); n != nil {
		return GetHint(n)
	}

	return ""
}

type withHint struct {
	cause error
	hint  string
}

func (w *withHint) Error() string { return w.cause.Error() }
func (w *withHint) Unwrap() error { return w.cause }
