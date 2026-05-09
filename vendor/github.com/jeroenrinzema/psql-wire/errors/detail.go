package errors

import "errors"

// WithDetail decorates the error with Postgres error details
func WithDetail(err error, detail string) error {
	if err == nil {
		return nil
	}

	return &withDetail{cause: err, detail: detail}
}

// GetDetail returns the Postgres detail inside the given error. If no error
// detail is an empty string returned.
func GetDetail(err error) string {
	if h, ok := err.(*withDetail); ok {
		return h.detail
	}

	if n := errors.Unwrap(err); n != nil {
		return GetDetail(n)
	}

	return ""
}

type withDetail struct {
	cause  error
	detail string
}

func (w *withDetail) Error() string { return w.cause.Error() }
func (w *withDetail) Unwrap() error { return w.cause }
