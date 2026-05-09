package errors

import (
	"errors"
	"strings"

	"github.com/jeroenrinzema/psql-wire/codes"
)

// WithCode decorates the error with a Postgres error code
func WithCode(err error, code codes.Code) error {
	if err == nil {
		return nil
	}

	return &withCode{cause: err, code: code}
}

// GetCode returns the Postgres error code inside the given error. If no error
// code is found a Uncategorized error code returned.
func GetCode(err error) (code codes.Code) {
	code = codes.Uncategorized
	if c, ok := err.(*withCode); ok {
		return c.code
	}

	if n := errors.Unwrap(err); n != nil {
		inner := GetCode(n)
		code = combineCodes(inner, code)
	}

	return code
}

type withCode struct {
	cause error
	code  codes.Code
}

func (w *withCode) Error() string { return w.cause.Error() }
func (w *withCode) Unwrap() error { return w.cause }

// combineCodes returns the most specific error code.
func combineCodes(inner, outer codes.Code) codes.Code {
	if outer == codes.Uncategorized {
		return inner
	}
	if strings.HasPrefix(string(outer), "XX") {
		return outer
	}
	if inner != codes.Uncategorized {
		return inner
	}
	return outer
}
