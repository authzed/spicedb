package spiceerrors

import (
	"fmt"
	"os"
	"strings"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/go-errors/errors"
)

// IsInTests returns true if go test is running
// Based on: https://stackoverflow.com/a/58945030
func IsInTests() bool {
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.") {
			return true
		}
	}
	return false
}

// MustPanicf is a special function for panicing when necessary to violate the linter.
func MustPanicf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// MustBugf returns an error representing a bug in the system. Will panic if run under testing.
func MustBugf(format string, args ...any) error {
	if IsInTests() {
		panic(fmt.Sprintf(format, args...))
	}

	e := errors.Errorf(format, args...)
	return fmt.Errorf("BUG: %s", e.ErrorStack())
}

// MustSafecast converts a numeric value to another numeric type.
// It is intended for conversions that should never fail in practice — if the
// conversion fails, it represents a bug. In tests it panics immediately (same
// sentinel as MustBugf); in production it returns the zero value of NumOut so
// that the caller degrades gracefully rather than returning an error.
func MustSafecast[NumOut safecast.Number, NumIn safecast.Number](value NumIn) NumOut {
	result, err := safecast.Convert[NumOut](value)
	if err != nil {
		if IsInTests() {
			panic(fmt.Sprintf("safecast conversion failed: %v", err))
		}
		var zero NumOut
		return zero
	}
	return result
}
