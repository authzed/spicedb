package spiceerrors

import (
	"fmt"
	"os"
	"strings"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/go-errors/errors"

	log "github.com/authzed/spicedb/internal/logging"
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

// MustSafecast converts a value from one numeric type to another using safecast.
// If the conversion fails (value out of range), it panics in tests and returns
// the zero value in production. This should only be used where the value is
// expected to always be convertible (e.g., converting from a statically defined
// value or a value known to be non-negative).
func MustSafecast[To, From safecast.Number](from From) To {
	result, err := safecast.Convert[To](from)
	if err != nil {
		if IsInTests() {
			panic(fmt.Sprintf("safecast conversion failed: %v (from %v to %T)", err, from, result))
		}
		// In production, log a warning and return the zero value
		var zero To
		log.Warn().
			Interface("from_value", from).
			Str("from_type", fmt.Sprintf("%T", from)).
			Str("to_type", fmt.Sprintf("%T", zero)).
			Err(err).
			Msg("MustSafecast conversion failed in production, returning zero value")
		return zero
	}
	return result
}
