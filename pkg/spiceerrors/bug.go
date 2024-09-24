package spiceerrors

import (
	"fmt"
	"os"
	"strings"

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

// MustPanic is a special function for panicing when necessary to violate the linter.
func MustPanic(format string, args ...any) {
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
