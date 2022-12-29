package spiceerrors

import (
	"fmt"
	"os"
	"strings"
)

// Based on: https://stackoverflow.com/a/58945030
func isInTests() bool {
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
	if isInTests() {
		panic(fmt.Sprintf(format, args...))
	}

	return fmt.Errorf("BUG: "+format, args...)
}
