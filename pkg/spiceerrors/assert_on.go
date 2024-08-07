//go:build ci
// +build ci

package spiceerrors

import "fmt"

// DebugAssert panics if the condition is false in CI builds.
func DebugAssert(condition func() bool, format string, args ...any) {
	if !condition() {
		panic(fmt.Sprintf(format, args...))
	}
}
