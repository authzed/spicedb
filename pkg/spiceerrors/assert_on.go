//go:build ci

package spiceerrors

import (
	"fmt"
	"runtime"
)

const DebugAssertionsEnabled = true

// DebugAssertf panics if the condition is false in CI builds.
func DebugAssertf(condition func() bool, format string, args ...any) {
	if !condition() {
		panic(fmt.Sprintf(format, args...))
	}
}

// DebugAssertNotNilf panics if the object is nil in CI builds.
func DebugAssertNotNilf(obj any, format string, args ...any) {
	if obj == nil {
		panic(fmt.Sprintf(format, args...))
	}
}

// SetFinalizerForDebugging sets a finalizer on the object for debugging purposes
// in CI builds.
func SetFinalizerForDebugging[T any](obj any, finalizer func(obj T)) {
	runtime.SetFinalizer(obj, finalizer)
}
