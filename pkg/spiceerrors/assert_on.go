//go:build ci
// +build ci

package spiceerrors

import (
	"fmt"
	"runtime"
)

// DebugAssert panics if the condition is false in CI builds.
func DebugAssert(condition func() bool, format string, args ...any) {
	if !condition() {
		panic(fmt.Sprintf(format, args...))
	}
}

// SetFinalizerForDebugging sets a finalizer on the object for debugging purposes
// in CI builds.
func SetFinalizerForDebugging[T any](obj interface{}, finalizer func(obj T)) {
	runtime.SetFinalizer(obj, finalizer)
}
