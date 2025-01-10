//go:build !ci
// +build !ci

package spiceerrors

const DebugAssertionsEnabled = false

// DebugAssert is a no-op in non-CI builds
func DebugAssert(condition func() bool, format string, args ...any) {
	// Do nothing on purpose
}

// DebugAssertNotNil is a no-op in non-CI builds
func DebugAssertNotNil(obj any, format string, args ...any) {
	// Do nothing on purpose
}

// SetFinalizerForDebugging is a no-op in non-CI builds
func SetFinalizerForDebugging[T any](obj interface{}, finalizer func(obj T)) {
	// Do nothing on purpose
}
