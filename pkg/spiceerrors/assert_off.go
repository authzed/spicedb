//go:build !ci

package spiceerrors

const DebugAssertionsEnabled = false

// DebugAssertf is a no-op in non-CI builds
func DebugAssertf(condition func() bool, format string, args ...any) {
	// Do nothing on purpose
}

// DebugAssertNotNilf is a no-op in non-CI builds
func DebugAssertNotNilf(obj any, format string, args ...any) {
	// Do nothing on purpose
}

// SetFinalizerForDebugging is a no-op in non-CI builds
func SetFinalizerForDebugging[T any](obj any, finalizer func(obj T)) {
	// Do nothing on purpose
}
