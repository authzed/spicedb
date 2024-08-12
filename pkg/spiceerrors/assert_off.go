//go:build !ci
// +build !ci

package spiceerrors

// DebugAssert is a no-op in non-CI builds
func DebugAssert(condition func() bool, format string, args ...any) {
	// Do nothing on purpose
}
