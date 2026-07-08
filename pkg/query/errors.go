package query

import "fmt"

// MaxRecursionDepthError indicates that a recursive traversal did not resolve
// within the configured maximum recursion depth. It mirrors the legacy
// dispatcher's MaxDepthExceeded semantics: the answer is unknown, not negative.
// Callers must surface this as an error rather than treating it as NOT_MEMBER or
// an empty result set.
type MaxRecursionDepthError struct {
	Depth int
}

func (e MaxRecursionDepthError) Error() string {
	return fmt.Sprintf("max recursion depth (%d) exceeded during recursive traversal: this usually indicates a recursive or too deep data dependency", e.Depth)
}
