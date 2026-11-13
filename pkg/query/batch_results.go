package query

// CheckResultMode specifies whether a batch must resolve every target.
type CheckResultMode uint8

const (
	// AllCheckResults returns a matching path or a definite denial for every target.
	AllCheckResults CheckResultMode = iota

	// AllowSingleResult permits stopping after an unconditional grant. Returned
	// paths retain input slots, but nil slots may be unvisited rather than denials.
	// Without an unconditional grant, every target is resolved.
	AllowSingleResult
)

// checkBatchMode preserves exhaustive callbacks for legacy observers, whose
// interface cannot represent a skipped check separately from a denial.
func (ctx *Context) checkBatchMode(modes []CheckResultMode) CheckResultMode {
	if len(ctx.observers) > 0 {
		return AllCheckResults
	}

	if len(modes) == 0 {
		return AllCheckResults
	}

	return modes[0]
}

func hasUnconditionalGrant(paths []*Path) bool {
	for _, path := range paths {
		if path != nil && path.Caveat == nil {
			return true
		}
	}

	return false
}

// pathsAreUnconditional guards existential propagation across arrow edges.
// A caveat on any connecting edge can invalidate the selected child grant.
func pathsAreUnconditional(paths []*Path) bool {
	for _, path := range paths {
		if path.Caveat != nil {
			return false
		}
	}

	return true
}
