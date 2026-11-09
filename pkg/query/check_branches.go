package query

// NamedCheckBranch identifies an independent relation or permission branch.
// A set of these branches can run concurrently without changing the ordering
// between the direct and indirect forms inside each individual relation.
type NamedCheckBranch interface {
	// IsNamedCheckBranch reports whether the iterator preserves a named boundary.
	IsNamedCheckBranch() bool
}

func namedBatchBranches(children []Iterator) bool {
	for _, child := range children {
		branch, ok := child.(NamedCheckBranch)
		if !ok || !branch.IsNamedCheckBranch() {
			return false
		}
	}

	return true
}

// Set iterator wire flags retain branch ownership across dispatch. Scheduler
// limits remain receiver policy and are never serialized with this marker.
const independentCheckBranchesFlag uint = 0
