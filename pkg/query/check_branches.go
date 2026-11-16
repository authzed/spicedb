package query

// NamedCheckBranch identifies an independent relation or permission branch.
// A set of these branches can run concurrently without changing the ordering
// between the direct and indirect forms inside each individual relation.
// For "relation viewer: user | group#member", the direct form checks the user
// tuple first. The indirect form finds related groups and checks membership;
// an unconditional direct grant skips those extra reads. Separate permission
// operands can still run concurrently without speculating on a relation's forms.
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
