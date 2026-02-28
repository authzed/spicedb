package query

// PlanAdvisor provides query plan optimization guidance through hints and mutations.
// Implementations can use internal cost models or other heuristics to suggest
// optimizations without exposing those details to callers.
type PlanAdvisor interface {
	// GetHints returns a list of hints to apply to the given outline node.
	// keySource can be used to resolve the CanonicalKey for any node in the
	// outline tree by its ID, including the current node and its children.
	// The caller is responsible for walking the outline tree and calling this
	// method on each node, then applying the returned hints.
	GetHints(outline Outline, keySource CanonicalKeySource) ([]Hint, error)

	// GetMutations returns a list of outline mutations to apply to the given
	// outline node. keySource can be used to resolve the CanonicalKey for any
	// node in the outline tree by its ID, including the current node and its children.
	// The caller is responsible for walking the outline tree and
	// applying these transformations.
	GetMutations(outline Outline, keySource CanonicalKeySource) ([]OutlineMutation, error)
}
