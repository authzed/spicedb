package query

// PlanAdvisor provides query plan optimization guidance through hints and mutations.
// Implementations can use internal cost models or other heuristics to suggest
// optimizations without exposing those details to callers.
type PlanAdvisor interface {
	// GetHints returns a list of hints to apply to the given outline node.
	// The canonical key uniquely identifies this node's structure for caching/memoization.
	// The caller is responsible for walking the outline tree and calling this
	// method on each node, then applying the returned hints.
	GetHints(outline Outline, key CanonicalKey) ([]Hint, error)

	// GetMutations returns a list of outline mutations to apply to the given
	// outline node. The canonical key uniquely identifies this node's structure.
	// The caller is responsible for walking the outline tree and
	// applying these transformations.
	GetMutations(outline Outline, key CanonicalKey) ([]OutlineMutation, error)
}
