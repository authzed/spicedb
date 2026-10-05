package optimization

import "github.com/authzed/spicedb/pkg/query"

// RequestParams holds request-specific values available to optimizations and
// optimizer selection. Static (schema-level) optimizations ignore these;
// request-parameterized optimizations (e.g. reachability pruning) use them
// to tailor their behavior. The selection function uses them to decide which
// optimizers to include.
type RequestParams struct {
	// Operation identifies which query operation is being planned.
	// Used by the selection function to determine which optimizers are safe.
	Operation query.Operation
	// SubjectType is the object type of the subject/filter.
	SubjectType string
	// SubjectRelation is the relation on the subject.
	// Set to "..." for ellipsis subjects (e.g. user:...), "" for bare subjects,
	// or a specific relation name (e.g. "viewer").
	SubjectRelation string
}

// OutlineTransform is a function that transforms an entire outline tree.
// Each optimizer produces one of these, and they are applied sequentially.
type OutlineTransform func(query.Outline) query.Outline

// Optimizer describes a single named outline optimization.
type Optimizer struct {
	Name        string
	Description string
	// NewTransform creates a whole-tree transformation for this optimizer,
	// optionally using request-specific parameters. Each transform typically
	// calls query.MutateOutline internally with its own mutations.
	NewTransform func(RequestParams) OutlineTransform
	// Priority controls the order in which optimizations are applied.
	// Higher values run first.
	Priority int
}
