package query

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Context represents a single execution of a query.
// It is both a standard context.Context and all the query-time specific handles needed to evaluate a query, such as which datastore it is running against.
//
// Context is the concrete type that contains the overall handles, and uses the executor as a strategy for continuing execution.
type Context struct {
	context.Context
	Executor  Executor
	Datastore datastore.ReadOnlyDatastore
	Revision  datastore.Revision
}

// Check tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
// any of the `resources` are connected to `subject`.
// Returns the sequence of matching relations, if they exist, at most `len(resources)`.
func (ctx *Context) Check(it Iterator, resources []Object, subject ObjectAndRelation) (RelationSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}
	return ctx.Executor.Check(ctx, it, resources, subject)
}

// IterSubjects returns a sequence of all the relations in this set that match the given resource.
func (ctx *Context) IterSubjects(it Iterator, resource Object) (RelationSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}
	return ctx.Executor.IterSubjects(ctx, it, resource)
}

// IterResources returns a sequence of all the relations in this set that match the given subject.
func (ctx *Context) IterResources(it Iterator, subject ObjectAndRelation) (RelationSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}
	return ctx.Executor.IterResources(ctx, it, subject)
}

// Executor as chooses how to proceed given an iterator -- perhaps in parallel, perhaps by RPC, etc -- and chooses how to process iteration from the subtree.
// The correctness logic for the results that are generated are up to each iterator, and each iterator may use statistics to choose the best, yet still correct, logical evaluation strategy.
// The Executor, meanwhile, makes that evaluation happen in the most convienent form, based on its implementation.
type Executor interface {
	// Check tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
	// any of the `resources` are connected to `subject`.
	// Returns the sequence of matching relations, if they exist, at most `len(resources)`.
	Check(ctx *Context, it Iterator, resources []Object, subject ObjectAndRelation) (RelationSeq, error)

	// IterSubjects returns a sequence of all the relations in this set that match the given resource.
	IterSubjects(ctx *Context, it Iterator, resource Object) (RelationSeq, error)

	// IterResources returns a sequence of all the relations in this set that match the given subject.
	IterResources(ctx *Context, it Iterator, subject ObjectAndRelation) (RelationSeq, error)
}
