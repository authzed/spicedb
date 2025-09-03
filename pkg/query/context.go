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
// any of the `resourceIDs` are connected to `subjectID`.
// Returns the sequence of matching relations, if they exist, at most `len(resourceIDs)`.
func (ctx *Context) Check(it Iterator, resourceIDs []string, subjectID string) (RelationSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}
	return ctx.Executor.Check(ctx, it, resourceIDs, subjectID)
}

// IterSubjects returns a sequence of all the relations in this set that match the given resourceID.
func (ctx *Context) IterSubjects(it Iterator, resourceID string) (RelationSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}
	return ctx.Executor.IterSubjects(ctx, it, resourceID)
}

// IterResources returns a sequence of all the relations in this set that match the given subjectID.
func (ctx *Context) IterResources(it Iterator, subjectID string) (RelationSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}
	return ctx.Executor.IterResources(ctx, it, subjectID)
}

// Executor as chooses how to proceed given an iterator -- perhaps in parallel, perhaps by RPC, etc -- and chooses how to process iteration from the subtree.
// The correctness logic for the results that are generated are up to each iterator, and each iterator may use statistics to choose the best, yet still correct, logical evaluation strategy.
// The Executor, meanwhile, makes that evaluation happen in the most convienent form, based on its implementation.
type Executor interface {
	// Check tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
	// any of the `resourceIDs` are connected to `subjectID`.
	// Returns the sequence of matching relations, if they exist, at most `len(resourceIDs)`.
	Check(ctx *Context, it Iterator, resourceIDs []string, subjectID string) (RelationSeq, error)

	// IterSubjects returns a sequence of all the relations in this set that match the given resourceID.
	IterSubjects(ctx *Context, it Iterator, resourceID string) (RelationSeq, error)

	// IterResources returns a sequence of all the relations in this set that match the given subjectID.
	IterResources(ctx *Context, it Iterator, subjectID string) (RelationSeq, error)
}
