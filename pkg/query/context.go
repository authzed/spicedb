package query

import (
	"context"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Context represents a single execution of a query.
// It is both a standard context.Context and all the query-time specific handles needed to evaluate a query, such as which datastore it is running against.
//
// Context is the concrete type that contains the overall handles, and uses the executor as a strategy for continuing execution.
type Context struct {
	context.Context
	Executor          Executor
	Reader            datalayer.RevisionedReader // Datastore reader for this query at a specific revision
	CaveatContext     map[string]any
	CaveatRunner      *caveats.CaveatRunner
	TraceLogger       *TraceLogger // For debugging iterator execution (used by TraceStep calls inside iterators)
	MaxRecursionDepth int          // Maximum depth for recursive iterators (0 = use default of 10)

	// Pagination options for IterSubjects and IterResources
	PaginationCursors map[string]*tuple.Relationship // Cursors for pagination, keyed by iterator ID
	PaginationLimit   *uint64                        // Limit for pagination (max number of results to return)
	PaginationSort    options.SortOrder              // Sort order for pagination

	// observers holds the list of Observer implementations to notify during query execution.
	observers []Observer

	// recursiveFrontierCollectors holds frontier collections for BFS IterSubjects.
	// Key: RecursiveIterator.CanonicalKey().Hash()
	// Value: collected Objects for the next frontier
	// A non-nil entry for an ID enables collection mode for that RecursiveIterator.
	recursiveFrontierCollectors map[uint64][]Object
}

// NewLocalContext creates a new query execution context with a LocalExecutor.
// This is a convenience constructor for tests and local execution scenarios.
func NewLocalContext(stdContext context.Context, opts ...ContextOption) *Context {
	ctx := &Context{
		Context:  stdContext,
		Executor: LocalExecutor{},
	}
	for _, opt := range opts {
		opt(ctx)
	}
	return ctx
}

// ContextOption is a function that configures a Context.
type ContextOption func(*Context)

// WithReader sets the datastore reader for the context.
func WithReader(reader datalayer.RevisionedReader) ContextOption {
	return func(ctx *Context) { ctx.Reader = reader }
}

// WithObserver adds an Observer to the context.
func WithObserver(o Observer) ContextOption {
	return func(ctx *Context) { ctx.observers = append(ctx.observers, o) }
}

// WithTraceLogger sets the trace logger for the context.
func WithTraceLogger(logger *TraceLogger) ContextOption {
	return func(ctx *Context) { ctx.TraceLogger = logger }
}

// WithCaveatRunner sets the caveat runner for the context.
func WithCaveatRunner(runner *caveats.CaveatRunner) ContextOption {
	return func(ctx *Context) { ctx.CaveatRunner = runner }
}

// WithCaveatContext sets the caveat context for the context.
func WithCaveatContext(caveatCtx map[string]any) ContextOption {
	return func(ctx *Context) { ctx.CaveatContext = caveatCtx }
}

// WithMaxRecursionDepth sets the maximum recursion depth for the context.
func WithMaxRecursionDepth(depth int) ContextOption {
	return func(ctx *Context) { ctx.MaxRecursionDepth = depth }
}

// WithPaginationLimit sets the pagination limit for the context.
func WithPaginationLimit(limit uint64) ContextOption {
	return func(ctx *Context) { ctx.PaginationLimit = &limit }
}

// WithPaginationSort sets the pagination sort order for the context.
func WithPaginationSort(sort options.SortOrder) ContextOption {
	return func(ctx *Context) { ctx.PaginationSort = sort }
}

// GetPaginationCursor retrieves the cursor for a specific iterator ID.
func (ctx *Context) GetPaginationCursor(iteratorID string) *tuple.Relationship {
	if ctx.PaginationCursors == nil {
		return nil
	}
	return ctx.PaginationCursors[iteratorID]
}

// SetPaginationCursor sets the cursor for a specific iterator ID.
func (ctx *Context) SetPaginationCursor(iteratorID string, cursor *tuple.Relationship) {
	if ctx.PaginationCursors == nil {
		ctx.PaginationCursors = make(map[string]*tuple.Relationship)
	}
	ctx.PaginationCursors[iteratorID] = cursor
}

func (ctx *Context) TraceStep(it Iterator, step string, data ...any) {
	if ctx.TraceLogger != nil {
		ctx.TraceLogger.LogStep(it, step, data...)
	}
}

func (ctx *Context) TraceEnter(it Iterator, traceString string) {
	if ctx.TraceLogger != nil {
		ctx.TraceLogger.EnterIterator(it, traceString)
	}
}

func (ctx *Context) TraceExit(it Iterator, paths []Path) {
	if ctx.TraceLogger != nil {
		ctx.TraceLogger.ExitIterator(it, paths)
	}
}

// shouldTrace reports whether tracing is enabled on this context.
func (ctx *Context) shouldTrace() bool {
	return ctx.TraceLogger != nil
}

func (ctx *Context) traceEnterIfEnabled(it Iterator, traceString string) Iterator {
	if !ctx.shouldTrace() {
		return nil
	}
	ctx.TraceEnter(it, traceString)
	return it
}

func (ctx *Context) traceExitIfEnabled(it Iterator, paths []Path) {
	if ctx.shouldTrace() && it != nil {
		ctx.TraceExit(it, paths)
	}
}

// wrapPathSeqForTracing wraps a PathSeq to collect results for exit tracing if tracing is enabled,
// otherwise returns the original PathSeq unchanged
func (ctx *Context) wrapPathSeqForTracing(it Iterator, pathSeq PathSeq) PathSeq {
	if !ctx.shouldTrace() || it == nil {
		return pathSeq
	}

	return func(yield func(Path, error) bool) {
		var resultPaths []Path
		defer func() {
			ctx.traceExitIfEnabled(it, resultPaths)
		}()

		for path, err := range pathSeq {
			if err != nil {
				if !yield(path, err) {
					return
				}
				continue
			}

			resultPaths = append(resultPaths, path)
			if !yield(path, nil) {
				return
			}
		}
	}
}

// notifyEnterIterator calls ObserveEnterIterator on all registered observers.
func (ctx *Context) notifyEnterIterator(op ObserverOperation, key CanonicalKey) {
	for _, obs := range ctx.observers {
		obs.ObserveEnterIterator(op, key)
	}
}

// wrapPathSeqWithObservers wraps a PathSeq to notify all registered observers of paths and completion.
// Returns the original PathSeq unchanged when there are no observers.
func (ctx *Context) wrapPathSeqWithObservers(op ObserverOperation, key CanonicalKey, pathSeq PathSeq) PathSeq {
	if len(ctx.observers) == 0 {
		return pathSeq
	}
	return func(yield func(Path, error) bool) {
		defer func() {
			for _, obs := range ctx.observers {
				obs.ObserveReturnIterator(op, key)
			}
		}()
		for path, err := range pathSeq {
			if err == nil {
				for _, obs := range ctx.observers {
					obs.ObservePath(op, key, path)
				}
			}
			if !yield(path, err) {
				return
			}
		}
	}
}

// Check tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
// any of the `resources` are connected to `subject`.
// Returns the sequence of matching paths, if they exist, at most `len(resources)`.
func (ctx *Context) Check(it Iterator, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	tracedIterator := ctx.traceEnterIfEnabled(it, checkTraceString(resources, subject))
	key := it.CanonicalKey()
	ctx.notifyEnterIterator(CheckOperation, key)

	pathSeq, err := ctx.Executor.Check(ctx, it, resources, subject)
	if err != nil {
		return nil, err
	}

	pathSeq = ctx.wrapPathSeqForTracing(tracedIterator, pathSeq)
	return ctx.wrapPathSeqWithObservers(CheckOperation, key, pathSeq), nil
}

// IterSubjects returns a sequence of all the paths in this set that match the given resource.
// The filterSubjectType parameter filters results to only include subjects matching the
// specified ObjectType. If filterSubjectType.Type is empty, no filtering is applied.
func (ctx *Context) IterSubjects(it Iterator, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	tracedIterator := ctx.traceEnterIfEnabled(it, iterSubjectsTraceString(resource, filterSubjectType))
	key := it.CanonicalKey()
	ctx.notifyEnterIterator(IterSubjectsOperation, key)

	pathSeq, err := ctx.Executor.IterSubjects(ctx, it, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}

	pathSeq = ctx.wrapPathSeqForTracing(tracedIterator, pathSeq)
	return ctx.wrapPathSeqWithObservers(IterSubjectsOperation, key, pathSeq), nil
}

// IterResources returns a sequence of all the relations in this set that match the given subject.
// The filterResourceType parameter filters results to only include resources matching the
// specified ObjectType. If filterResourceType.Type is empty, no filtering is applied.
func (ctx *Context) IterResources(it Iterator, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	tracedIterator := ctx.traceEnterIfEnabled(it, iterResourcesTraceString(subject, filterResourceType))
	key := it.CanonicalKey()
	ctx.notifyEnterIterator(IterResourcesOperation, key)

	pathSeq, err := ctx.Executor.IterResources(ctx, it, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	pathSeq = ctx.wrapPathSeqForTracing(tracedIterator, pathSeq)
	return ctx.wrapPathSeqWithObservers(IterResourcesOperation, key, pathSeq), nil
}

// Executor as chooses how to proceed given an iterator -- perhaps in parallel, perhaps by RPC, etc -- and chooses how to process iteration from the subtree.
// The correctness logic for the results that are generated are up to each iterator, and each iterator may use statistics to choose the best, yet still correct, logical evaluation strategy.
// The Executor, meanwhile, makes that evaluation happen in the most convienent form, based on its implementation.
type Executor interface {
	// Check tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
	// any of the `resources` are connected to `subject`.
	// Returns the sequence of matching relations, if they exist, at most `len(resources)`.
	Check(ctx *Context, it Iterator, resources []Object, subject ObjectAndRelation) (PathSeq, error)

	// IterSubjects returns a sequence of all the relations in this set that match the given resource.
	// The filterSubjectType parameter filters results to only include subjects matching the
	// specified ObjectType. If filterSubjectType.Type is empty, no filtering is applied.
	IterSubjects(ctx *Context, it Iterator, resource Object, filterSubjectType ObjectType) (PathSeq, error)

	// IterResources returns a sequence of all the relations in this set that match the given subject.
	// The filterResourceType parameter filters results to only include resources matching the
	// specified ObjectType. If filterResourceType.Type is empty, no filtering is applied.
	IterResources(ctx *Context, it Iterator, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error)
}

// EnableFrontierCollection enables frontier collection for a RecursiveIterator.
// Creates a non-nil entry in the map, which signals collection mode.
func (ctx *Context) EnableFrontierCollection(iteratorID uint64) {
	if ctx.recursiveFrontierCollectors == nil {
		ctx.recursiveFrontierCollectors = make(map[uint64][]Object)
	}
	ctx.recursiveFrontierCollectors[iteratorID] = []Object{}
}

// CollectFrontierObject appends an object to the frontier collection.
// Only appends if collection mode is enabled (non-nil entry exists).
func (ctx *Context) CollectFrontierObject(iteratorID uint64, obj Object) {
	if ctx.recursiveFrontierCollectors == nil {
		return
	}
	if collection, exists := ctx.recursiveFrontierCollectors[iteratorID]; exists {
		ctx.recursiveFrontierCollectors[iteratorID] = append(collection, obj)
	}
}

// ExtractFrontierCollection retrieves and removes the collected frontier.
func (ctx *Context) ExtractFrontierCollection(iteratorID uint64) []Object {
	if ctx.recursiveFrontierCollectors == nil {
		return nil
	}
	collection := ctx.recursiveFrontierCollectors[iteratorID]
	delete(ctx.recursiveFrontierCollectors, iteratorID)
	return collection
}

// IsCollectingFrontier checks if collection mode is enabled (non-nil entry exists).
func (ctx *Context) IsCollectingFrontier(iteratorID uint64) bool {
	if ctx.recursiveFrontierCollectors == nil {
		return false
	}
	_, exists := ctx.recursiveFrontierCollectors[iteratorID]
	return exists
}
