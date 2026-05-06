package query

import (
	"context"
	"sync"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/datalayer"
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
	Reader            QueryDatastoreReader // Datastore reader for this query at a specific revision
	CaveatContext     map[string]any
	CaveatRunner      *caveats.CaveatRunner
	TraceLogger       *TraceLogger // For debugging iterator execution (used by TraceStep calls inside iterators)
	MaxRecursionDepth int          // Maximum depth for recursive iterators (0 = use default of 10)

	// TopLevelOperation records the first operation (Check/IterSubjects/IterResources) invoked on
	// this context. It is set exactly once — on the first call — and never changes after that.
	// Iterators may inspect it to skip work that is irrelevant for the current operation type.
	TopLevelOperation Operation

	// BatchedArrows enables the batched arrow check path: arrows drain their
	// left/right side into a slice and issue a single CheckMany call instead of
	// one Check per element. Always set when running with DispatchExecutor so
	// arrow fanout collapses into a single RPC per alias boundary; left off in
	// LocalExecutor by default to keep the simpler path.
	BatchedArrows bool

	// Pagination options for IterSubjects and IterResources
	PaginationCursors map[string]*tuple.Relationship // Cursors for pagination, keyed by iterator ID
	PaginationLimit   *uint64                        // Limit for pagination (max number of results to return)

	// observers holds the list of Observer implementations to notify during query execution.
	observers []Observer

	// recursiveFrontierCollectors holds frontier collections for BFS IterSubjects.
	// Key: RecursiveIterator.CanonicalKey().Hash()
	// Value: collected Objects for the next frontier
	// A non-nil entry for an ID enables collection mode for that RecursiveIterator.
	recursiveFrontierCollectors map[uint64][]Object

	// topLevelIterator is the iterator passed to the first IterResources or
	// IterSubjects call on this context. Nil means not yet set.
	// Used to wrap only the top-level call with DeduplicatePathSeq.
	topLevelIterator Iterator
	topLevelOnce     sync.Once
}

// NewQueryContext builds a query plan exection context with the given executor implementation.
func NewQueryContext(stdContext context.Context, executor Executor, opts ...ContextOption) *Context {
	ctx := &Context{
		Context:  stdContext,
		Executor: executor,
	}
	for _, opt := range opts {
		opt(ctx)
	}
	return ctx
}

// NewLocalContext creates a new query execution context with a LocalExecutor.
// This is a convenience constructor for tests and local execution scenarios.
func NewLocalContext(stdContext context.Context, opts ...ContextOption) *Context {
	return NewQueryContext(stdContext, LocalExecutor{}, opts...)
}

// ContextOption is a function that configures a Context.
type ContextOption func(*Context)

// WithReader sets the datastore reader for the context.
func WithReader(reader QueryDatastoreReader) ContextOption {
	return func(ctx *Context) { ctx.Reader = reader }
}

// WithRevisionedReader wraps a datalayer.RevisionedReader as a QueryDatastoreReader
// and sets it as the datastore reader for the context.
func WithRevisionedReader(reader datalayer.RevisionedReader) ContextOption {
	return func(ctx *Context) { ctx.Reader = NewQueryDatastoreReader(reader) }
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

// WithBatchedArrows enables the batched arrow check path on the context.
func WithBatchedArrows(enabled bool) ContextOption {
	return func(ctx *Context) { ctx.BatchedArrows = enabled }
}

// WithPaginationLimit sets the pagination limit for the context.
func WithPaginationLimit(limit uint64) ContextOption {
	return func(ctx *Context) { ctx.PaginationLimit = &limit }
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

func (ctx *Context) TraceExit(it Iterator, paths []*Path) {
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

func (ctx *Context) traceExitIfEnabled(it Iterator, paths []*Path) {
	if ctx.shouldTrace() && it != nil {
		ctx.TraceExit(it, paths)
	}
}

// traceCheckResult records the result of a Check call for exit tracing if tracing is enabled.
func (ctx *Context) traceCheckResult(it Iterator, path *Path) {
	if !ctx.shouldTrace() || it == nil {
		return
	}
	var paths []*Path
	if path != nil {
		paths = []*Path{path}
	}
	ctx.traceExitIfEnabled(it, paths)
}

// notifyEnterIterator calls ObserveEnterIterator on all registered observers.
func (ctx *Context) notifyEnterIterator(op Operation, key CanonicalKey) {
	for _, obs := range ctx.observers {
		obs.ObserveEnterIterator(op, key)
	}
}

// notifyCheckResult notifies all observers of a Check result (path may be nil for a miss).
// ObservePath is always called — even for nil — so observers can cache failures.
// ObserveReturnIterator is always called afterward.
func (ctx *Context) notifyCheckResult(key CanonicalKey, path *Path) {
	for _, obs := range ctx.observers {
		obs.ObservePath(OperationCheck, key, path)
		obs.ObserveReturnIterator(OperationCheck, key)
	}
}

// wrapPathSeqWithObservers wraps a PathSeq to notify all registered observers of paths and completion.
// Returns the original PathSeq unchanged when there are no observers.
func (ctx *Context) wrapPathSeqWithObservers(op Operation, key CanonicalKey, pathSeq PathSeq) PathSeq {
	if len(ctx.observers) == 0 {
		return pathSeq
	}
	return func(yield func(*Path, error) bool) {
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

// Check tests if the given resource is connected to subject in the underlying set of
// relationships. Returns the matching Path if found, or nil if not found.
func (ctx *Context) Check(it Iterator, resource Object, subject ObjectAndRelation) (*Path, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	ctx.MarkAsOperation(it, OperationCheck)

	var tracedIterator Iterator
	if ctx.shouldTrace() {
		tracedIterator = ctx.traceEnterIfEnabled(it, checkTraceString(resource, subject))
	}
	key := it.CanonicalKey()
	ctx.notifyEnterIterator(OperationCheck, key)

	path, err := ctx.Executor.Check(ctx, it, resource, subject)
	if err != nil {
		return nil, err
	}

	ctx.traceCheckResult(tracedIterator, path)
	ctx.notifyCheckResult(key, path)
	return path, nil
}

// MarkAsOperation records the top-level iterator and operation for this
// context, idempotently. The first caller "wins" and gets isTopLevel=true; all
// subsequent calls are no-ops and return false. This is used in two ways:
//
//  1. By Context.{Check,IterSubjects,IterResources,CheckManyX}, which call it
//     on entry so that any nested calls into the same context know they are
//     not the top-level — and so that the entry path can apply API-boundary
//     wrappers (FilterWildcardSubjects / DeduplicatePathSeq) only when truly
//     at the top.
//
//  2. By dispatch receivers, which pre-seal it so that the first inner
//     ctx.IterX call on the body is NOT mistaken for the top-level. Without
//     pre-sealing, the receiver's first inner call applies FilterWildcardSubjects
//     to a sub-iterator's results, silently dropping wildcards that should
//     propagate back to the sender's intersection/exclusion logic.
//
// It also sets TopLevelOperation so iterators that consult it (e.g.
// AliasIterator.shouldIncludeSelfEdge) see the right value.
func (ctx *Context) MarkAsOperation(it Iterator, op Operation) (isTopLevel bool) {
	ctx.topLevelOnce.Do(func() {
		ctx.topLevelIterator = it
		ctx.TopLevelOperation = op
		isTopLevel = true
	})
	return
}

// CheckManySubjects tests resource against each subject in subjects. Returns a
// parallel slice of paths (result[i] matches subjects[i], nil if no match).
func (ctx *Context) CheckManySubjects(it Iterator, resource Object, subjects []ObjectAndRelation) ([]*Path, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}
	if len(subjects) == 0 {
		return nil, nil
	}

	ctx.MarkAsOperation(it, OperationCheck)

	key := it.CanonicalKey()
	ctx.notifyEnterIterator(OperationCheck, key)

	paths, err := ctx.Executor.CheckManySubjects(ctx, it, resource, subjects)
	if err != nil {
		return nil, err
	}

	for _, p := range paths {
		ctx.notifyCheckResult(key, p)
	}
	return paths, nil
}

// CheckManyResources tests each resource in resources against subject. Returns a
// parallel slice of paths (result[i] matches resources[i], nil if no match).
func (ctx *Context) CheckManyResources(it Iterator, resources []Object, subject ObjectAndRelation) ([]*Path, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}
	if len(resources) == 0 {
		return nil, nil
	}

	ctx.MarkAsOperation(it, OperationCheck)

	key := it.CanonicalKey()
	ctx.notifyEnterIterator(OperationCheck, key)

	paths, err := ctx.Executor.CheckManyResources(ctx, it, resources, subject)
	if err != nil {
		return nil, err
	}

	for _, p := range paths {
		ctx.notifyCheckResult(key, p)
	}
	return paths, nil
}

// wrapPathSeqForTracing wraps a PathSeq to collect results for exit tracing if tracing is enabled,
// otherwise returns the original PathSeq unchanged.
func (ctx *Context) wrapPathSeqForTracing(it Iterator, pathSeq PathSeq) PathSeq {
	if !ctx.shouldTrace() || it == nil {
		return pathSeq
	}

	return func(yield func(*Path, error) bool) {
		var resultPaths []*Path
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

// IterSubjects returns a sequence of all the paths in this set that match the given resource.
// The filterSubjectType parameter filters results to only include subjects matching the
// specified ObjectType. If filterSubjectType.Type is empty, no filtering is applied.
// The first call sets topLevelIteratorHash and wraps the result with DeduplicatePathSeq;
// recursive sub-calls pass through unchanged.
func (ctx *Context) IterSubjects(it Iterator, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	isTopLevel := ctx.MarkAsOperation(it, OperationIterSubjects)

	var tracedIterator Iterator
	if ctx.shouldTrace() {
		tracedIterator = ctx.traceEnterIfEnabled(it, iterSubjectsTraceString(resource, filterSubjectType))
	}
	key := it.CanonicalKey()
	ctx.notifyEnterIterator(OperationIterSubjects, key)

	pathSeq, err := ctx.Executor.IterSubjects(ctx, it, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}

	if isTopLevel {
		// Wildcards propagate through the iterator tree for correct intersection/exclusion
		// semantics, but must be stripped before returning to the caller (service layer).
		pathSeq = FilterWildcardSubjects(DeduplicatePathSeq(pathSeq))
	}

	pathSeq = ctx.wrapPathSeqForTracing(tracedIterator, pathSeq)
	return ctx.wrapPathSeqWithObservers(OperationIterSubjects, key, pathSeq), nil
}

// IterResources returns a sequence of all the relations in this set that match the given subject.
// The filterResourceType parameter filters results to only include resources matching the
// specified ObjectType. If filterResourceType.Type is empty, no filtering is applied.
// The first call sets topLevelIteratorHash and wraps the result with DeduplicatePathSeq;
// recursive sub-calls pass through unchanged.
func (ctx *Context) IterResources(it Iterator, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	isTopLevel := ctx.MarkAsOperation(it, OperationIterResources)

	var tracedIterator Iterator
	if ctx.shouldTrace() {
		tracedIterator = ctx.traceEnterIfEnabled(it, iterResourcesTraceString(subject, filterResourceType))
	}
	key := it.CanonicalKey()
	ctx.notifyEnterIterator(OperationIterResources, key)

	pathSeq, err := ctx.Executor.IterResources(ctx, it, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	if isTopLevel {
		pathSeq = DeduplicatePathSeq(pathSeq)
	}

	pathSeq = ctx.wrapPathSeqForTracing(tracedIterator, pathSeq)
	return ctx.wrapPathSeqWithObservers(OperationIterResources, key, pathSeq), nil
}

// Executor chooses how to proceed given an iterator -- perhaps in parallel, perhaps by RPC, etc --
// and chooses how to process iteration from the subtree. The correctness logic for the results
// that are generated are up to each iterator, and each iterator may use statistics to choose the
// best, yet still correct, logical evaluation strategy.
type Executor interface {
	// Check tests if the given resource is connected to subject.
	// Returns the matching Path if found, or nil if not found.
	Check(ctx *Context, it Iterator, resource Object, subject ObjectAndRelation) (*Path, error)

	// CheckManySubjects tests resource against each subject in the slice.
	// Returns a parallel slice of paths: result[i] is the matching Path for
	// subjects[i], or nil if subjects[i] does not match.
	CheckManySubjects(ctx *Context, it Iterator, resource Object, subjects []ObjectAndRelation) ([]*Path, error)

	// CheckManyResources tests each resource in the slice against subject.
	// Returns a parallel slice of paths: result[i] is the matching Path for
	// resources[i], or nil if resources[i] does not match.
	CheckManyResources(ctx *Context, it Iterator, resources []Object, subject ObjectAndRelation) ([]*Path, error)

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
