package query

import (
	"context"
	"fmt"
	"io"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TraceLogger is used for debugging iterator execution
type TraceLogger struct {
	traces []string
	depth  int
	stack  []Iterator // Stack of iterator pointers for proper indentation context
	writer io.Writer  // Optional writer to output traces in real-time
}

// NewTraceLogger creates a new trace logger
func NewTraceLogger() *TraceLogger {
	return &TraceLogger{
		traces: make([]string, 0),
		depth:  0,
		stack:  make([]Iterator, 0),
		writer: nil,
	}
}

// NewTraceLoggerWithWriter creates a new trace logger with an optional writer
// for real-time trace output
func NewTraceLoggerWithWriter(w io.Writer) *TraceLogger {
	return &TraceLogger{
		traces: make([]string, 0),
		depth:  0,
		stack:  make([]Iterator, 0),
		writer: w,
	}
}

// appendTrace appends a trace line to the traces slice and optionally writes it
// to the writer if one is configured
func (t *TraceLogger) appendTrace(line string) {
	t.traces = append(t.traces, line)
	if t.writer != nil {
		// Write the line with a newline
		fmt.Fprintln(t.writer, line)
	}
}

// iteratorIDPrefix generates a formatted prefix string for an iterator with its ID
func iteratorIDPrefix(it Iterator) string {
	explain := it.Explain()
	iteratorName := explain.Name
	if iteratorName == "" {
		iteratorName = explain.Info
	}

	id := it.ID()
	if id == "" {
		return " " + iteratorName
	}

	if len(id) >= 6 {
		return fmt.Sprintf(" %s[%s]", iteratorName, id[:6])
	}
	return fmt.Sprintf(" %s[%s]", iteratorName, id)
}

// EnterIterator logs entering an iterator and pushes it onto the stack
func (t *TraceLogger) EnterIterator(it Iterator, traceString string) {
	indent := strings.Repeat("  ", t.depth)
	idPrefix := iteratorIDPrefix(it)

	t.appendTrace(fmt.Sprintf("%s-> %s: %s",
		indent,
		idPrefix,
		traceString,
	))
	t.depth++
	t.stack = append(t.stack, it) // Push iterator pointer onto stack
}

func subjectRelationTraceString(subject ObjectAndRelation) string {
	var subjectRelation string
	if subject.Relation != "" {
		subjectRelation = "#" + subject.Relation
	}
	return subjectRelation
}

func checkTraceString(resources []Object, subject ObjectAndRelation) string {
	resourceStrs := make([]string, len(resources))
	for i, r := range resources {
		resourceStrs[i] = fmt.Sprintf("%s:%s", r.ObjectType, r.ObjectID)
	}
	return fmt.Sprintf("check(%s, %s:%s%s)", strings.Join(resourceStrs, ", "), subject.ObjectType, subject.ObjectID, subjectRelationTraceString(subject))
}

func iterResourcesTraceString(subject ObjectAndRelation, filter ObjectType) string {
	filterStr := ""
	if filter.Type != "" {
		filterStr = ", filter=" + filter.Type
		if filter.Subrelation != "" {
			filterStr += "#" + filter.Subrelation
		}
	}
	return fmt.Sprintf("iterResources(%s:%s%s%s)", subject.ObjectType, subject.ObjectID, subjectRelationTraceString(subject), filterStr)
}

func iterSubjectsTraceString(resource Object, filter ObjectType) string {
	filterStr := ""
	if filter.Type != "" {
		filterStr = ", filter=" + filter.Type
		if filter.Subrelation != "" {
			filterStr += "#" + filter.Subrelation
		}
	}
	return fmt.Sprintf("iterSubjects(%s:%s%s)", resource.ObjectType, resource.ObjectID, filterStr)
}

// ExitIterator logs exiting an iterator and pops it from the stack
func (t *TraceLogger) ExitIterator(it Iterator, paths []Path) {
	// Pop from stack to maintain stack consistency
	if len(t.stack) > 0 {
		t.stack = t.stack[:len(t.stack)-1]
	}
	t.depth--

	indent := strings.Repeat("  ", t.depth)
	idPrefix := iteratorIDPrefix(it)

	pathStrs := make([]string, len(paths))
	for i, p := range paths {
		caveatInfo := ""
		if p.Caveat != nil {
			// Extract caveat name from CaveatExpression
			if p.Caveat.GetCaveat() != nil {
				caveatInfo = fmt.Sprintf("[%s]", p.Caveat.GetCaveat().CaveatName)
			} else {
				caveatInfo = "[complex_caveat]"
			}
		}
		pathStrs[i] = fmt.Sprintf("%s:%s#%s@%s:%s%s",
			p.Resource.ObjectType, p.Resource.ObjectID, p.Relation,
			p.Subject.ObjectType, p.Subject.ObjectID, caveatInfo)
	}
	t.appendTrace(fmt.Sprintf("%s<- %s: returned %d paths: [%s]",
		indent, idPrefix, len(paths), strings.Join(pathStrs, ", ")))
}

// LogStep logs an intermediate step within an iterator, using the iterator pointer to find the correct indentation level
func (t *TraceLogger) LogStep(it Iterator, step string, data ...any) {
	// Find the iterator's position in the stack by comparing pointers
	indentLevel := 0
	found := false

	for i, stackIt := range t.stack {
		if stackIt == it {
			indentLevel = i + 1 // +1 because we want to be indented inside the iterator
			found = true
			break
		}
	}

	if !found {
		// Iterator not found in stack, use current depth
		indentLevel = t.depth
	}

	indent := strings.Repeat("  ", indentLevel)
	idPrefix := iteratorIDPrefix(it)
	message := fmt.Sprintf(step, data...)
	t.appendTrace(fmt.Sprintf("%s   %s: %s", indent, idPrefix, message))
}

// DumpTrace returns all traces as a string
func (t *TraceLogger) DumpTrace() string {
	return strings.Join(t.traces, "\n")
}

// AnalyzeStats collects the number of operations performed for each iterator as a query takes place.
type AnalyzeStats struct {
	CheckCalls           int
	IterSubjectsCalls    int
	IterResourcesCalls   int
	CheckResults         int
	IterSubjectsResults  int
	IterResourcesResults int
	CheckTime            time.Duration
	IterSubjectsTime     time.Duration
	IterResourcesTime    time.Duration
}

// AnalyzeCollector is a thread-safe wrapper around the analysis stats map
type AnalyzeCollector struct {
	mu    sync.Mutex
	stats map[string]AnalyzeStats // GUARDED_BY(mu)
}

// NewAnalyzeCollector creates a new thread-safe analyze collector
func NewAnalyzeCollector() *AnalyzeCollector {
	return &AnalyzeCollector{
		stats: make(map[string]AnalyzeStats),
	}
}

// IncrementCall increments the call counter for a given iterator and operation type
func (ac *AnalyzeCollector) IncrementCall(iterID, opType string) {
	if ac == nil {
		return
	}
	ac.mu.Lock()
	defer ac.mu.Unlock()

	stats := ac.stats[iterID]
	switch opType {
	case "Check":
		stats.CheckCalls++
	case "IterSubjects":
		stats.IterSubjectsCalls++
	case "IterResources":
		stats.IterResourcesCalls++
	}
	ac.stats[iterID] = stats
}

// RecordResults updates the result count and timing for an iterator
func (ac *AnalyzeCollector) RecordResults(iterID, opType string, count int, elapsed time.Duration) {
	if ac == nil {
		return
	}
	ac.mu.Lock()
	defer ac.mu.Unlock()

	stats := ac.stats[iterID]
	switch opType {
	case "Check":
		stats.CheckResults += count
		stats.CheckTime += elapsed
	case "IterSubjects":
		stats.IterSubjectsResults += count
		stats.IterSubjectsTime += elapsed
	case "IterResources":
		stats.IterResourcesResults += count
		stats.IterResourcesTime += elapsed
	}
	ac.stats[iterID] = stats
}

// GetStats returns a copy of all stats for reading
func (ac *AnalyzeCollector) GetStats() map[string]AnalyzeStats {
	if ac == nil {
		return nil
	}
	ac.mu.Lock()
	defer ac.mu.Unlock()

	result := make(map[string]AnalyzeStats, len(ac.stats))
	maps.Copy(result, ac.stats)
	return result
}

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
	TraceLogger       *TraceLogger      // For debugging iterator execution
	Analyze           *AnalyzeCollector // Thread-safe collector for query analysis stats
	MaxRecursionDepth int               // Maximum depth for recursive iterators (0 = use default of 10)

	// Pagination options for IterSubjects and IterResources
	PaginationCursors map[string]*tuple.Relationship // Cursors for pagination, keyed by iterator ID
	PaginationLimit   *uint64                        // Limit for pagination (max number of results to return)
	PaginationSort    options.SortOrder              // Sort order for pagination

	// recursiveFrontierCollectors holds frontier collections for BFS IterSubjects.
	// Key: RecursiveIterator.ID()
	// Value: collected Objects for the next frontier
	// A non-nil entry for an ID enables collection mode for that RecursiveIterator.
	recursiveFrontierCollectors map[string][]Object
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

// WithAnalyze sets the analysis collector for the context.
func WithAnalyze(analyze *AnalyzeCollector) ContextOption {
	return func(ctx *Context) { ctx.Analyze = analyze }
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

// Helper methods for conditional tracing with iterator name extraction
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

// wrapPathSeqForAnalysis wraps a PathSeq to count results and track timing if analysis is enabled
func (ctx *Context) wrapPathSeqForAnalysis(it Iterator, pathSeq PathSeq, opType string) PathSeq {
	if ctx.Analyze == nil || it == nil {
		return pathSeq
	}

	iterID := it.ID()

	// Increment call counter based on operation type (thread-safe)
	ctx.Analyze.IncrementCall(iterID, opType)

	// Wrap the PathSeq to count results and track timing
	return func(yield func(Path, error) bool) {
		startTime := time.Now()
		resultCount := 0

		defer func() {
			elapsed := time.Since(startTime)
			ctx.Analyze.RecordResults(iterID, opType, resultCount, elapsed)
		}()

		for path, err := range pathSeq {
			if err == nil {
				resultCount++
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

	pathSeq, err := ctx.Executor.Check(ctx, it, resources, subject)
	if err != nil {
		return nil, err
	}

	pathSeq = ctx.wrapPathSeqForTracing(tracedIterator, pathSeq)
	pathSeq = ctx.wrapPathSeqForAnalysis(it, pathSeq, "Check")
	return pathSeq, nil
}

// IterSubjects returns a sequence of all the paths in this set that match the given resource.
// The filterSubjectType parameter filters results to only include subjects matching the
// specified ObjectType. If filterSubjectType.Type is empty, no filtering is applied.
func (ctx *Context) IterSubjects(it Iterator, resource Object, filterSubjectType ObjectType) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	tracedIterator := ctx.traceEnterIfEnabled(it, iterSubjectsTraceString(resource, filterSubjectType))

	pathSeq, err := ctx.Executor.IterSubjects(ctx, it, resource, filterSubjectType)
	if err != nil {
		return nil, err
	}

	pathSeq = ctx.wrapPathSeqForTracing(tracedIterator, pathSeq)
	pathSeq = ctx.wrapPathSeqForAnalysis(it, pathSeq, "IterSubjects")
	return pathSeq, nil
}

// IterResources returns a sequence of all the relations in this set that match the given subject.
// The filterResourceType parameter filters results to only include resources matching the
// specified ObjectType. If filterResourceType.Type is empty, no filtering is applied.
func (ctx *Context) IterResources(it Iterator, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	tracedIterator := ctx.traceEnterIfEnabled(it, iterResourcesTraceString(subject, filterResourceType))

	pathSeq, err := ctx.Executor.IterResources(ctx, it, subject, filterResourceType)
	if err != nil {
		return nil, err
	}

	pathSeq = ctx.wrapPathSeqForTracing(tracedIterator, pathSeq)
	pathSeq = ctx.wrapPathSeqForAnalysis(it, pathSeq, "IterResources")
	return pathSeq, nil
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
func (ctx *Context) EnableFrontierCollection(iteratorID string) {
	if ctx.recursiveFrontierCollectors == nil {
		ctx.recursiveFrontierCollectors = make(map[string][]Object)
	}
	ctx.recursiveFrontierCollectors[iteratorID] = []Object{}
}

// CollectFrontierObject appends an object to the frontier collection.
// Only appends if collection mode is enabled (non-nil entry exists).
func (ctx *Context) CollectFrontierObject(iteratorID string, obj Object) {
	if ctx.recursiveFrontierCollectors == nil {
		return
	}
	if collection, exists := ctx.recursiveFrontierCollectors[iteratorID]; exists {
		ctx.recursiveFrontierCollectors[iteratorID] = append(collection, obj)
	}
}

// ExtractFrontierCollection retrieves and removes the collected frontier.
func (ctx *Context) ExtractFrontierCollection(iteratorID string) []Object {
	if ctx.recursiveFrontierCollectors == nil {
		return nil
	}
	collection := ctx.recursiveFrontierCollectors[iteratorID]
	delete(ctx.recursiveFrontierCollectors, iteratorID)
	return collection
}

// IsCollectingFrontier checks if collection mode is enabled (non-nil entry exists).
func (ctx *Context) IsCollectingFrontier(iteratorID string) bool {
	if ctx.recursiveFrontierCollectors == nil {
		return false
	}
	_, exists := ctx.recursiveFrontierCollectors[iteratorID]
	return exists
}
