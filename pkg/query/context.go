package query

import (
	"context"
	"fmt"
	"strings"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// TraceLogger is used for debugging iterator execution
type TraceLogger struct {
	traces []string
	depth  int
	stack  []Iterator // Stack of iterator pointers for proper indentation context
}

// NewTraceLogger creates a new trace logger
func NewTraceLogger() *TraceLogger {
	return &TraceLogger{
		traces: make([]string, 0),
		depth:  0,
		stack:  make([]Iterator, 0),
	}
}

// EnterIterator logs entering an iterator and pushes it onto the stack
func (t *TraceLogger) EnterIterator(it Iterator, resources []Object, subject ObjectAndRelation) {
	// Get iterator name from Explain
	explain := it.Explain()
	iteratorType := explain.Name
	if iteratorType == "" {
		iteratorType = explain.Info
	}

	indent := strings.Repeat("  ", t.depth)
	resourceStrs := make([]string, len(resources))
	for i, r := range resources {
		resourceStrs[i] = fmt.Sprintf("%s:%s", r.ObjectType, r.ObjectID)
	}
	t.traces = append(t.traces, fmt.Sprintf("%s-> %s: check(%s, %s:%s)",
		indent, iteratorType, strings.Join(resourceStrs, ","), subject.ObjectType, subject.ObjectID))
	t.depth++
	t.stack = append(t.stack, it) // Push iterator pointer onto stack
}

// ExitIterator logs exiting an iterator and pops it from the stack
func (t *TraceLogger) ExitIterator(it Iterator, paths []Path) {
	// Pop from stack to maintain stack consistency
	if len(t.stack) > 0 {
		t.stack = t.stack[:len(t.stack)-1]
	}
	t.depth--

	// Get iterator name from Explain
	explain := it.Explain()
	iteratorType := explain.Name
	if iteratorType == "" {
		iteratorType = explain.Info
	}

	indent := strings.Repeat("  ", t.depth)
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
	t.traces = append(t.traces, fmt.Sprintf("%s<- %s: returned %d paths: [%s]",
		indent, iteratorType, len(paths), strings.Join(pathStrs, ", ")))
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

	// Get iterator name from Explain
	explain := it.Explain()
	iteratorName := explain.Name
	if iteratorName == "" {
		iteratorName = explain.Info
	}

	indent := strings.Repeat("  ", indentLevel)
	message := fmt.Sprintf(step, data...)
	t.traces = append(t.traces, fmt.Sprintf("%s   %s: %s", indent, iteratorName, message))
}

// DumpTrace returns all traces as a string
func (t *TraceLogger) DumpTrace() string {
	return strings.Join(t.traces, "\n")
}

// Context represents a single execution of a query.
// It is both a standard context.Context and all the query-time specific handles needed to evaluate a query, such as which datastore it is running against.
//
// Context is the concrete type that contains the overall handles, and uses the executor as a strategy for continuing execution.
type Context struct {
	context.Context
	Executor          Executor
	Reader            datastore.Reader // Datastore reader for this query at a specific revision
	CaveatContext     map[string]any
	CaveatRunner      *caveats.CaveatRunner
	TraceLogger       *TraceLogger // For debugging iterator execution
	MaxRecursionDepth int          // Maximum depth for recursive iterators (0 = use default of 10)
}

func (ctx *Context) TraceStep(it Iterator, step string, data ...any) {
	if ctx.TraceLogger != nil {
		ctx.TraceLogger.LogStep(it, step, data...)
	}
}

func (ctx *Context) TraceEnter(it Iterator, resources []Object, subject ObjectAndRelation) {
	if ctx.TraceLogger != nil {
		ctx.TraceLogger.EnterIterator(it, resources, subject)
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

func (ctx *Context) traceEnterIfEnabled(it Iterator, resources []Object, subject ObjectAndRelation) Iterator {
	if !ctx.shouldTrace() {
		return nil
	}
	ctx.TraceEnter(it, resources, subject)
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

// Check tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
// any of the `resources` are connected to `subject`.
// Returns the sequence of matching paths, if they exist, at most `len(resources)`.
func (ctx *Context) Check(it Iterator, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	tracedIterator := ctx.traceEnterIfEnabled(it, resources, subject)

	pathSeq, err := ctx.Executor.Check(ctx, it, resources, subject)
	if err != nil {
		return nil, err
	}

	return ctx.wrapPathSeqForTracing(tracedIterator, pathSeq), nil
}

// IterSubjects returns a sequence of all the paths in this set that match the given resource.
func (ctx *Context) IterSubjects(it Iterator, resource Object) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	tracedIterator := ctx.traceEnterIfEnabled(it, []Object{resource}, ObjectAndRelation{})

	pathSeq, err := ctx.Executor.IterSubjects(ctx, it, resource)
	if err != nil {
		return nil, err
	}

	return ctx.wrapPathSeqForTracing(tracedIterator, pathSeq), nil
}

// IterResources returns a sequence of all the relations in this set that match the given subject.
func (ctx *Context) IterResources(it Iterator, subject ObjectAndRelation) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}

	tracedIterator := ctx.traceEnterIfEnabled(it, []Object{}, subject)

	pathSeq, err := ctx.Executor.IterResources(ctx, it, subject)
	if err != nil {
		return nil, err
	}

	return ctx.wrapPathSeqForTracing(tracedIterator, pathSeq), nil
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
	IterSubjects(ctx *Context, it Iterator, resource Object) (PathSeq, error)

	// IterResources returns a sequence of all the relations in this set that match the given subject.
	IterResources(ctx *Context, it Iterator, subject ObjectAndRelation) (PathSeq, error)
}
