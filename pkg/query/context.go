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
}

// NewTraceLogger creates a new trace logger
func NewTraceLogger() *TraceLogger {
	return &TraceLogger{
		traces: make([]string, 0),
		depth:  0,
	}
}

// EnterIterator logs entering an iterator
func (t *TraceLogger) EnterIterator(iteratorType string, resources []Object, subject ObjectAndRelation) {
	indent := strings.Repeat("  ", t.depth)
	resourceStrs := make([]string, len(resources))
	for i, r := range resources {
		resourceStrs[i] = fmt.Sprintf("%s:%s", r.ObjectType, r.ObjectID)
	}
	t.traces = append(t.traces, fmt.Sprintf("%s-> %s: check(%s, %s:%s)",
		indent, iteratorType, strings.Join(resourceStrs, ","), subject.ObjectType, subject.ObjectID))
	t.depth++
}

// ExitIterator logs exiting an iterator with results
func (t *TraceLogger) ExitIterator(iteratorType string, paths []*Path) {
	t.depth--
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

// LogStep logs an intermediate step within an iterator
func (t *TraceLogger) LogStep(iteratorType, step string, data ...any) {
	indent := strings.Repeat("  ", t.depth)
	message := fmt.Sprintf(step, data...)
	t.traces = append(t.traces, fmt.Sprintf("%s   %s: %s", indent, iteratorType, message))
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
	Executor      Executor
	Datastore     datastore.ReadOnlyDatastore
	Revision      datastore.Revision
	CaveatContext map[string]any
	CaveatRunner  *caveats.CaveatRunner
	TraceLogger   *TraceLogger // For debugging iterator execution
}

// Trace helper methods for easier tracing
func (ctx *Context) TraceEnterIterator(iteratorType string, resources []Object, subject ObjectAndRelation) {
	if ctx.TraceLogger != nil {
		ctx.TraceLogger.EnterIterator(iteratorType, resources, subject)
	}
}

func (ctx *Context) TraceExitIterator(iteratorType string, paths []*Path) {
	if ctx.TraceLogger != nil {
		ctx.TraceLogger.ExitIterator(iteratorType, paths)
	}
}

func (ctx *Context) TraceStep(iteratorType, step string, data ...any) {
	if ctx.TraceLogger != nil {
		ctx.TraceLogger.LogStep(iteratorType, step, data...)
	}
}

// Check tests if, for the underlying set of relationships (which may be a full expression or a basic lookup, depending on the iterator)
// any of the `resources` are connected to `subject`.
// Returns the sequence of matching paths, if they exist, at most `len(resources)`.
func (ctx *Context) Check(it Iterator, resources []Object, subject ObjectAndRelation) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}
	return ctx.Executor.Check(ctx, it, resources, subject)
}

// IterSubjects returns a sequence of all the paths in this set that match the given resource.
func (ctx *Context) IterSubjects(it Iterator, resource Object) (PathSeq, error) {
	if ctx.Executor == nil {
		return nil, spiceerrors.MustBugf("no executor has been set")
	}
	return ctx.Executor.IterSubjects(ctx, it, resource)
}

// IterResources returns a sequence of all the relations in this set that match the given subject.
func (ctx *Context) IterResources(it Iterator, subject ObjectAndRelation) (PathSeq, error) {
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
	Check(ctx *Context, it Iterator, resources []Object, subject ObjectAndRelation) (PathSeq, error)

	// IterSubjects returns a sequence of all the relations in this set that match the given resource.
	IterSubjects(ctx *Context, it Iterator, resource Object) (PathSeq, error)

	// IterResources returns a sequence of all the relations in this set that match the given subject.
	IterResources(ctx *Context, it Iterator, subject ObjectAndRelation) (PathSeq, error)
}
