package query

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
)

// executionObservationState belongs to the request, rather than any one branch.
// Descriptors are initialized once and then immutable; callbacks are serialized
// independently of execution so observers never serialize datastore work.
type executionObservationState struct {
	sequence    atomic.Uint64
	descriptors sync.Map
	initialize  sync.Once
	callbacks   sync.Mutex
	// Observer invocation state is GUARDED_BY(callbacks); the list is immutable during execution.
	observers []ExecutionObserver
}

// forkForCheck inherits the already-marked request while isolating state that
// recursive traversal can change. Iterator, schema and executor handles are
// immutable or concurrency-safe; branch-local maps contain immutable references.
// The parallel reducer rejects active frontier collectors before calling this.
func (ctx *Context) forkForCheck(stdContext context.Context) *Context {
	branch := &Context{
		Context:            stdContext,
		Executor:           ctx.Executor,
		Reader:             ctx.Reader,
		CaveatContext:      ctx.CaveatContext,
		MaxRecursionDepth:  ctx.MaxRecursionDepth,
		TargetSubjectType:  ctx.TargetSubjectType,
		BatchedArrows:      ctx.BatchedArrows,
		PaginationCursors:  maps.Clone(ctx.PaginationCursors),
		checkExecution:     ctx.checkExecution,
		checkConcurrency:   ctx.checkConcurrency,
		hintLedger:         ctx.hintLedger,
		targetedRecursions: maps.Clone(ctx.targetedRecursions),
		targetedDepth:      ctx.targetedDepth,
		executionState:     ctx.executionState,
	}

	if ctx.CaveatRunner != nil {
		branch.CaveatRunner = ctx.CaveatRunner.Fork()
	}

	if ctx.PaginationLimit != nil {
		limit := *ctx.PaginationLimit
		branch.PaginationLimit = &limit
	}

	// Seal a fresh sync.Once rather than copying the parent's live guard. Nested
	// enumeration must retain the request's operation and API boundary semantics.
	branch.MarkAsOperation(ctx.topLevelIterator, ctx.TopLevelOperation)
	return branch
}
