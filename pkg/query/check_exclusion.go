package query

import (
	"context"

	"github.com/authzed/spicedb/pkg/genutil/execscope"
)

// checkExclusionOperands overlaps the two operands while retaining native's
// base-first error precedence. An empty base cancels the excluded task without
// interpreting its result. Each operand owns its mutable request state, and the
// request scope tracks any work still unwinding after the foreground returns.
func checkExclusionOperands[T any](ctx *Context, main, excluded func(*Context) (T, error), empty func(T) bool) (T, T, error) {
	workerContext, cancel := context.WithCancel(ctx.Context)
	defer cancel()
	baseContext := ctx.forkForCheck(workerContext)
	excludedContext := ctx.forkForCheck(workerContext)

	type outcome struct {
		value T
		err   error
	}

	completed := make(chan outcome, 1)
	finish := execscope.Begin(workerContext)
	go func() {
		defer finish()
		value, err := excluded(excludedContext)
		completed <- outcome{value: value, err: err}
	}()

	base, err := main(baseContext)
	var zero T

	if err != nil || empty(base) {
		return base, zero, err
	}

	select {
	case <-ctx.Done():
		return zero, zero, ctx.Err()
	case result := <-completed:
		return base, result.value, result.err
	}
}

// checkExclusionBatch retains all candidates in both operands: the first base
// grant can be excluded, so AllowSingleResult cannot be propagated to either
// operand. Native also checks the full vector in each concurrent operand.
func (e *ExclusionIterator) checkExclusionBatch(ctx *Context, batch checkBatch) ([]*Path, error) {
	limit, err := ctx.setCheckConcurrency(e.checkConcurrency, e.concurrencyHint)
	if err != nil {
		return nil, err
	}

	childBatch := batch
	childBatch.mode = AllCheckResults
	empty := func(paths []*Path) bool {
		for _, path := range paths {
			if path != nil {
				return false
			}
		}

		return true
	}

	var main, excluded []*Path

	switch {
	case limit > 1:
		childBatch = childBatch.cloneTargets()
		main, excluded, err = checkExclusionOperands(
			ctx,
			func(branch *Context) ([]*Path, error) { return childBatch.child(branch, e.mainSet) },
			func(branch *Context) ([]*Path, error) { return childBatch.child(branch, e.excluded) },
			empty,
		)
	case ctx.checkExecution.BaseFirstExclusion:
		main, err = childBatch.child(ctx, e.mainSet)
		if err == nil && !empty(main) {
			excluded, err = childBatch.child(ctx, e.excluded)
		}
	default:
		excluded, err = childBatch.child(ctx, e.excluded)
		if err == nil {
			main, err = childBatch.child(ctx, e.mainSet)
		}
	}

	if err != nil {
		return nil, err
	}

	for index, path := range main {
		if path == nil || excluded[index] == nil {
			continue
		}

		result, keep := combineExclusionCaveats(path, excluded[index])
		main[index] = nil

		if keep {
			main[index] = result
		}
	}

	return main, nil
}
