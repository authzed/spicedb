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
