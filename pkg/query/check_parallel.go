package query

import (
	"context"

	"github.com/authzed/spicedb/internal/taskrunner"
)

type setCheckKind string

const (
	setCheckUnion        setCheckKind = "union"
	setCheckIntersection setCheckKind = "intersection"
)

type setCheckResult struct {
	path *Path
	err  error
}

// checkSetParallel limits workers within this reducer. Nested reducers own
// independent runners, so a parent waiting for children cannot exhaust their
// capacity. The foreground returns on a decision; the request scope tracks
// cancellation cleanup separately from permission-check latency.
func checkSetParallel(
	ctx *Context,
	children []Iterator,
	resource Object,
	subject ObjectAndRelation,
	limit uint16,
	kind setCheckKind,
) (*Path, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, nil
	}

	if len(children) == 1 {
		return ctx.Check(children[0], resource, subject)
	}

	// Width one must not launch speculative work: an early grant or denial can
	// avoid every later child, including its datastore reads and potential errors.
	if limit <= 1 {
		var result *Path

		for _, child := range children {
			path, err := ctx.Check(child, resource, subject)
			if err != nil {
				return nil, err
			}

			result, err = mergeSetCheck(result, path, kind)
			if err != nil {
				return nil, err
			}

			if setCheckDecided(result, path, kind) {
				return result, nil
			}
		}

		return result, nil
	}

	workerContext, cancel := context.WithCancel(ctx.Context)
	defer cancel()

	// Every child can publish exactly once, even after the foreground exits. No
	// abandoned reducer can leave a worker blocked while publishing a late result.
	results := make(chan setCheckResult, len(children))
	runner := taskrunner.NewPreloadedTaskRunner(workerContext, limit, len(children))

	for _, child := range children {
		// Copy mutable request state before launching any sibling. Only immutable
		// handles and the synchronized completion-observer state cross this boundary.
		branch := ctx.forkForCheck(workerContext)
		runner.Add(func(taskContext context.Context) error {
			branch.Context = taskContext
			path, err := branch.Check(child, resource, subject)
			results <- setCheckResult{path: path, err: err}

			// Deliver errors through the same channel as decisions, matching native's
			// first-observed reducer outcome rather than canceling ahead of publication.
			return nil
		})
	}

	runner.Start()
	var result *Path

	for range children {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case completed := <-results:
			if completed.err != nil {
				return nil, completed.err
			}

			merged, err := mergeSetCheck(result, completed.path, kind)
			if err != nil {
				return nil, err
			}

			result = merged

			if setCheckDecided(result, completed.path, kind) {
				return result, nil
			}
		}
	}

	return result, nil
}

func mergeSetCheck(result, path *Path, kind setCheckKind) (*Path, error) {
	if kind == setCheckIntersection {
		if path == nil {
			return nil, nil
		}

		return result.MergeAnd(path)
	}

	if path == nil {
		return result, nil
	}

	return result.MergeOr(path)
}

func setCheckDecided(result, path *Path, kind setCheckKind) bool {
	if kind == setCheckIntersection {
		return path == nil
	}

	return result != nil && result.Caveat == nil
}
