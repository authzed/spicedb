package query

import (
	"context"

	"github.com/authzed/spicedb/internal/taskrunner"
)

type batchCheckResult struct {
	paths []*Path
	err   error
}

// checkSetBatchParallel schedules complete named branches while preserving the
// target vector inside each branch. Branch contexts isolate recursion and caveat
// state. As with scalar/native reducers, the first observed decisive outcome
// ends foreground work; the request execution scope tracks canceled workers.
func checkSetBatchParallel(ctx *Context, batch checkBatch, children []Iterator, limit uint16, intersection bool) ([]*Path, error) {
	workerContext, cancel := context.WithCancel(ctx.Context)
	defer cancel()

	results := make(chan batchCheckResult, len(children))
	runner := taskrunner.NewPreloadedTaskRunner(workerContext, limit, len(children))
	childBatch := batch.cloneTargets()

	if intersection {
		// A grant for one target in one child is insufficient for an intersection.
		// Each child must retain every target that another child might also grant.
		childBatch.mode = AllCheckResults
	}

	for _, child := range children {
		branch := ctx.forkForCheck(workerContext)
		runner.Add(func(taskContext context.Context) error {
			branch.Context = taskContext
			paths, err := childBatch.child(branch, child)
			results <- batchCheckResult{paths: paths, err: err}
			return nil
		})
	}

	runner.Start()
	out := make([]*Path, batch.size())
	denied := make([]bool, len(out))
	decided := 0
	kind := setCheckUnion

	if intersection {
		kind = setCheckIntersection
	}

	for range children {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-results:
			if result.err != nil {
				return nil, result.err
			}

			for i, path := range result.paths {
				if intersection {
					if denied[i] {
						continue
					}

					if path == nil {
						denied[i] = true
						decided++
						out[i] = nil
						continue
					}
				} else if out[i] != nil && out[i].Caveat == nil {
					continue
				}

				merged, err := mergeSetCheck(out[i], path, kind)
				if err != nil {
					return nil, err
				}

				out[i] = merged

				if !intersection && merged != nil && merged.Caveat == nil {
					decided++
				}
			}

			if decided == len(out) || (!intersection && batch.mode == AllowSingleResult && decided > 0) {
				return out, nil
			}
		}
	}

	return out, nil
}
