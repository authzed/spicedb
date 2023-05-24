package graph

import (
	"context"
	"sync"
)

// preloadedTaskRunner is a task runner that invokes a series of preloaded tasks,
// running until the tasks are completed, the context is canceled or an error is
// returned by one of the tasks (which cancels the context).
type preloadedTaskRunner struct {
	// ctx holds the context given to the task runner and annotated with the cancel
	// function.
	ctx    context.Context
	cancel func()

	// sem is a chan of length `concurrencyLimit` used to ensure the task runner does
	// not exceed the concurrencyLimit with spawned goroutines.
	sem chan token

	lock  sync.Mutex
	tasks []TaskFunc
}

func newPreloadedTaskRunner(ctx context.Context, concurrencyLimit uint16, initialCapacity int) *preloadedTaskRunner {
	// Ensure a concurrency level of at least 1.
	if concurrencyLimit <= 0 {
		concurrencyLimit = 1
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	return &preloadedTaskRunner{
		ctx:    ctxWithCancel,
		cancel: cancel,
		sem:    make(chan token, concurrencyLimit),
		tasks:  make([]TaskFunc, 0, initialCapacity),
	}
}

// add adds the given task function to be run.
func (tr *preloadedTaskRunner) add(f TaskFunc) {
	tr.tasks = append(tr.tasks, f)
}

// start starts running the tasks in the task runner. This does *not* wait for the tasks
// to complete, but rather returns immediately.
func (tr *preloadedTaskRunner) start() {
	for range tr.tasks {
		tr.spawnIfAvailable()
	}
}

func (tr *preloadedTaskRunner) spawnIfAvailable() {
	// To spawn a runner, write a token to the sem channel. If the task runner
	// is already at the concurrency limit, then this chan write will fail,
	// and nothing will be spawned. This also checks if the context has already
	// been canceled, in which case nothing needs to be done.
	select {
	case tr.sem <- token{}:
		go tr.runner()

	case <-tr.ctx.Done():
		return

	default:
		return
	}
}

func (tr *preloadedTaskRunner) runner() {
	for {
		select {
		case <-tr.ctx.Done():
			// If the context was canceled, nothing more to do.
			return

		default:
			// Select a task from the list, if any.
			task := tr.selectTask()
			if task == nil {
				return
			}

			// Run the task. If an error occurs, store it and cancel any further tasks.
			err := task(tr.ctx)
			if err != nil {
				tr.cancel()
			}
		}
	}
}

func (tr *preloadedTaskRunner) selectTask() TaskFunc {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if len(tr.tasks) == 0 {
		return nil
	}

	var task TaskFunc
	task, tr.tasks = tr.tasks[0], tr.tasks[1:]
	return task
}
