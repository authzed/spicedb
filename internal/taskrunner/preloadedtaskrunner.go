package taskrunner

import (
	"context"
	"sync"
)

// PreloadedTaskRunner is a task runner that invokes a series of preloaded tasks,
// running until the tasks are completed, the context is canceled or an error is
// returned by one of the tasks (which cancels the context).
type PreloadedTaskRunner struct {
	// ctx holds the context given to the task runner and annotated with the cancel
	// function.
	ctx    context.Context
	cancel func()

	// sem is a chan of length `concurrencyLimit` used to ensure the task runner does
	// not exceed the concurrencyLimit with spawned goroutines.
	sem chan struct{}

	wg sync.WaitGroup

	lock  sync.Mutex
	err   error      // GUARDED_BY(lock)
	tasks []TaskFunc // GUARDED_BY(lock)
}

func NewPreloadedTaskRunner(ctx context.Context, concurrencyLimit uint16, initialCapacity int) *PreloadedTaskRunner {
	// Ensure a concurrency level of at least 1.
	if concurrencyLimit <= 0 {
		concurrencyLimit = 1
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	return &PreloadedTaskRunner{
		ctx:    ctxWithCancel,
		cancel: cancel,
		sem:    make(chan struct{}, concurrencyLimit),
		tasks:  make([]TaskFunc, 0, initialCapacity),
	}
}

// Add adds the given task function to be run.
func (tr *PreloadedTaskRunner) Add(f TaskFunc) {
	tr.tasks = append(tr.tasks, f)
	tr.wg.Add(1)
}

// Start starts running the tasks in the task runner. This does *not* wait for the tasks
// to complete, but rather returns immediately.
func (tr *PreloadedTaskRunner) Start() {
	for range tr.tasks {
		tr.spawnIfAvailable()
	}
}

// Wait calls .Wait() on the underlying waitgroup, allowing for e.g.
// cleanup of existing tasks to happen.
func (tr *PreloadedTaskRunner) Wait() error {
	tr.wg.Wait()

	tr.lock.Lock()
	defer tr.lock.Unlock()

	return tr.err
}

// StartAndWait starts running the tasks in the task runner and waits for them to complete.
func (tr *PreloadedTaskRunner) StartAndWait() error {
	tr.Start()
	return tr.Wait()
}

func (tr *PreloadedTaskRunner) spawnIfAvailable() {
	// To spawn a runner, write a struct{} to the sem channel. If the task runner
	// is already at the concurrency limit, then this chan write will fail,
	// and nothing will be spawned. This also checks if the context has already
	// been canceled, in which case nothing needs to be done.
	select {
	case tr.sem <- struct{}{}:
		go tr.runner()

	case <-tr.ctx.Done():
		// If the context was canceled, nothing more to do.
		tr.emptyForCancel()
		return

	default:
		return
	}
}

func (tr *PreloadedTaskRunner) runner() {
	for {
		select {
		case <-tr.ctx.Done():
			// If the context was canceled, nothing more to do.
			tr.emptyForCancel()
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
				tr.storeErrorAndCancel(err)
			}
			tr.wg.Done()
		}
	}
}

func (tr *PreloadedTaskRunner) selectTask() TaskFunc {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if len(tr.tasks) == 0 {
		return nil
	}

	task := tr.tasks[0]
	tr.tasks[0] = nil // to free the reference once the task completes.
	tr.tasks = tr.tasks[1:]
	return task
}

func (tr *PreloadedTaskRunner) storeErrorAndCancel(err error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if tr.err == nil {
		tr.err = err
		tr.cancel()
	}
}

func (tr *PreloadedTaskRunner) emptyForCancel() {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if tr.err == nil {
		tr.err = tr.ctx.Err()
	}

	for len(tr.tasks) != 0 {
		tr.tasks[0] = nil // to free the reference
		tr.tasks = tr.tasks[1:]
		tr.wg.Done()
	}
}
