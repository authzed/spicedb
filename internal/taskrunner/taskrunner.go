package taskrunner

import (
	"context"
	"sync"
)

// TaskRunner is a helper which runs a series of scheduled tasks against a defined
// limit of goroutines.
type TaskRunner struct {
	// ctx holds the context given to the task runner and annotated with the cancel
	// function.
	ctx    context.Context
	cancel func()

	// sem is a chan of length `concurrencyLimit` used to ensure the task runner does
	// not exceed the concurrencyLimit with spawned goroutines.
	sem chan struct{}

	// err holds the error returned by any task, if any. If the context is canceled,
	// this err will hold the cancelation error.
	err error

	wg    sync.WaitGroup
	lock  sync.Mutex
	tasks []TaskFunc
}

// TaskFunc defines functions representing tasks.
type TaskFunc func(ctx context.Context) error

// NewTaskRunner creates a new task runner with the given starting context and
// concurrency limit. The TaskRunner will schedule no more goroutines that the
// specified concurrencyLimit. If the given context is canceled, then all tasks
// started after that point will also be canceled and the error returned. If
// a task returns an error, the context provided to all tasks is also canceled.
func NewTaskRunner(ctx context.Context, concurrencyLimit uint16) *TaskRunner {
	if concurrencyLimit < 1 {
		concurrencyLimit = 1
	}

	ctxWithCancel, cancel := context.WithCancel(ctx)
	return &TaskRunner{
		ctx:    ctxWithCancel,
		cancel: cancel,
		sem:    make(chan struct{}, concurrencyLimit),
		tasks:  make([]TaskFunc, 0),
	}
}

// Schedule schedules a task to be run. This is safe to call from within another
// task handler function and immediately returns.
func (tr *TaskRunner) Schedule(f TaskFunc) {
	if tr.addTask(f) {
		tr.spawnIfAvailable()
	}
}

func (tr *TaskRunner) spawnIfAvailable() {
	// To spawn a runner, write a struct{} to the sem channel. If the task runner
	// is already at the concurrency limit, then this chan write will fail,
	// and nothing will be spawned. This also checks if the context has already
	// been canceled, in which case nothing needs to be done.
	select {
	case tr.sem <- struct{}{}:
		go tr.runner()

	case <-tr.ctx.Done():
		return

	default:
		return
	}
}

func (tr *TaskRunner) runner() {
	for {
		select {
		case <-tr.ctx.Done():
			// If the context was canceled, mark all the remaining tasks as "Done".
			tr.emptyForCancel()
			return

		default:
			// Select a task from the list, if any.
			task := tr.selectTask()
			if task == nil {
				// If there are no further tasks, then "return" the struct{} by reading
				// it from the channel (freeing a slot potentially for another worker
				// to be spawned later).
				<-tr.sem
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

func (tr *TaskRunner) addTask(f TaskFunc) bool {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if tr.err != nil {
		return false
	}

	tr.wg.Add(1)
	tr.tasks = append(tr.tasks, f)
	return true
}

func (tr *TaskRunner) selectTask() TaskFunc {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if len(tr.tasks) == 0 {
		return nil
	}

	task := tr.tasks[0]
	tr.tasks = tr.tasks[1:]
	return task
}

func (tr *TaskRunner) storeErrorAndCancel(err error) {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if tr.err == nil {
		tr.err = err
		tr.cancel()
	}
}

func (tr *TaskRunner) emptyForCancel() {
	tr.lock.Lock()
	defer tr.lock.Unlock()

	if tr.err == nil {
		tr.err = tr.ctx.Err()
	}

	for {
		if len(tr.tasks) == 0 {
			break
		}

		tr.tasks = tr.tasks[1:]
		tr.wg.Done()
	}
}

// Wait waits for all tasks to be completed, or a task to raise an error,
// or the parent context to have been canceled.
func (tr *TaskRunner) Wait() error {
	tr.wg.Wait()
	return tr.err
}
