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

	wg    sync.WaitGroup
	err   error
	lock  sync.Mutex
	tasks chan TaskFunc
}

// NewPreloadedTaskRunner creates a new PreloadedTaskRunner with the specified concurrency limit and initial capacity.
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
		tasks:  make(chan TaskFunc, initialCapacity),
	}
}

// Add adds the given task function to be run.
func (tr *PreloadedTaskRunner) Add(f TaskFunc) {
	tr.wg.Add(1)
	tr.tasks <- f
}

// Start starts running the tasks in the task runner. This does *not* wait for the tasks to complete, but rather returns immediately.
func (tr *PreloadedTaskRunner) Start() {
	go tr.run()
}

// StartAndWait starts running the tasks in the task runner and waits for them to complete.
func (tr *PreloadedTaskRunner) StartAndWait() error {
	tr.Start()
	tr.wg.Wait()

	tr.lock.Lock()
	defer tr.lock.Unlock()

	return tr.err
}

func (tr *PreloadedTaskRunner) run() {
	for {
		select {
		case <-tr.ctx.Done():
			tr.emptyForCancel()
			return
		case task := <-tr.tasks:
			tr.sem <- struct{}{}
			go tr.runTask(task)
		}
	}
}

func (tr *PreloadedTaskRunner) runTask(task TaskFunc) {
	defer func() {
		<-tr.sem
		tr.wg.Done()
	}()

	err := task(tr.ctx)
	if err != nil {
		tr.storeErrorAndCancel(err)
	}
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

	for len(tr.tasks) > 0 {
		<-tr.tasks
		tr.wg.Done()
	}
}
