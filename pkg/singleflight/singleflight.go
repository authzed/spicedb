// Package singleflight provides a duplicate function call suppression
// mechanism.
//
// This is a fork of golang.org/x/sync/singleflight that adds generic types and
// supports context deadlines and cancellation.
package singleflight

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// contextGroup represents a list of contexts that must all reach deadline or
// cancellation before Done() is closed.
type contextGroup struct {
	first    context.Context
	doneChan chan struct{}
	added    chan<- context.Context
	err      atomic.Value
}

var _ context.Context = &contextGroup{}

func newCG(first context.Context) (*contextGroup, func()) {
	firstWrapped, firstCancel := context.WithCancel(first)
	addedChan := make(chan context.Context)
	c := &contextGroup{
		first:    firstWrapped,
		doneChan: make(chan struct{}),
		added:    addedChan,
	}

	tracked := []context.Context{firstWrapped}
	awaiting := 1

	go func() {
		for awaiting > 0 {
			cases := make([]reflect.SelectCase, 0, len(tracked)+1)
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(addedChan),
			})
			for _, c := range tracked {
				cases = append(cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(c.Done()),
				})
			}

			selected, newChan, ok := reflect.Select(cases)
			if selected == 0 {
				if ok {
					tracked = append(tracked, newChan.Interface().(context.Context))
					awaiting++
				}
			} else {
				// One of the underlying channels is done
				awaiting--
			}
		}

		c.err.Store(context.Canceled)
		close(c.doneChan)
	}()

	return c, firstCancel
}

func (c *contextGroup) add(ctx context.Context) func() {
	newWrapped, newCancel := context.WithCancel(ctx)
	c.added <- newWrapped
	return newCancel
}

func (c *contextGroup) Err() error                  { return c.err.Load().(error) }
func (c *contextGroup) Value(key any) any           { return c.first.Value(key) }
func (c *contextGroup) Deadline() (time.Time, bool) { return c.first.Deadline() }
func (c *contextGroup) Done() <-chan struct{}       { return c.doneChan }

// errGoexit indicates the runtime.Goexit was called in
// the user given function.
var errGoexit = errors.New("runtime.Goexit was called")

// A panicError is an arbitrary value recovered from a panic
// with the stack trace during the execution of given function.
type panicError struct {
	value any
	stack []byte
}

// Error implements error interface.
func (p *panicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

func newPanicError(v any) error {
	stack := debug.Stack()

	// The first line of the stack trace is of the form "goroutine N [status]:"
	// but by the time the panic reaches Do the goroutine may no longer exist
	// and its status will have changed. Trim out the misleading line.
	if line := bytes.IndexByte(stack[:], '\n'); line >= 0 {
		stack = stack[line+1:]
	}
	return &panicError{value: v, stack: stack}
}

// call is an in-flight or completed singleflight.Do call
type call[T any] struct {
	wg sync.WaitGroup

	// These fields are written once before the WaitGroup is done
	// and are only read after the WaitGroup is done.
	val T
	err error

	// These fields are read and written with the singleflight
	// mutex held before the WaitGroup is done, and are read but
	// not written after the WaitGroup is done.
	dups  int
	chans []chan<- Result[T]
	cg    *contextGroup
}

// Group represents a class of work and forms a namespace in
// which units of work can be executed with duplicate suppression.
type Group[T any, K comparable] struct {
	mu sync.Mutex     // protects m
	m  map[K]*call[T] // lazily initialized
}

// Result holds the results of Do, so they can be passed
// on a channel.
type Result[T any] struct {
	Val    T
	Err    error
	Shared bool
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// The return value shared indicates whether v was given to multiple callers.
func (g *Group[T, K]) Do(ctx context.Context, key K, fn func(context.Context) (T, error)) (v T, shared bool, err error) {
	ch, cancelFn := g.DoChan(ctx, key, fn)
	defer cancelFn()
	select {
	case result := <-ch:
		return result.Val, result.Shared, result.Err
	case <-ctx.Done():
		var zero T
		return zero, false, ctx.Err()
	}
}

// DoChan is like Do but returns a channel that will receive the results when
// they are ready.
//
// The returned channel will not be closed.
func (g *Group[T, K]) DoChan(ctx context.Context, key K, fn func(context.Context) (T, error)) (<-chan Result[T], func()) {
	ch := make(chan Result[T], 1)
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[K]*call[T])
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		c.chans = append(c.chans, ch)
		cancelFn := c.cg.add(ctx)
		g.mu.Unlock()
		c.wg.Wait()
		return ch, cancelFn
	}
	cg, cancelFn := newCG(ctx)
	c := &call[T]{chans: []chan<- Result[T]{ch}, cg: cg}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	go g.doCall(ctx, c, key, fn)

	return ch, cancelFn
}

// doCall handles the single call for a key.
func (g *Group[T, K]) doCall(ctx context.Context, c *call[T], key K, fn func(context.Context) (T, error)) {
	normalReturn := false
	recovered := false

	// use double-defer to distinguish panic from runtime.Goexit,
	// more details see https://golang.org/cl/134395
	defer func() {
		// the given function invoked runtime.Goexit
		if !normalReturn && !recovered {
			c.err = errGoexit
		}

		g.mu.Lock()
		defer g.mu.Unlock()
		c.wg.Done()
		if g.m[key] == c {
			delete(g.m, key)
		}

		var e *panicError
		if errors.As(c.err, &e) {
			// In order to prevent the waiting channels from being blocked forever,
			// needs to ensure that this panic cannot be recovered.
			if len(c.chans) > 0 {
				go panic(e)
				select {} // Keep this goroutine around so that it will appear in the crash dump.
			} else {
				panic(e)
			}
		} else if errors.Is(c.err, errGoexit) {
			// Already in the process of goexit, no need to call again
		} else {
			// Normal return
			for _, ch := range c.chans {
				ch <- Result[T]{c.val, c.err, c.dups > 0}
			}
		}
	}()

	func() {
		defer func() {
			if !normalReturn {
				// Ideally, we would wait to take a stack trace until we've determined
				// whether this is a panic or a runtime.Goexit.
				//
				// Unfortunately, the only way we can distinguish the two is to see
				// whether the recover stopped the goroutine from terminating, and by
				// the time we know that, the part of the stack trace relevant to the
				// panic has been discarded.
				if r := recover(); r != nil {
					c.err = newPanicError(r)
				}
			}
		}()

		c.val, c.err = fn(ctx)
		normalReturn = true
	}()

	if !normalReturn {
		recovered = true
	}
}

// Forget tells the singleflight to forget about a key.  Future calls
// to Do for this key will call the function rather than waiting for
// an earlier call to complete.
func (g *Group[T, K]) Forget(key K) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}
