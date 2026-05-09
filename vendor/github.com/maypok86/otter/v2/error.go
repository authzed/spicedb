package otter

import (
	"bytes"
	"fmt"
	"runtime/debug"
)

const (
	// ErrNotFound should be returned from a Loader.Load/Loader.Reload to indicate that an entry is
	// missing at the underlying data source. This helps the cache to determine
	// if an entry should be deleted.
	//
	// NOTE: this only applies to Cache.Get/Cache.Refresh/Loader.Load/Loader.Reload. For Cache.BulkGet/Cache.BulkRefresh,
	// this works implicitly if you return a map without the key.
	ErrNotFound strError = "otter: the entry was not found in the data source"
)

// strError allows declaring errors as constants.
type strError string

func (err strError) Error() string { return string(err) }

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

func (p *panicError) Unwrap() error {
	err, ok := p.value.(error)
	if !ok {
		return nil
	}

	return err
}

func newPanicError(v any) error {
	stack := debug.Stack()

	// The first line of the stack trace is of the form "goroutine N [status]:"
	// but by the time the panic reaches cache the goroutine may no longer exist
	// and its status will have changed. Trim out the misleading line.
	if line := bytes.IndexByte(stack, '\n'); line >= 0 {
		stack = stack[line+1:]
	}
	return &panicError{value: v, stack: stack}
}
