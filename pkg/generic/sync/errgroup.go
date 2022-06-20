package sync

import (
	"context"
	"errors"
	"sync"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type TypedGroup[T error] struct {
	sync.Once
	*errgroup.Group
}

// WithContext returns a new Group and an associated Context derived from ctx.
//
// The derived Context is canceled the first time a function passed to Go
// returns a non-nil error or the first time Wait returns, whichever occurs
// first.
func WithContext[T error](ctx context.Context) (*TypedGroup[T], context.Context) {
	inner, ctx := errgroup.WithContext(ctx)
	return &TypedGroup[T]{sync.Once{}, inner}, ctx
}

// Go calls the given function in a new goroutine.
// It blocks until the new goroutine can be added without the number of
// active goroutines in the group exceeding the configured limit.
//
// The first call to return a non-nil error cancels the group; its error will be
// returned by Wait.
func (g *TypedGroup[T]) Go(f func() T) {
	g.initialize()
	g.Group.Go(func() error {
		resp := f()
		log.Trace().Err(resp).Msg("delegate returned")
		return resp
	})
}

// SetLimit limits the number of active goroutines in this group to at most n.
// A negative value indicates no limit.
//
// Any subsequent call to the Go method will block until it can add an active
// goroutine without exceeding the configured limit.
//
// The limit must not be modified while any goroutines in the group are active.
func (g *TypedGroup[T]) SetLimit(n int) {
	if n == 0 {
		// There might be a good reason to allow this upstream, but not for our use cases
		panic("errgroup called with concurrency of 0, will always block")
	}
	g.initialize()
	g.Group.SetLimit(n)
}

// TryGo calls the given function in a new goroutine only if the number of
// active goroutines in the group is currently below the configured limit.
//
// The return value reports whether the goroutine was started.
func (g *TypedGroup[T]) TryGo(f func() T) bool {
	g.initialize()
	return g.Group.TryGo(func() error {
		return f()
	})
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the first non-nil error (if any) from them.
func (g *TypedGroup[T]) Wait() T {
	var returnedErr T

	g.initialize()
	if err := g.Group.Wait(); err != nil {
		if !errors.As(err, &returnedErr) {
			panic("all returned errors should be of the proper type")
		}
	}
	return returnedErr
}

func (g *TypedGroup[T]) initialize() {
	g.Once.Do(func() {
		if g.Group == nil {
			g.Group = &errgroup.Group{}
		}
	})
}
