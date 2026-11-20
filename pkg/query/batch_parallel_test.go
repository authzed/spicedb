package query

import (
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatchNamedBranchesUseBoundedParallelism(t *testing.T) {
	for _, intersection := range []bool{false, true} {
		t.Run(fmt.Sprint(intersection), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, scope := parallelTestContext(t)
				WithCheckConcurrency(2)(ctx)
				WithCheckExecution(CheckExecutionOptions{BatchChecks: true})(ctx)
				started := make(chan int, 3)
				release := make(chan struct{})
				defer close(release)
				children := make([]Iterator, 3)

				for i := range children {
					leaf := checkLeaf(func(ctx *Context) (*Path, error) {
						started <- i

						select {
						case <-release:
							return testCheckPath(), nil
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					})
					children[i] = NewAliasIterator("document", fmt.Sprintf("branch%d", i), leaf)
				}

				var it Iterator

				if intersection {
					set := NewIntersectionIterator(children...).(*IntersectionIterator)
					set.checkConcurrency = 2
					it = set
				} else {
					set := NewUnionIterator(children...).(*UnionIterator)
					set.checkConcurrency = 2
					it = set
				}

				done := make(chan error, 1)
				go func() {
					_, err := ctx.CheckManyResources(it, []Object{NewObject("document", "d")}, NewObject("user", "u").WithEllipses())
					done <- err
				}()

				for range 2 {
					select {
					case <-started:
					case <-time.After(time.Second):
						t.Fatal("independent batch branches remained serial")
					}
				}

				synctest.Wait()
				require.Empty(t, started, "the third branch must wait for capacity")

				if intersection {
					release <- struct{}{}
					release <- struct{}{}
					<-started
					release <- struct{}{}
				} else {
					release <- struct{}{}
				}

				require.NoError(t, <-done)
				scope.Wait()
			})
		})
	}
}

func TestBatchParallelRetainsPerTargetIntersectionDenials(t *testing.T) {
	ctx, scope := parallelTestContext(t)
	WithCheckConcurrency(2)(ctx)
	WithCheckExecution(CheckExecutionOptions{BatchChecks: true})(ctx)
	first := NewAliasIterator("document", "reader", NewFixedIterator(
		*MustPathFromString("document:d0#reader@user:u"),
		*MustPathFromString("document:d1#reader@user:u"),
	))
	second := NewAliasIterator("document", "writer", NewFixedIterator(*MustPathFromString("document:d1#writer@user:u")))
	set := NewIntersectionIterator(first, second).(*IntersectionIterator)
	set.checkConcurrency = 2
	paths, err := ctx.CheckManyResources(
		set,
		[]Object{NewObject("document", "d0"), NewObject("document", "d1")},
		NewObject("user", "u").WithEllipses(),
		AllowSingleResult,
	)
	require.NoError(t, err)
	require.Nil(t, paths[0])
	require.NotNil(t, paths[1], "every intersection child must retain the second candidate")
	scope.Wait()
}
