package query

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/genutil/execscope"
)

// controlledCheck embeds the ordinary leaf shape while controlling its execution.
type controlledCheck struct {
	Iterator
	check func(*Context) (*Path, error)
}

func (c *controlledCheck) CheckImpl(ctx *Context, _ Object, _ ObjectAndRelation) (*Path, error) {
	return c.check(ctx)
}

func checkLeaf(check func(*Context) (*Path, error)) Iterator {
	return &controlledCheck{Iterator: NewFixedIterator(), check: check}
}

func parallelTestContext(t *testing.T) (*Context, *execscope.Scope) {
	t.Helper()
	scope := execscope.New()
	ctx := NewLocalContext(scope.Context(t.Context()))
	ctx.MarkAsOperation(NewFixedIterator(), OperationCheck)
	return ctx, scope
}

func testCheckPath() *Path { return MustPathFromString("document:d#view@user:u") }

func TestScalarRelationFormsKeepDirectBeforeIndirect(t *testing.T) {
	for _, intersection := range []bool{false, true} {
		t.Run(strconv.FormatBool(intersection), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, scope := parallelTestContext(t)
				WithCheckConcurrency(2)(ctx)
				var indirectStarted atomic.Bool
				direct := checkLeaf(func(*Context) (*Path, error) {
					// Let a speculative sibling run before the direct decision arrives.
					// Relation forms must await that decision rather than speculate.
					time.Sleep(time.Second)

					if intersection {
						return nil, nil
					}

					return testCheckPath(), nil
				})
				indirect := checkLeaf(func(*Context) (*Path, error) {
					indirectStarted.Store(true)
					return testCheckPath(), nil
				})
				var it Iterator

				if intersection {
					set := NewIntersectionIterator(direct, indirect).(*IntersectionIterator)
					set.checkConcurrency = 2
					it = set
				} else {
					set := NewUnionIterator(direct, indirect).(*UnionIterator)
					set.checkConcurrency = 2
					it = set
				}

				_, err := ctx.Check(it, Object{}, ObjectAndRelation{})
				require.NoError(t, err)
				scope.Wait()
				require.False(t, indirectStarted.Load(), "a decisive direct result must skip indirect work")
			})
		})
	}
}

func TestSetCheckParallelWidthAndAdmission(t *testing.T) {
	ctx, scope := parallelTestContext(t)
	started := make(chan int, 5)
	release := make(chan struct{})
	var active, peak atomic.Int32
	children := make([]Iterator, 5)

	for index := range children {
		children[index] = checkLeaf(func(*Context) (*Path, error) {
			current := active.Add(1)

			for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
			}

			defer active.Add(-1)
			started <- index
			<-release
			return nil, nil
		})
	}

	done := make(chan error, 1)
	go func() {
		_, err := checkSetParallel(ctx, children, Object{}, ObjectAndRelation{}, 2, setCheckUnion)
		done <- err
	}()
	first, second := <-started, <-started
	require.ElementsMatch(t, []int{0, 1}, []int{first, second})
	require.Equal(t, int32(2), active.Load())
	close(release)
	require.NoError(t, <-done)
	scope.Wait()
	require.LessOrEqual(t, peak.Load(), int32(2))
}

func TestSetCheckParallelSerialDoesNotSpeculate(t *testing.T) {
	ctx, scope := parallelTestContext(t)
	var later atomic.Bool
	children := []Iterator{
		checkLeaf(func(*Context) (*Path, error) { return testCheckPath(), nil }),
		checkLeaf(func(*Context) (*Path, error) { later.Store(true); return nil, nil }),
	}
	result, err := checkSetParallel(ctx, children, Object{}, ObjectAndRelation{}, 1, setCheckUnion)
	require.NoError(t, err)
	require.NotNil(t, result)
	scope.Wait()
	require.False(t, later.Load())
}

func TestSetCheckParallelNestedProgress(t *testing.T) {
	ctx, scope := parallelTestContext(t)
	deadline, cancel := context.WithTimeout(ctx.Context, 5*time.Second)
	defer cancel()
	ctx.Context = deadline
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	leaf := checkLeaf(func(ctx *Context) (*Path, error) {
		entered <- struct{}{}

		select {
		case <-release:
			return testCheckPath(), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	nested := checkLeaf(func(ctx *Context) (*Path, error) {
		return checkSetParallel(ctx, []Iterator{leaf, leaf}, Object{}, ObjectAndRelation{}, 2, setCheckIntersection)
	})
	done := make(chan error, 1)
	go func() {
		_, err := checkSetParallel(ctx, []Iterator{nested, NewFixedIterator()}, Object{}, ObjectAndRelation{}, 2, setCheckUnion)
		done <- err
	}()

	for range 2 {
		select {
		case <-entered:
		case <-deadline.Done():
			t.Fatal("nested reducer could not launch its children")
		}
	}

	close(release)
	require.NoError(t, <-done)
	scope.Wait()
}

func TestSetCheckParallelConditionalResults(t *testing.T) {
	for _, kind := range []setCheckKind{setCheckUnion, setCheckIntersection} {
		t.Run(string(kind), func(t *testing.T) {
			ctx, scope := parallelTestContext(t)
			children := make([]Iterator, 2)

			for index, name := range []string{"first", "second"} {
				children[index] = checkLeaf(func(*Context) (*Path, error) {
					path := testCheckPath()
					path.Caveat = caveats.CaveatExprForTesting(name)
					return path, nil
				})
			}

			result, err := checkSetParallel(ctx, children, Object{}, ObjectAndRelation{}, 2, kind)
			require.NoError(t, err)
			require.NotNil(t, result.Caveat)
			require.Len(t, result.Caveat.GetOperation().Children, 2)
			scope.Wait()
		})
	}
}

func TestSetCheckParallelShortCircuitAndErrors(t *testing.T) {
	for _, kind := range []setCheckKind{setCheckUnion, setCheckIntersection} {
		t.Run(string(kind), func(t *testing.T) {
			ctx, scope := parallelTestContext(t)
			started := make(chan struct{})
			stopped := make(chan struct{})
			loser := checkLeaf(func(ctx *Context) (*Path, error) {
				close(started)
				<-ctx.Done()
				close(stopped)
				return nil, ctx.Err()
			})
			winner := checkLeaf(func(*Context) (*Path, error) {
				<-started

				if kind == setCheckUnion {
					return testCheckPath(), nil
				}

				return nil, nil
			})
			result, err := checkSetParallel(ctx, []Iterator{loser, winner}, Object{}, ObjectAndRelation{}, 2, kind)
			require.NoError(t, err)
			require.Equal(t, kind == setCheckUnion, result != nil)
			scope.Wait()
			<-stopped

			failure := errors.New("datastore failure")
			bad := checkLeaf(func(*Context) (*Path, error) { return nil, failure })
			waiting := checkLeaf(func(ctx *Context) (*Path, error) { <-ctx.Done(); return nil, ctx.Err() })
			_, err = checkSetParallel(ctx, []Iterator{bad, waiting}, Object{}, ObjectAndRelation{}, 2, kind)
			require.ErrorIs(t, err, failure)
			scope.Wait()
		})
	}
}

func TestSetCheckParallelCanceledBeforeLaunch(t *testing.T) {
	ctx, scope := parallelTestContext(t)
	canceled, cancel := context.WithCancel(ctx.Context)
	cancel()
	ctx.Context = canceled
	var called atomic.Bool
	child := checkLeaf(func(*Context) (*Path, error) { called.Store(true); return nil, nil })
	_, err := checkSetParallel(ctx, []Iterator{child, child}, Object{}, ObjectAndRelation{}, 2, setCheckUnion)
	require.ErrorIs(t, err, context.Canceled)
	scope.Wait()
	require.False(t, called.Load())
}

func TestSetCheckParallelEmptyAndDenials(t *testing.T) {
	for _, kind := range []setCheckKind{setCheckUnion, setCheckIntersection} {
		t.Run(string(kind), func(t *testing.T) {
			ctx, scope := parallelTestContext(t)

			for _, children := range [][]Iterator{nil, {NewFixedIterator()}, {NewFixedIterator(), NewFixedIterator()}} {
				result, err := checkSetParallel(ctx, children, Object{}, ObjectAndRelation{}, 2, kind)
				require.NoError(t, err)
				require.Nil(t, result)
				scope.Wait()
			}
		})
	}
}

func TestSetCheckParallelCompletionPermutations(t *testing.T) {
	for _, kind := range []setCheckKind{setCheckUnion, setCheckIntersection} {
		for _, first := range []int{0, 1} {
			t.Run(string(kind)+strconv.Itoa(first), func(t *testing.T) {
				ctx, scope := parallelTestContext(t)
				release := []chan struct{}{make(chan struct{}), make(chan struct{})}
				completed := []chan struct{}{make(chan struct{}), make(chan struct{})}
				started := make(chan struct{}, 2)
				children := make([]Iterator, 2)

				for index, name := range []string{"first", "second"} {
					children[index] = checkLeaf(func(*Context) (*Path, error) {
						started <- struct{}{}
						<-release[index]
						path := testCheckPath()
						path.Caveat = caveats.CaveatExprForTesting(name)
						close(completed[index])
						return path, nil
					})
				}

				done := make(chan *Path, 1)
				go func() {
					result, err := checkSetParallel(ctx, children, Object{}, ObjectAndRelation{}, 2, kind)
					if err != nil {
						t.Error(err)
					}

					done <- result
				}()
				<-started
				<-started
				close(release[first])
				<-completed[first]
				close(release[1-first])
				result := <-done
				require.NotNil(t, result)
				require.Len(t, result.Caveat.GetOperation().Children, 2)
				scope.Wait()
			})
		}
	}
}
