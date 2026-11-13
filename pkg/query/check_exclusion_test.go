package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParallelExclusionKeepsBaseErrorPrecedence(t *testing.T) {
	for _, baseError := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty-base", true: "base-error"}[baseError], func(t *testing.T) {
			ctx, scope := parallelTestContext(t)
			WithCheckConcurrency(2)(ctx)
			excludedFinished := make(chan struct{})
			baseErr := errors.New("base failed")
			excludedErr := errors.New("excluded failed")
			main := checkLeaf(func(*Context) (*Path, error) {
				<-excludedFinished

				if baseError {
					return nil, baseErr
				}

				return nil, nil
			})
			excluded := checkLeaf(func(*Context) (*Path, error) {
				defer close(excludedFinished)
				return nil, excludedErr
			})
			it := NewExclusionIterator(main, excluded)
			it.checkConcurrency = 2
			path, err := ctx.Check(it, Object{}, ObjectAndRelation{})
			require.Nil(t, path)

			if baseError {
				require.ErrorIs(t, err, baseErr)
			} else {
				require.NoError(t, err)
			}

			scope.Wait()
		})
	}
}

func TestSerialExclusionDoesNotStartExcludedAfterBaseDenial(t *testing.T) {
	ctx, scope := parallelTestContext(t)
	WithCheckExecution(CheckExecutionOptions{BaseFirstExclusion: true})(ctx)
	it := NewExclusionIterator(NewFixedIterator(), checkLeaf(func(*Context) (*Path, error) {
		t.Error("an empty base must skip the excluded operand at width one")
		return nil, errors.New("unused")
	}))
	it.checkConcurrency = 1
	path, err := ctx.Check(it, Object{}, ObjectAndRelation{})
	require.NoError(t, err)
	require.Nil(t, path)
	scope.Wait()
}
