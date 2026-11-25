package scopedflight_test

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/genutil/execscope"
	"github.com/authzed/spicedb/pkg/genutil/scopedflight"
)

func TestCanceledCallerStillRegistersBeforeLaunch(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	scope := execscope.New()
	ctx, cancel := context.WithCancel(scope.Context(t.Context()))
	finishRoot := execscope.Begin(ctx)
	cancel()
	var started atomic.Bool
	var group scopedflight.Group[string, int]
	done := make(chan struct{})
	_, _, err := group.Do(ctx, "key", func(inner context.Context) (int, error) {
		finish := execscope.Begin(inner)
		defer finish()
		started.Store(true)
		close(done)
		return 1, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	finishRoot()
	scope.Wait()
	observed := started.Load()
	<-done
	scope.Wait()
	require.True(t, observed, "phase barrier returned before the canceled flight started")
}
