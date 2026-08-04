package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

type PauseFunc func(t testing.TB)

type PausableDatastoreTester interface {
	// NewPausable returns a datastore along with a function that freezes its
	// backing store. Nothing that runs concurrently with the suite may share that backing
	// store, since freezing it would stall those tests too.
	NewPausable(t testing.TB) (datastore.Datastore, PauseFunc)
}

// PausableTester grants a DatastoreTester the ability to create a pausable
// datastore, so that the suite's pause-dependent tests run instead of skipping.
// Engines whose backing store cannot be frozen are returned unchanged.
func PausableTester(tester DatastoreTester, engine testdatastore.RunningEngineForTest) DatastoreTester {
	pausableEngine, ok := engine.(testdatastore.PausableEngineForTest)
	if !ok {
		return tester
	}

	return &pausableTester{
		DatastoreTester: tester,
		newPausable: func(t testing.TB) (datastore.Datastore, PauseFunc) {
			ds, err := tester.New(t, DefaultRevisionParameters(), 16)
			require.NoError(t, err)

			return ds, pausableEngine.Pause
		},
	}
}

type pausableTester struct {
	DatastoreTester
	newPausable func(t testing.TB) (datastore.Datastore, PauseFunc)
}

func (p *pausableTester) NewPausable(t testing.TB) (datastore.Datastore, PauseFunc) {
	return p.newPausable(t)
}

func (p *pausableTester) unwrap() DatastoreTester { return p.DatastoreTester }

// asPausable reports whether the tester, or any tester it decorates, can pause
// its datastore. Decorators embed DatastoreTester as an interface, which does
// not promote the optional capabilities of what they wrap.
func asPausable(tester DatastoreTester) (PausableDatastoreTester, bool) {
	for tester != nil {
		if pausable, ok := tester.(PausableDatastoreTester); ok {
			return pausable, true
		}

		unwrappable, ok := tester.(interface{ unwrap() DatastoreTester })
		if !ok {
			return nil, false
		}
		tester = unwrappable.unwrap()
	}

	return nil, false
}

// ReadyStateWhenPausedTest asserts that ReadyState fails within a bounded time
// once the datastore's backing store stops responding.
// It also asserts that the same call, passed through the ctxPrxy, is also bounded.
func ReadyStateWhenPausedTest(t *testing.T, tester DatastoreTester) {
	pausable, ok := asPausable(tester)
	if !ok {
		t.Skip("tester cannot pause the datastore's backing store")
	}

	ds, pause := pausable.NewPausable(t)

	// Establish that the datastore reports ready first, so any failure below is
	// attributable to the pause.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := ds.ReadyState(t.Context())
		assert.NoError(c, err)
		assert.True(c, state.IsReady, state.Message)
	}, 30*time.Second, 100*time.Millisecond)

	t.Log("datastore reported ready. Pausing it...")

	pause(t)

	t.Log("datastore paused. Calling ReadyState again...")
	state, elapsed, err := waitForReadyStateToRespond(t, ds)
	require.Error(t, err, "ReadyState succeeded against a paused datastore after %s (ready: %v, message: %q)", elapsed, state.IsReady, state.Message)

	t.Log("ctxProxy.ReadyState should pass context/deadline along")
	ctxProxy := datastore.NewSeparatingContextDatastoreProxy(ds)
	state, elapsed, err = waitForReadyStateToRespond(t, ctxProxy)
	require.Error(t, err, "ctxProxy.ReadyState succeeded against a paused datastore after %s (ready: %v, message: %q)", elapsed, state.IsReady, state.Message)
}

func waitForReadyStateToRespond(t *testing.T, ds datastore.Datastore) (datastore.ReadyState, time.Duration, error) {
	t.Helper()

	type readyStateResult struct {
		state   datastore.ReadyState
		err     error
		elapsed time.Duration
	}

	// The call outlives the test when it hangs, so the channel is buffered and
	// the context is detached from the test's own cancellation. Otherwise the
	// abandoned call would observe cleanup canceling t.Context() and the test
	// would pass on an error it caused itself.
	resultChan := make(chan readyStateResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 500*time.Millisecond)
		defer cancel()

		startedAt := time.Now()
		state, err := ds.ReadyState(ctx)
		resultChan <- readyStateResult{state: state, err: err, elapsed: time.Since(startedAt)}
	}()

	const maxReadyStateDuration = 10 * time.Second

	select {
	case result := <-resultChan:
		return result.state, result.elapsed, result.err
	case <-time.After(maxReadyStateDuration):
		t.Fatalf("ReadyState did not return within %s of the datastore being paused, despite a %s deadline", maxReadyStateDuration, 500*time.Millisecond)
		return datastore.ReadyState{}, maxReadyStateDuration, nil
	}
}
