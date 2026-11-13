package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type lsResponse = *v1.DispatchLookupSubjectsResponse

// raceClient is a ClusterClient used only to identify a participant in race tests.
type raceClient struct {
	ClusterClient
	name string
}

// scriptedReceiver returns its results in order, then end. If gate is set, each Recv first
// waits for a value on it. If block is set, Recv blocks until ctx is done after the results.
type scriptedReceiver struct {
	ctx     context.Context
	results []lsResponse
	end     error // returned after the results; io.EOF if nil
	gate    chan struct{}
	block   bool
}

func (r *scriptedReceiver) Recv() (lsResponse, error) {
	if r.gate != nil {
		select {
		case <-r.gate:
		case <-r.ctx.Done():
			return nil, r.ctx.Err()
		}
	}
	if len(r.results) > 0 {
		result := r.results[0]
		r.results = r.results[1:]
		return result, nil
	}
	if r.block {
		<-r.ctx.Done()
		return nil, r.ctx.Err()
	}
	if r.end != nil {
		return nil, r.end
	}
	return nil, io.EOF
}

// script describes how one participant behaves in a race test.
type script struct {
	openErr error
	results []lsResponse
	end     error
	gate    chan struct{}
	block   bool
}

func lsResults(prefix string, n int) []lsResponse {
	results := make([]lsResponse, 0, n)
	for i := range n {
		results = append(results, &v1.DispatchLookupSubjectsResponse{
			FoundSubjectsByResourceId: map[string]*v1.FoundSubjects{fmt.Sprintf("%s%d", prefix, i): {}},
		})
	}
	return results
}

func resultIDs(results []lsResponse) []string {
	ids := make([]string, 0, len(results))
	for _, r := range results {
		for id := range r.FoundSubjectsByResourceId {
			ids = append(ids, id)
		}
	}
	return ids
}

// testPolicy is a configurable policy that records what the engine reports to it.
type testPolicy struct {
	basePolicy

	// startFn overrides start, if set.
	startFn func(ctx context.Context, p participant) bool

	// claimAt is the number of results at which a participant may claim; 0 means the default.
	// A negative value means never.
	claimAt int

	// noWinnerErr is returned by noWinnerError, if set.
	noWinnerErr error

	// claimHook, if set, is called at the start of readyToClaim.
	claimHook func(p participant)

	lock      sync.Mutex
	doneCalls map[string][]error         // GUARDED_BY(lock)
	latencies map[string][]time.Duration // GUARDED_BY(lock)
	noWinner  map[string]error           // GUARDED_BY(lock)
}

func (tp *testPolicy) start(ctx context.Context, p participant) bool {
	if tp.startFn != nil {
		return tp.startFn(ctx, p)
	}
	return true
}

func (tp *testPolicy) readyToClaim(p participant, received int, complete bool) bool {
	if tp.claimHook != nil {
		tp.claimHook(p)
	}
	switch {
	case tp.claimAt < 0:
		return false
	case tp.claimAt == 0:
		return tp.basePolicy.readyToClaim(p, received, complete)
	default:
		return complete || received >= tp.claimAt
	}
}

func (tp *testPolicy) firstResult(p participant, latency time.Duration) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	if tp.latencies == nil {
		tp.latencies = map[string][]time.Duration{}
	}
	tp.latencies[p.name] = append(tp.latencies[p.name], latency)
}

func (tp *testPolicy) done(p participant, err error) {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	if tp.doneCalls == nil {
		tp.doneCalls = map[string][]error{}
	}
	tp.doneCalls[p.name] = append(tp.doneCalls[p.name], err)
}

func (tp *testPolicy) noWinnerError(errs map[string]error) error {
	tp.lock.Lock()
	defer tp.lock.Unlock()
	tp.noWinner = errs
	if tp.noWinnerErr != nil {
		return tp.noWinnerErr
	}
	return tp.basePolicy.noWinnerError(errs)
}

// testStrategy hands a fixed policy to the engine.
type testStrategy struct{ policy *testPolicy }

func (testStrategy) name() string                       { return "test" }
func (s testStrategy) newPolicy(raceRequest) racePolicy { return s.policy }

// runTestRace races the scripted participants (the first one is the primary) under policy.
// It makes no assertions, so tests may call it from any goroutine.
func runTestRace(ctx context.Context, policy *testPolicy, names []string, scripts map[string]script) ([]lsResponse, error) {
	stats, err := newRaceStats(time.Millisecond)
	if err != nil {
		return nil, err
	}

	participants := make([]participant, 0, len(names))
	for i, name := range names {
		participants = append(participants, participant{
			name:    name,
			primary: i == 0,
			client:  raceClient{name: name},
		})
	}

	stream := dispatch.NewCollectingDispatchStream[lsResponse](ctx)
	err = runRace(ctx, raceRequest{
		reqKey:       "lookupsubjects",
		resource:     tuple.RR("document", "view"),
		subject:      tuple.RR("user", "..."),
		participants: participants,
		stats:        stats,
	}, testStrategy{policy}, func(ctx context.Context, c ClusterClient) (receiver[lsResponse], error) {
		s := scripts[c.(raceClient).name]
		if s.openErr != nil {
			return nil, s.openErr
		}
		return &scriptedReceiver{ctx: ctx, results: s.results, end: s.end, gate: s.gate, block: s.block}, nil
	}, stream)
	return stream.Results(), err
}

func TestRaceDoneCalledOncePerParticipant(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		openErr := errors.New("open failed")
		recvErr := errors.New("recv failed")

		policy := &testPolicy{
			startFn: func(ctx context.Context, p participant) bool {
				switch p.name {
				case "not-started":
					return false
				case "waiting":
					// Never allowed to start: canceled once another participant wins.
					<-ctx.Done()
				case "winner":
					// Let every other participant fail or block first.
					time.Sleep(time.Second)
				}
				return true
			},
		}

		names := []string{primaryDispatcher, "not-started", "waiting", "open-error", "recv-error", "winner"}
		results, err := runTestRace(t.Context(), policy, names, map[string]script{
			primaryDispatcher: {block: true},
			"open-error":      {openErr: openErr},
			"recv-error":      {end: recvErr},
			"winner":          {results: lsResults("w", 2)},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"w0", "w1"}, resultIDs(results))

		require.Len(t, policy.doneCalls, len(names))
		for _, name := range names {
			require.Len(t, policy.doneCalls[name], 1, name)
		}
		require.ErrorIs(t, policy.doneCalls[primaryDispatcher][0], context.Canceled)
		require.ErrorIs(t, policy.doneCalls["not-started"][0], errNotStarted)
		require.ErrorIs(t, policy.doneCalls["waiting"][0], context.Canceled)
		require.ErrorIs(t, policy.doneCalls["open-error"][0], openErr)
		require.ErrorIs(t, policy.doneCalls["recv-error"][0], recvErr)
		require.NoError(t, policy.doneCalls["winner"][0])
	})
}

func TestRaceLoserReportsNoError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		primaryGate := make(chan struct{})
		release := make(chan struct{})
		policy := &testPolicy{claimHook: func(p participant) {
			if p.name == "loser" {
				// Hold the loser until the primary has won, so its claim fails.
				<-release
			}
		}}

		var results []lsResponse
		var raceErr error
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			results, raceErr = runTestRace(t.Context(), policy, []string{primaryDispatcher, "loser"}, map[string]script{
				primaryDispatcher: {results: lsResults("p", 1), gate: primaryGate},
				"loser":           {results: lsResults("l", 1)},
			})
		}()

		synctest.Wait()           // the loser is now held in readyToClaim
		primaryGate <- struct{}{} // p0: the primary claims and wins
		synctest.Wait()
		close(release) // the loser's claim now fails
		synctest.Wait()
		primaryGate <- struct{}{} // EOF
		<-finished

		require.NoError(t, raceErr)
		require.Equal(t, []string{"p0"}, resultIDs(results))
		require.Equal(t, []error{nil}, policy.doneCalls["loser"])
	})
}

func TestRaceDefaultClaimPublishesImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		policy := &testPolicy{}
		stats, err := newRaceStats(time.Millisecond)
		require.NoError(t, err)

		stream := dispatch.NewCollectingDispatchStream[lsResponse](t.Context())
		var raceErr error
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			raceErr = runRace(t.Context(), raceRequest{
				reqKey:       "lookupsubjects",
				participants: []participant{{name: primaryDispatcher, primary: true}, {name: "secondary", client: raceClient{name: "secondary"}}},
				stats:        stats,
			}, testStrategy{policy}, func(ctx context.Context, c ClusterClient) (receiver[lsResponse], error) {
				if c == nil {
					return &scriptedReceiver{ctx: ctx, block: true}, nil
				}
				return &scriptedReceiver{ctx: ctx, results: lsResults("s", 3), gate: gate}, nil
			}, stream)
		}()

		// Each result is published as soon as it is received.
		for i := 1; i <= 3; i++ {
			gate <- struct{}{}
			synctest.Wait()
			require.Len(t, stream.Results(), i)
		}
		gate <- struct{}{} // EOF
		<-finished

		require.NoError(t, raceErr)
		require.Equal(t, []string{"s0", "s1", "s2"}, resultIDs(stream.Results()))
	})
}

func TestRaceBufferedClaimFlushesInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		policy := &testPolicy{claimAt: 3}
		stats, err := newRaceStats(time.Millisecond)
		require.NoError(t, err)

		stream := dispatch.NewCollectingDispatchStream[lsResponse](t.Context())
		var raceErr error
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			raceErr = runRace(t.Context(), raceRequest{
				reqKey: "lookupsubjects",
				participants: []participant{
					{name: primaryDispatcher, primary: true, client: raceClient{name: primaryDispatcher}},
					{name: "secondary", client: raceClient{name: "secondary"}},
				},
				stats: stats,
			}, testStrategy{policy}, func(ctx context.Context, c ClusterClient) (receiver[lsResponse], error) {
				if c.(raceClient).name == primaryDispatcher {
					// Two quick results, then nothing: never reaches the claim threshold.
					return &scriptedReceiver{ctx: ctx, results: lsResults("p", 2), block: true}, nil
				}
				return &scriptedReceiver{ctx: ctx, results: lsResults("s", 5), gate: gate}, nil
			}, stream)
		}()

		// Below the threshold, nothing is published.
		for range 2 {
			gate <- struct{}{}
			synctest.Wait()
			require.Empty(t, stream.Results())
		}

		// At the threshold, the buffered results and the current one are published, in order.
		gate <- struct{}{}
		synctest.Wait()
		require.Equal(t, []string{"s0", "s1", "s2"}, resultIDs(stream.Results()))

		// After winning, results are published as they arrive.
		gate <- struct{}{}
		synctest.Wait()
		require.Len(t, stream.Results(), 4)

		gate <- struct{}{} // s4
		gate <- struct{}{} // EOF
		<-finished

		require.NoError(t, raceErr)
		require.Equal(t, []string{"s0", "s1", "s2", "s3", "s4"}, resultIDs(stream.Results()))
		require.ErrorIs(t, policy.doneCalls[primaryDispatcher][0], context.Canceled)
	})
}

func TestRaceCompleteStreamClaimsBelowThreshold(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		policy := &testPolicy{claimAt: 10}
		results, err := runTestRace(t.Context(), policy, []string{primaryDispatcher, "secondary"}, map[string]script{
			primaryDispatcher: {block: true},
			"secondary":       {results: lsResults("s", 2)},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"s0", "s1"}, resultIDs(results))
	})
}

func TestRaceNeverClaimingPolicyHitsBufferCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		policy := &testPolicy{claimAt: -1}
		results, err := runTestRace(t.Context(), policy, []string{primaryDispatcher}, map[string]script{
			primaryDispatcher: {results: lsResults("p", maxBufferedResults+1)},
		})
		require.ErrorIs(t, err, errClaimBufferFull)
		require.Empty(t, results)
	})
}

func TestRaceNeverClaimingCompleteStreamPublishesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		policy := &testPolicy{claimAt: -1}
		results, err := runTestRace(t.Context(), policy, []string{primaryDispatcher}, map[string]script{
			primaryDispatcher: {results: lsResults("p", 2)},
		})
		require.ErrorContains(t, err, "no dispatcher returned results")
		require.Empty(t, results)
		require.NoError(t, policy.doneCalls[primaryDispatcher][0])
	})
}

func TestRaceWinnerErrorAfterPublishingIsReturned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		streamErr := errors.New("stream broke")
		policy := &testPolicy{}
		results, err := runTestRace(t.Context(), policy, []string{primaryDispatcher}, map[string]script{
			primaryDispatcher: {results: lsResults("p", 1), end: streamErr},
		})
		require.ErrorIs(t, err, streamErr)
		require.Equal(t, []string{"p0"}, resultIDs(results))
	})
}

func TestRaceNoWinnerErrorFallbacks(t *testing.T) {
	primaryErr := errors.New("primary failed")
	secondaryErr := errors.New("secondary failed")
	policyErr := errors.New("chosen by policy")

	t.Run("primary error by default", func(t *testing.T) {
		policy := &testPolicy{}
		_, err := runTestRace(t.Context(), policy, []string{primaryDispatcher, "secondary"}, map[string]script{
			primaryDispatcher: {end: primaryErr},
			"secondary":       {end: secondaryErr},
		})
		require.ErrorIs(t, err, primaryErr)
		require.Equal(t, map[string]error{primaryDispatcher: primaryErr, "secondary": secondaryErr}, policy.noWinner)
	})

	t.Run("error chosen by the policy", func(t *testing.T) {
		policy := &testPolicy{noWinnerErr: policyErr}
		_, err := runTestRace(t.Context(), policy, []string{primaryDispatcher, "secondary"}, map[string]script{
			primaryDispatcher: {end: primaryErr},
			"secondary":       {end: secondaryErr},
		})
		require.ErrorIs(t, err, policyErr)
	})

	t.Run("context error when the policy returns nil", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		policy := &testPolicy{startFn: func(context.Context, participant) bool {
			cancel()
			return false
		}}
		_, err := runTestRace(ctx, policy, []string{primaryDispatcher}, map[string]script{})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("generic error otherwise", func(t *testing.T) {
		policy := &testPolicy{startFn: func(context.Context, participant) bool { return false }}
		_, err := runTestRace(t.Context(), policy, []string{primaryDispatcher}, map[string]script{})
		require.ErrorContains(t, err, "no dispatcher returned results")
		require.Empty(t, policy.noWinner, "not-started participants have no error")
	})
}

func TestRaceLatencyExcludesWaitingToStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		policy := &testPolicy{startFn: func(context.Context, participant) bool {
			time.Sleep(time.Hour)
			return true
		}}
		_, err := runTestRace(t.Context(), policy, []string{primaryDispatcher}, map[string]script{
			primaryDispatcher: {results: lsResults("p", 1)},
		})
		require.NoError(t, err)
		require.Equal(t, []time.Duration{0}, policy.latencies[primaryDispatcher])
	})
}

func TestRaceRecordsSecondaryStats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stats, err := newRaceStats(time.Millisecond)
		require.NoError(t, err)

		stream := dispatch.NewCollectingDispatchStream[lsResponse](t.Context())
		err = runRace(t.Context(), raceRequest{
			reqKey: "lookupsubjects",
			participants: []participant{
				{name: primaryDispatcher, primary: true, client: raceClient{name: primaryDispatcher}},
				{name: "secondary", client: raceClient{name: "secondary"}},
			},
			stats: stats,
		}, testStrategy{&testPolicy{}}, func(ctx context.Context, c ClusterClient) (receiver[lsResponse], error) {
			if c.(raceClient).name == primaryDispatcher {
				time.Sleep(time.Second)
				return &scriptedReceiver{ctx: ctx, results: lsResults("p", 1)}, nil
			}
			time.Sleep(3 * time.Millisecond)
			return &scriptedReceiver{ctx: ctx, results: lsResults("s", 1)}, nil
		}, stream)
		require.NoError(t, err)

		// Only the secondary's latency is recorded, asynchronously.
		synctest.Wait()
		digest := stats.secondaryDigests["lookupsubjects"].digest
		require.Equal(t, uint64(1), digest.Count())
		require.InDelta(t, 3, digest.Quantile(0.5), 0.001)
	})
}

func TestUnaryReceiver(t *testing.T) {
	ok := &unaryReceiver[int]{resp: 42}
	v, err := ok.Recv()
	require.NoError(t, err)
	require.Equal(t, 42, v)
	_, err = ok.Recv()
	require.ErrorIs(t, err, io.EOF)

	callErr := errors.New("call failed")
	failed := &unaryReceiver[int]{resp: 42, err: callErr}
	_, err = failed.Recv()
	require.ErrorIs(t, err, callErr)
}

// unaryClient answers a unary call with its fixed response, or blocks until the call's
// context is done when it has none.
type unaryClient struct {
	ClusterClient
	resp *v1.DispatchCheckResponse
}

func TestUnaryRaceReturnsWithoutWaitingForLosers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stats, err := newRaceStats(time.Millisecond)
		require.NoError(t, err)
		policy := &testPolicy{}

		ctx, cancel := context.WithCancel(t.Context())
		resp, err := runUnaryRace(ctx, raceRequest{
			reqKey: "check",
			participants: []participant{
				{name: primaryDispatcher, primary: true, client: &unaryClient{}},
				{name: "secondary", client: &unaryClient{resp: &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 2}}}},
			},
			stats: stats,
		}, testStrategy{policy}, func(ctx context.Context, c ClusterClient) (*v1.DispatchCheckResponse, error) {
			client := c.(*unaryClient)
			if client.resp == nil {
				<-ctx.Done() // the primary hangs until the caller cancels
				return &v1.DispatchCheckResponse{}, ctx.Err()
			}
			return client.resp, nil
		})

		// The race returned while the primary was still running.
		require.NoError(t, err)
		require.Equal(t, uint32(2), resp.Metadata.DispatchCount)

		// Once the caller cancels, the loser exits and is reported exactly once.
		cancel()
		synctest.Wait()
		policy.lock.Lock()
		defer policy.lock.Unlock()
		require.Len(t, policy.doneCalls[primaryDispatcher], 1)
		require.ErrorIs(t, policy.doneCalls[primaryDispatcher][0], context.Canceled)
		require.Equal(t, []error{nil}, policy.doneCalls["secondary"])
	})
}

func TestUnaryRaceWaitsForEveryoneWithoutWinner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stats, err := newRaceStats(time.Millisecond)
		require.NoError(t, err)
		primaryErr := errors.New("primary failed")

		_, err = runUnaryRace(t.Context(), raceRequest{
			reqKey: "check",
			participants: []participant{
				{name: primaryDispatcher, primary: true, client: raceClient{name: primaryDispatcher}},
				{name: "secondary", client: raceClient{name: "secondary"}},
			},
			stats: stats,
		}, testStrategy{&testPolicy{}}, func(_ context.Context, c ClusterClient) (*v1.DispatchCheckResponse, error) {
			if c.(raceClient).name == primaryDispatcher {
				return &v1.DispatchCheckResponse{}, primaryErr
			}
			// The secondary fails later; the race must wait for it before choosing the error.
			time.Sleep(time.Second)
			return &v1.DispatchCheckResponse{}, errors.New("secondary failed")
		})
		require.ErrorIs(t, err, primaryErr)
	})
}

func TestRaceEmptySecondaryStreamReportsFirstResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A secondary whose stream ends without results has still answered: its time to the end
		// of the stream is its first-result latency.
		stats, err := newRaceStats(time.Millisecond)
		require.NoError(t, err)
		policy := &testPolicy{}

		stream := dispatch.NewCollectingDispatchStream[lsResponse](t.Context())
		err = runRace(t.Context(), raceRequest{
			reqKey: "lookupsubjects",
			participants: []participant{
				{name: primaryDispatcher, primary: true, client: raceClient{name: primaryDispatcher}},
				{name: "secondary", client: raceClient{name: "secondary"}},
			},
			stats: stats,
		}, testStrategy{policy}, func(ctx context.Context, c ClusterClient) (receiver[lsResponse], error) {
			if c.(raceClient).name == primaryDispatcher {
				return &scriptedReceiver{ctx: ctx, block: true}, nil
			}
			time.Sleep(2 * time.Millisecond)
			return &scriptedReceiver{ctx: ctx}, nil
		}, stream)
		require.NoError(t, err)
		require.Empty(t, stream.Results())

		synctest.Wait() // stats are recorded asynchronously
		require.Equal(t, []time.Duration{2 * time.Millisecond}, policy.latencies["secondary"])
		require.Equal(t, uint64(1), stats.secondaryDigests["lookupsubjects"].digest.Count())
	})
}

func TestRaceSingleFailedParticipantErrorIsReturned(t *testing.T) {
	// Without a primary error, the error of the only failed participant is returned, so its
	// status reaches the caller.
	secondaryErr := errors.New("secondary failed")
	_, err := runTestRace(t.Context(), &testPolicy{}, []string{"secondary"}, map[string]script{
		"secondary": {end: secondaryErr},
	})
	require.ErrorIs(t, err, secondaryErr)
}
