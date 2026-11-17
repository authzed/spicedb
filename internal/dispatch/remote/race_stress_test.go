package remote

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// The stress tests race a primary and two secondaries with generated delays and failures under
// the real strategies, and check the engine's invariants whatever the interleaving: done is
// called exactly once per participant, results all come from a single participant, and an
// error is returned only when every participant failed.

// doneCountingPolicy wraps a policy to count its done calls.
type doneCountingPolicy struct {
	racePolicy

	lock  sync.Mutex
	dones map[string]int // GUARDED_BY(lock)
}

func (c *doneCountingPolicy) done(p participant, err error) {
	c.lock.Lock()
	c.dones[p.name]++
	c.lock.Unlock()
	c.racePolicy.done(p, err)
}

func (c *doneCountingPolicy) doneCounts() map[string]int {
	c.lock.Lock()
	defer c.lock.Unlock()
	counts := make(map[string]int, len(c.dones))
	for name, count := range c.dones {
		counts[name] = count
	}
	return counts
}

// doneCountingStrategy wraps a strategy, and keeps the policy it creates.
type doneCountingStrategy struct {
	secondaryStrategy
	policy **doneCountingPolicy
}

func (s doneCountingStrategy) newPolicy(r raceRequest) racePolicy {
	policy := &doneCountingPolicy{racePolicy: s.secondaryStrategy.newPolicy(r), dones: map[string]int{}}
	*s.policy = policy
	return policy
}

// delayedReceiver waits the next delay before each Recv, then returns the next result, then end.
type delayedReceiver struct {
	ctx     context.Context
	results []lsResponse
	delays  []time.Duration
	end     error // io.EOF if nil
}

func (r *delayedReceiver) Recv() (lsResponse, error) {
	if len(r.delays) > 0 {
		delay := r.delays[0]
		r.delays = r.delays[1:]
		select {
		case <-time.After(delay):
		case <-r.ctx.Done():
			return nil, r.ctx.Err()
		}
	}
	if len(r.results) > 0 {
		result := r.results[0]
		r.results = r.results[1:]
		return result, nil
	}
	if r.end != nil {
		return nil, r.end
	}
	return nil, io.EOF
}

var stressParticipantNames = []string{primaryDispatcher, "a", "b"}

func stressParticipants() []participant {
	participants := make([]participant, 0, len(stressParticipantNames))
	for i, name := range stressParticipantNames {
		participants = append(participants, participant{
			name:            name,
			primary:         i == 0,
			client:          raceClient{name: name},
			maxPrimaryDelay: time.Millisecond,
		})
	}
	return participants
}

// streamingBehavior is how one participant behaves in a streaming stress race.
type streamingBehavior struct {
	results int
	delays  []time.Duration // before each Recv
	end     error           // after the results; io.EOF if nil
	openErr error
}

func drawStreamingBehavior(t *rapid.T, name string) streamingBehavior {
	results := rapid.IntRange(0, 3).Draw(t, name+" results")
	delays := rapid.SliceOfN(rapid.IntRange(0, 500), results+1, results+1).Draw(t, name+" delays (µs)")
	b := streamingBehavior{results: results, delays: make([]time.Duration, 0, len(delays))}
	for _, d := range delays {
		b.delays = append(b.delays, time.Duration(d)*time.Microsecond)
	}
	switch rapid.SampledFrom([]string{"succeeds", "fails while streaming", "fails to open"}).Draw(t, name+" outcome") {
	case "fails while streaming":
		b.end = errors.New(name + " failed")
	case "fails to open":
		b.openErr = errors.New(name + " failed to open")
	}
	return b
}

// The stress tests run under real time: they check invariants under real interleavings of
// the participants' goroutines, which a synctest bubble's fake clock would narrow down.
// rapid generates the races and shrinks a failing one to a minimal case.

func TestRaceStressStreaming(t *testing.T) {
	strategies := []secondaryStrategy{
		hedgedStrategy{},
		hedgedStrategy{maxDelay: time.Millisecond},
		preferSecondaryStrategy{deadline: time.Millisecond},
	}

	rapid.Check(t, func(t *rapid.T) {
		behaviors := make(map[string]streamingBehavior, len(stressParticipantNames))
		for _, name := range stressParticipantNames {
			behaviors[name] = drawStreamingBehavior(t, name)
		}

		stats, err := newRaceStats(time.Millisecond)
		require.NoError(t, err)

		var policy *doneCountingPolicy
		strategy := doneCountingStrategy{
			secondaryStrategy: rapid.SampledFrom(strategies).Draw(t, "strategy"),
			policy:            &policy,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stream := dispatch.NewCollectingDispatchStream[lsResponse](ctx)
		err = runRace(ctx, raceRequest{
			reqKey:       "lookupsubjects",
			resource:     tuple.RR("document", "view"),
			subject:      tuple.RR("user", "..."),
			participants: stressParticipants(),
			stats:        stats,
		}, strategy, func(ctx context.Context, c ClusterClient) (receiver[lsResponse], error) {
			name := c.(raceClient).name
			b := behaviors[name]
			if b.openErr != nil {
				return nil, b.openErr
			}
			return &delayedReceiver{ctx: ctx, results: lsResults(name, b.results), delays: append([]time.Duration(nil), b.delays...), end: b.end}, nil
		}, stream)

		// A streaming race returns once every participant has exited.
		for _, name := range stressParticipantNames {
			require.Equal(t, 1, policy.doneCounts()[name], "done calls for %s", name)
		}

		if err != nil {
			return
		}

		// Without an error, the results are all of a single participant's results, in order.
		got := resultIDs(stream.Results())
		matched := false
		for _, name := range stressParticipantNames {
			b := behaviors[name]
			if b.openErr == nil && b.end == nil && assert.ObjectsAreEqual(resultIDs(lsResults(name, b.results)), got) {
				matched = true
			}
		}
		require.True(t, matched, "results %v come from no single participant", got)
	})
}

func TestRaceStressUnary(t *testing.T) {
	strategies := []secondaryStrategy{
		hedgedStrategy{},
		preferSecondaryStrategy{deadline: time.Millisecond},
	}

	rapid.Check(t, func(t *rapid.T) {
		delays := make(map[string]time.Duration, len(stressParticipantNames))
		fails := make(map[string]bool, len(stressParticipantNames))
		allFail := true
		for _, name := range stressParticipantNames {
			delays[name] = time.Duration(rapid.IntRange(0, 300).Draw(t, name+" delay (µs)")) * time.Microsecond
			fails[name] = rapid.Bool().Draw(t, name+" fails")
			allFail = allFail && fails[name]
		}

		stats, err := newRaceStats(time.Millisecond)
		require.NoError(t, err)

		var policy *doneCountingPolicy
		strategy := doneCountingStrategy{
			secondaryStrategy: rapid.SampledFrom(strategies).Draw(t, "strategy"),
			policy:            &policy,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := runUnaryRace(ctx, raceRequest{
			reqKey:       "check",
			participants: stressParticipants(),
			stats:        stats,
		}, strategy, func(ctx context.Context, c ClusterClient) (*v1.DispatchCheckResponse, error) {
			name := c.(raceClient).name
			select {
			case <-time.After(delays[name]):
			case <-ctx.Done():
				return &v1.DispatchCheckResponse{}, ctx.Err()
			}
			if fails[name] {
				return &v1.DispatchCheckResponse{}, errors.New(name + " failed")
			}
			return &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 1}}, nil
		})

		if allFail {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, uint32(1), resp.Metadata.DispatchCount)
		}

		// A unary race may return before its losers exit: they stop once the caller cancels.
		cancel()
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			counts := policy.doneCounts()
			for _, name := range stressParticipantNames {
				assert.Equal(c, 1, counts[name], "done calls for %s", name)
			}
		}, 5*time.Second, time.Millisecond)
	})
}
