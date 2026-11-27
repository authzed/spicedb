package remote

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/dispatch/secondary"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// primaryOnlyWins is an external strategy whose policy lets only the primary claim the race, and
// records the race and the results the engine reports to it.
type primaryOnlyWins struct {
	lock    sync.Mutex
	race    secondary.Race    // GUARDED_BY(lock)
	results map[string]uint32 // GUARDED_BY(lock): the dispatch count of each participant's result
}

func (s *primaryOnlyWins) NewPolicy(race secondary.Race) secondary.Policy {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.race = race
	s.results = map[string]uint32{}
	return primaryOnlyWinsPolicy{strategy: s}
}

type primaryOnlyWinsPolicy struct {
	secondary.BasePolicy
	strategy *primaryOnlyWins
}

func (p primaryOnlyWinsPolicy) ObserveResult(pt secondary.Participant, result any) {
	p.strategy.lock.Lock()
	defer p.strategy.lock.Unlock()
	p.strategy.results[pt.Name] = result.(*v1.DispatchCheckResponse).Metadata.DispatchCount
}

func (p primaryOnlyWinsPolicy) ReadyToClaim(ctx context.Context, pt secondary.Participant, received int, complete bool) bool {
	return pt.Primary && p.BasePolicy.ReadyToClaim(ctx, pt, received, complete)
}

type basePolicyStrategy struct{}

func (basePolicyStrategy) NewPolicy(secondary.Race) secondary.Policy { return secondary.BasePolicy{} }

func TestExternalStrategyRunsTheRace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The secondary answers first, but the external policy lets only the primary win.
		release := make(chan struct{})
		go func() {
			time.Sleep(time.Second)
			close(release)
		}()
		primary := &gatedCheckClient{
			resp:  &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: fromPrimary}},
			after: release,
		}
		secondaryClient := &gatedCheckClient{resp: &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: fromSecondary}}}

		strategy := &primaryOnlyWins{}
		parsed, err := ParseDispatchExpression("check", "primary_only_wins(['secondary'])",
			secondary.Function{Name: "primary_only_wins", Strategy: strategy})
		require.NoError(t, err)
		d, err := NewClusterDispatcher(primary, nil, ClusterDispatcherConfig{
			DispatchOverallTimeout: time.Hour,
			SecondaryDispatch: map[string]SecondaryDispatch{
				"secondary": {Name: "secondary", Client: secondaryClient, MaximumPrimaryHedgingDelay: time.Millisecond},
			},
			SecondaryDispatchExprs:      map[string]*DispatchExpr{"check": parsed},
			StartingPrimaryHedgingDelay: time.Millisecond,
		})
		require.NoError(t, err)
		strategyCount := testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", "primary_only_wins", "config"))

		req := testCheckRequest()
		resp, err := d.DispatchCheck(t.Context(), req)
		require.NoError(t, err)
		require.Equal(t, fromPrimary, resp.Metadata.DispatchCount)

		strategy.lock.Lock()
		defer strategy.lock.Unlock()
		require.Equal(t, "check", strategy.race.Method)
		require.Same(t, req, strategy.race.Request)
		require.Equal(t, []secondary.Participant{
			{Name: secondary.PrimaryName, Primary: true},
			{Name: "secondary"},
		}, strategy.race.Participants)
		require.Equal(t, map[string]uint32{secondary.PrimaryName: fromPrimary, "secondary": fromSecondary}, strategy.results)
		require.InDelta(t, strategyCount+1, testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", "primary_only_wins", "config")), 0)
	})
}

func TestExternalStrategyRegistration(t *testing.T) {
	mine := secondary.Function{Name: "mine", Methods: []string{"check"}, Strategy: basePolicyStrategy{}}

	tests := []struct {
		name      string
		method    string
		expr      string
		functions []secondary.Function
		wantErr   string
	}{
		{
			name:      "allowed method",
			method:    "check",
			expr:      "mine(['secondary'])",
			functions: []secondary.Function{mine},
		},
		{
			name:      "built-in name",
			method:    "check",
			expr:      "['secondary']",
			functions: []secondary.Function{{Name: "hedged", Strategy: basePolicyStrategy{}}},
			wantErr:   `"hedged" is already defined`,
		},
		{
			name:      "registered twice",
			method:    "check",
			expr:      "['secondary']",
			functions: []secondary.Function{mine, mine},
			wantErr:   `"mine" is already defined`,
		},
		{
			name:      "no strategy",
			method:    "check",
			expr:      "['secondary']",
			functions: []secondary.Function{{Name: "mine"}},
			wantErr:   `"mine" has no Strategy`,
		},
		{
			name:      "CEL function name",
			method:    "check",
			expr:      "['secondary']",
			functions: []secondary.Function{{Name: "duration", Strategy: basePolicyStrategy{}}},
			wantErr:   `"duration" is already defined`,
		},
		{
			name:      "CEL standard library name",
			method:    "check",
			expr:      "['secondary']",
			functions: []secondary.Function{{Name: "size", Strategy: basePolicyStrategy{}}},
			wantErr:   `"size" is already defined`,
		},
		{
			name:      "not an identifier",
			method:    "check",
			expr:      "['secondary']",
			functions: []secondary.Function{{Name: "my-strategy", Strategy: basePolicyStrategy{}}},
			wantErr:   `"my-strategy" cannot be called by that name`,
		},
		{
			name:      "reserved word",
			method:    "check",
			expr:      "['secondary']",
			functions: []secondary.Function{{Name: "while", Strategy: basePolicyStrategy{}}},
			wantErr:   `"while" cannot be called by that name`,
		},
		{
			name:      "unsupported method",
			method:    "lookupresources",
			expr:      "request.resource_relation.namespace == 'document' ? mine(['secondary']) : prefer_primary()",
			functions: []secondary.Function{mine},
			wantErr:   "mine: not supported in `lookupresources` expressions; supported in: check",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDispatchExpression(tc.method, tc.expr, tc.functions...)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestExternalStrategiesDoNotModifyBuiltIns(t *testing.T) {
	before := len(registeredStrategies)
	_, strategies, err := newDispatchEnv([]secondary.Function{{Name: "mine", Strategy: basePolicyStrategy{}}}, true)
	require.NoError(t, err)
	require.Len(t, strategies, before+1)
	require.Len(t, registeredStrategies, before)
}

func TestExternalStrategyObservesOnlyWhenImplemented(t *testing.T) {
	r := newTestRaceRequest(t, time.Millisecond, testPrimary, testSecondary)

	_, ok := externalStrategy{function: "observing", strategy: &primaryOnlyWins{}}.newPolicy(r).(resultObserver)
	require.True(t, ok)

	_, ok = externalStrategy{function: "plain", strategy: basePolicyStrategy{}}.newPolicy(r).(resultObserver)
	require.False(t, ok, "a policy that does not observe results must not be given them")
}

func TestExternalStrategyRejectsNonStringNames(t *testing.T) {
	// A list whose elements are only known at evaluation passes type checking, so the binding
	// must reject non-string names itself.
	parsed, err := ParseDispatchExpression("check", "mine(['secondary', dyn(1)])",
		secondary.Function{Name: "mine", Strategy: basePolicyStrategy{}})
	require.NoError(t, err)

	_, err = runDispatchExpr(parsed, testCheckRequest())
	require.ErrorContains(t, err, "mine: expected a list of strings")
}

// failingPolicy reports its own error when nobody won.
type failingPolicy struct {
	secondary.BasePolicy
	err error
}

func (p failingPolicy) NoWinnerError(map[string]error) error { return p.err }

func TestExternalPolicyDelegatesNoWinnerError(t *testing.T) {
	want := errors.New("chosen by the external policy")
	adapted := externalPolicy{policy: failingPolicy{err: want}}

	adapted.firstResult(testSecondary, time.Millisecond) // external policies are not told latencies
	require.ErrorIs(t, adapted.noWinnerError(map[string]error{primaryDispatcher: errors.New("primary failed")}), want)
}
