package remote

import (
	"context"
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
		strategyCount := testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", "primary_only_wins"))

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
		require.InDelta(t, strategyCount+1, testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", "primary_only_wins")), 0)
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
	_, err := withExternalStrategies([]secondary.Function{{Name: "mine", Strategy: basePolicyStrategy{}}})
	require.NoError(t, err)
	require.Len(t, registeredStrategies, before)
}
