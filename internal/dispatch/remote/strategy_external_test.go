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

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/dispatch/secondary"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// primaryOnlyWins is an external strategy whose policy lets only the primary claim the race, and
// records what the engine reports to it.
type primaryOnlyWins struct {
	recorder *policyRecorder
}

func (s primaryOnlyWins) NewPolicy(race secondary.Race) secondary.Policy {
	s.recorder.setRace(race)
	return &primaryOnlyWinsPolicy{recorder: s.recorder}
}

type primaryOnlyWinsPolicy struct {
	secondary.BasePolicy
	recorder *policyRecorder
}

func (p *primaryOnlyWinsPolicy) ReadyToClaim(ctx context.Context, pt secondary.Participant, received int, complete bool) bool {
	return pt.Primary && p.BasePolicy.ReadyToClaim(ctx, pt, received, complete)
}

func (p *primaryOnlyWinsPolicy) ObserveResult(pt secondary.Participant, result any) {
	p.recorder.observed(pt, result)
}

func (p *primaryOnlyWinsPolicy) Done(pt secondary.Participant, err error) {
	p.recorder.done(pt, err)
}

type policyRecorder struct {
	lock    sync.Mutex
	race    secondary.Race    // GUARDED_BY(lock)
	results map[string]uint32 // GUARDED_BY(lock): the dispatch count of each participant's result
	dones   map[string]error  // GUARDED_BY(lock)
}

func newPolicyRecorder() *policyRecorder {
	return &policyRecorder{
		results: map[string]uint32{},
		dones:   map[string]error{},
	}
}

func (r *policyRecorder) setRace(race secondary.Race) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.race = race
}

func (r *policyRecorder) observed(p secondary.Participant, result any) {
	r.lock.Lock()
	defer r.lock.Unlock()
	resp := result.(*v1.DispatchCheckResponse)
	r.results[p.Name] = resp.Metadata.DispatchCount
}

func (r *policyRecorder) done(p secondary.Participant, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.dones[p.Name] = err
}

func primaryOnlyWinsFunction(recorder *policyRecorder) secondary.Function {
	return secondary.Function{
		Name: "primary_only_wins",
		New:  func() (secondary.Strategy, error) { return primaryOnlyWins{recorder: recorder}, nil },
	}
}

func newExternalStrategyDispatcher(t *testing.T, primary, secondaryClient *gatedCheckClient, expr string, functions ...secondary.Function) dispatch.Dispatcher {
	t.Helper()

	parsed, err := ParseDispatchExpression("check", expr, functions...)
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
	return d
}

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

		recorder := newPolicyRecorder()
		d := newExternalStrategyDispatcher(t, primary, secondaryClient, "primary_only_wins(['secondary'])", primaryOnlyWinsFunction(recorder))
		strategyCount := testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", "primary_only_wins"))

		req := testCheckRequest()
		resp, err := d.DispatchCheck(t.Context(), req)
		require.NoError(t, err)
		require.Equal(t, fromPrimary, resp.Metadata.DispatchCount)
		synctest.Wait() // the secondary's Done may be reported after the race returns

		recorder.lock.Lock()
		defer recorder.lock.Unlock()
		require.Equal(t, "check", recorder.race.Method)
		require.Same(t, req, recorder.race.Request)
		require.Equal(t, "sometype", recorder.race.Resource.ObjectType)
		require.Equal(t, []secondary.Participant{
			{Name: secondary.PrimaryName, Primary: true},
			{Name: "secondary"},
		}, recorder.race.Participants)
		require.Equal(t, map[string]uint32{secondary.PrimaryName: fromPrimary, "secondary": fromSecondary}, recorder.results)
		require.Len(t, recorder.dones, 2)
		require.InDelta(t, strategyCount+1, testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", "primary_only_wins")), 0)
	})
}

func TestExternalStrategyWithDuration(t *testing.T) {
	var got time.Duration
	fn := secondary.Function{
		Name: "with_timeout",
		NewWithDuration: func(d time.Duration) (secondary.Strategy, error) {
			got = d
			return primaryOnlyWins{recorder: newPolicyRecorder()}, nil
		},
	}

	parsed, err := ParseDispatchExpression("check", "with_timeout(['secondary'], duration('5s'))", fn)
	require.NoError(t, err)
	plan, err := runDispatchExpr(parsed, testCheckRequest())
	require.NoError(t, err)
	require.Equal(t, []string{"secondary"}, plan.secondaries)
	require.Equal(t, "with_timeout", plan.strategy.name())
	require.Equal(t, 5*time.Second, got)

	_, err = ParseDispatchExpression("check", "with_timeout(['secondary'])", fn)
	require.Error(t, err, "only the duration form is registered")

	_, err = ParseDispatchExpression("check", "with_timeout(['secondary'], duration('0s'))", fn)
	require.ErrorContains(t, err, "must be positive")
}

func TestExternalStrategyConstructorErrorFallsBackToPrimary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		primary := &gatedCheckClient{resp: &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: fromPrimary}}}
		secondaryClient := &gatedCheckClient{resp: &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: fromSecondary}}}
		failing := secondary.Function{
			Name: "failing",
			New:  func() (secondary.Strategy, error) { return nil, errors.New("not configured") },
		}
		d := newExternalStrategyDispatcher(t, primary, secondaryClient, "failing(['secondary'])", failing)
		evalErrors := testutil.ToFloat64(exprEvalErrorsCounter.WithLabelValues("check"))

		resp, err := d.DispatchCheck(t.Context(), testCheckRequest())
		require.NoError(t, err)
		require.Equal(t, fromPrimary, resp.Metadata.DispatchCount)
		require.Equal(t, int32(0), secondaryClient.calls.Load())
		require.InDelta(t, evalErrors+1, testutil.ToFloat64(exprEvalErrorsCounter.WithLabelValues("check")), 0)
	})
}

func TestExternalStrategyObserverOnlyWhenImplemented(t *testing.T) {
	r := newTestRaceRequest(t, time.Millisecond, testPrimary, testSecondary)

	observing := externalStrategy{function: "observing", strategy: primaryOnlyWins{recorder: newPolicyRecorder()}}
	_, ok := observing.newPolicy(r).(resultObserver)
	require.True(t, ok)

	plain := externalStrategy{function: "plain", strategy: basePolicyStrategy{}}
	_, ok = plain.newPolicy(r).(resultObserver)
	require.False(t, ok, "a policy that does not observe results must not be given them")
}

type basePolicyStrategy struct{}

func (basePolicyStrategy) NewPolicy(secondary.Race) secondary.Policy { return secondary.BasePolicy{} }

func TestExternalStrategyRegistrationErrors(t *testing.T) {
	newStrategy := func() (secondary.Strategy, error) { return basePolicyStrategy{}, nil }

	tests := []struct {
		name      string
		method    string
		expr      string
		functions []secondary.Function
		wantErr   string
	}{
		{
			name:      "built-in name",
			expr:      "['secondary']",
			functions: []secondary.Function{{Name: "hedged", New: newStrategy}},
			wantErr:   `"hedged" is already defined`,
		},
		{
			name:      "registered twice",
			expr:      "['secondary']",
			functions: []secondary.Function{{Name: "mine", New: newStrategy}, {Name: "mine", New: newStrategy}},
			wantErr:   `"mine" is already defined`,
		},
		{
			name:      "invalid name",
			expr:      "['secondary']",
			functions: []secondary.Function{{Name: "my-strategy", New: newStrategy}},
			wantErr:   "not a valid CEL identifier",
		},
		{
			name:      "no constructor",
			expr:      "['secondary']",
			functions: []secondary.Function{{Name: "mine"}},
			wantErr:   "neither New nor NewWithDuration",
		},
		{
			name:      "unsupported method",
			method:    "lookupresources",
			expr:      "request.resource_relation.namespace == 'document' ? mine(['secondary']) : prefer_primary()",
			functions: []secondary.Function{{Name: "mine", Methods: []string{"check"}, New: newStrategy}},
			wantErr:   "mine: not supported in `lookupresources` expressions; supported in: check",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			method := tc.method
			if method == "" {
				method = "check"
			}
			_, err := ParseDispatchExpression(method, tc.expr, tc.functions...)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestExternalStrategyAllowedMethod(t *testing.T) {
	fn := secondary.Function{
		Name:    "mine",
		Methods: []string{"check"},
		New:     func() (secondary.Strategy, error) { return basePolicyStrategy{}, nil },
	}
	_, err := ParseDispatchExpression("check", "mine(['secondary'])", fn)
	require.NoError(t, err)
}

func TestWithoutExternalStrategiesKeepsBuiltIns(t *testing.T) {
	strategies, err := withExternalStrategies(nil)
	require.NoError(t, err)
	require.Equal(t, registeredStrategies, strategies)

	before := len(registeredStrategies)
	_, err = withExternalStrategies([]secondary.Function{{Name: "mine", New: func() (secondary.Strategy, error) { return basePolicyStrategy{}, nil }}})
	require.NoError(t, err)
	require.Len(t, registeredStrategies, before, "registering must not modify the built-in list")
}
