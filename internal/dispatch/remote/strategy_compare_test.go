package remote

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

const (
	member    = v1.ResourceCheckResult_MEMBER
	notMember = v1.ResourceCheckResult_NOT_MEMBER
	caveated  = v1.ResourceCheckResult_CAVEATED_MEMBER
)

// checkResponse builds a Check response marked with dispatchCount, with the given memberships.
func checkResponse(dispatchCount uint32, memberships map[string]v1.ResourceCheckResult_Membership) *v1.DispatchCheckResponse {
	results := make(map[string]*v1.ResourceCheckResult, len(memberships))
	for id, m := range memberships {
		results[id] = &v1.ResourceCheckResult{Membership: m}
	}
	return &v1.DispatchCheckResponse{
		Metadata:            &v1.ResponseMeta{DispatchCount: dispatchCount},
		ResultsByResourceId: results,
	}
}

func checkRequestFor(setting v1.DispatchCheckRequest_ResultsSetting, ids ...string) *v1.DispatchCheckRequest {
	req := testCheckRequest()
	req.ResourceIds = ids
	req.ResultsSetting = setting
	return req
}

func compareCount(secondary, outcome, code string) float64 {
	return testutil.ToFloat64(compareCounter.WithLabelValues("check", secondary, outcome, code))
}

// compareCounts snapshots compareCount for the given (secondary, outcome, code) triples, and
// returns a function reporting how much one of them has grown since.
func compareCounts(triples ...[3]string) func(secondary, outcome, code string) float64 {
	before := make(map[[3]string]float64, len(triples))
	for _, k := range triples {
		before[k] = compareCount(k[0], k[1], k[2])
	}
	return func(secondary, outcome, code string) float64 {
		return compareCount(secondary, outcome, code) - before[[3]string{secondary, outcome, code}]
	}
}

func TestDiffCheckResults(t *testing.T) {
	type m = map[string]v1.ResourceCheckResult_Membership
	all := v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS
	single := v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT

	tests := []struct {
		name      string
		setting   v1.DispatchCheckRequest_ResultsSetting
		ids       []string
		primary   m
		secondary m
		want      []membershipDiff
	}{
		{
			name:      "equal",
			setting:   all,
			ids:       []string{"a", "b"},
			primary:   m{"a": member, "b": notMember},
			secondary: m{"a": member, "b": notMember},
		},
		{
			name:      "missing resource is not a member",
			setting:   all,
			ids:       []string{"a", "b"},
			primary:   m{"a": member},
			secondary: m{"a": member, "b": notMember},
		},
		{
			name:      "membership differs",
			setting:   all,
			ids:       []string{"a", "b", "c"},
			primary:   m{"a": member, "c": member},
			secondary: m{"a": notMember, "b": member, "c": member},
			want: []membershipDiff{
				{resourceID: "a", primary: member, secondary: notMember},
				{resourceID: "b", primary: notMember, secondary: member},
			},
		},
		{
			name:      "caveated differs from member",
			setting:   all,
			ids:       []string{"a"},
			primary:   m{"a": caveated},
			secondary: m{"a": member},
			want:      []membershipDiff{{resourceID: "a", primary: caveated, secondary: member}},
		},
		{
			name:      "unrequested resources are ignored",
			setting:   all,
			ids:       []string{"a"},
			primary:   m{"a": member},
			secondary: m{"a": member, "z": member},
		},
		{
			name:      "repeated resource reported once",
			setting:   all,
			ids:       []string{"a", "a"},
			primary:   m{"a": member},
			secondary: m{},
			want:      []membershipDiff{{resourceID: "a", primary: member, secondary: notMember}},
		},
		{
			name:      "single result: both have a member, different ones",
			setting:   single,
			ids:       []string{"a", "b", "c"},
			primary:   m{"a": member},
			secondary: m{"b": member, "c": member},
		},
		{
			name:      "single result: only the primary has a member",
			setting:   single,
			ids:       []string{"a", "b"},
			primary:   m{"a": member},
			secondary: m{"a": notMember, "b": caveated},
			want:      []membershipDiff{{resourceID: "a", primary: member, secondary: notMember}},
		},
		{
			name:      "single result: neither has a member, compared in full",
			setting:   single,
			ids:       []string{"a", "b"},
			primary:   m{"a": caveated},
			secondary: m{"a": caveated, "b": caveated},
			want:      []membershipDiff{{resourceID: "b", primary: notMember, secondary: caveated}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := diffCheckResults(checkRequestFor(tc.setting, tc.ids...), checkResponse(0, tc.primary), checkResponse(0, tc.secondary))
			require.Equal(t, tc.want, got)
		})
	}
}

func TestCompareSecondariesNeverClaim(t *testing.T) {
	r := newTestRaceRequest(t, time.Millisecond, testPrimary, testSecondary)
	policy := compareStrategy{}.newPolicy(r)

	require.False(t, policy.readyToClaim(t.Context(), testSecondary, 1, false))
	require.False(t, policy.readyToClaim(t.Context(), testSecondary, 1, true))
	require.False(t, policy.readyToClaim(t.Context(), testPrimary, 1, false), "the primary claims only once complete")
}

func TestComparePrimaryWaitsForEverySecondary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := participant{name: "cmp-wait-first"}
		second := participant{name: "cmp-wait-second"}
		r := newTestRaceRequest(t, time.Millisecond, testPrimary, first, second)
		r.request = checkRequestFor(v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS, "a")
		policy := compareStrategy{}.newPolicy(r)
		observer := policy.(resultObserver)
		grown := compareCounts([3]string{first.name, compareMatch, ""}, [3]string{second.name, compareMismatch, ""})

		observer.observe(testPrimary, checkResponse(fromPrimary, map[string]v1.ResourceCheckResult_Membership{"a": member}))

		claimed := make(chan bool, 1)
		go func() { claimed <- policy.readyToClaim(t.Context(), testPrimary, 1, true) }()

		observer.observe(first, checkResponse(fromSecondary, map[string]v1.ResourceCheckResult_Membership{"a": member}))
		policy.done(first, nil)
		synctest.Wait()
		require.Empty(t, claimed, "one of two secondaries is still running")

		observer.observe(second, checkResponse(fromSecondary, map[string]v1.ResourceCheckResult_Membership{"a": notMember}))
		policy.done(second, nil)
		require.True(t, <-claimed)

		require.InDelta(t, 1, grown(first.name, compareMatch, ""), 0)
		require.InDelta(t, 1, grown(second.name, compareMismatch, ""), 0)
	})
}

func TestComparePrimaryClaimsWhenContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		slow := participant{name: "cmp-ctx-slow"}
		r := newTestRaceRequest(t, time.Millisecond, testPrimary, slow)
		r.request = checkRequestFor(v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS, "a")
		policy := compareStrategy{}.newPolicy(r)
		policy.(resultObserver).observe(testPrimary, checkResponse(fromPrimary, nil))
		grown := compareCounts([3]string{slow.name, compareSecondaryError, codes.Canceled.String()})

		ctx, cancel := context.WithCancel(t.Context())
		claimed := make(chan bool, 1)
		go func() { claimed <- policy.readyToClaim(ctx, testPrimary, 1, true) }()
		synctest.Wait()
		require.Empty(t, claimed)

		cancel()
		require.True(t, <-claimed)
		require.InDelta(t, 1, grown(slow.name, compareSecondaryError, codes.Canceled.String()), 0)
	})
}

// newCompareDispatcher creates a dispatcher whose Check expression compares the primary with the
// given secondaries.
func newCompareDispatcher(t *testing.T, primary *gatedCheckClient, secondaries map[string]*gatedCheckClient, expr string) dispatch.Dispatcher {
	t.Helper()

	parsed, err := ParseDispatchExpression("check", expr)
	require.NoError(t, err)

	secondaryDispatch := make(map[string]SecondaryDispatch, len(secondaries))
	for name, client := range secondaries {
		secondaryDispatch[name] = SecondaryDispatch{Name: name, Client: client, MaximumPrimaryHedgingDelay: time.Millisecond}
	}

	d, err := NewClusterDispatcher(primary, nil, ClusterDispatcherConfig{
		DispatchOverallTimeout:      time.Hour,
		SecondaryDispatch:           secondaryDispatch,
		SecondaryDispatchExprs:      map[string]*DispatchExpr{"check": parsed},
		StartingPrimaryHedgingDelay: time.Millisecond,
	})
	require.NoError(t, err)
	return d
}

func TestCompareCheckReturnsPrimaryAfterEverySecondary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The primary answers at once; the differing secondary only after a second.
		release := make(chan struct{})
		go func() {
			time.Sleep(time.Second)
			close(release)
		}()

		primary := &gatedCheckClient{resp: checkResponse(fromPrimary, map[string]v1.ResourceCheckResult_Membership{"foo": member})}
		agrees := &gatedCheckClient{resp: checkResponse(fromSecondary, map[string]v1.ResourceCheckResult_Membership{"foo": member})}
		differs := &gatedCheckClient{
			resp:  checkResponse(fromSecondary, map[string]v1.ResourceCheckResult_Membership{"foo": notMember}),
			after: release,
		}
		d := newCompareDispatcher(t, primary, map[string]*gatedCheckClient{
			"cmp-e2e-agrees":  agrees,
			"cmp-e2e-differs": differs,
		}, "compare(['cmp-e2e-agrees', 'cmp-e2e-differs'])")
		grown := compareCounts([3]string{"cmp-e2e-agrees", compareMatch, ""}, [3]string{"cmp-e2e-differs", compareMismatch, ""})
		strategyCount := testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", compareFunction))

		start := time.Now()
		resp, err := d.DispatchCheck(t.Context(), testCheckRequest())
		require.NoError(t, err)
		require.Equal(t, fromPrimary, resp.Metadata.DispatchCount)
		require.Equal(t, member, resp.ResultsByResourceId["foo"].Membership)
		require.GreaterOrEqual(t, time.Since(start), time.Second, "returned before the slow secondary answered")

		require.InDelta(t, 1, grown("cmp-e2e-agrees", compareMatch, ""), 0)
		require.InDelta(t, 1, grown("cmp-e2e-differs", compareMismatch, ""), 0)
		require.InDelta(t, strategyCount+1, testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", compareFunction)), 0)
	})
}

func TestCompareCheckSecondaryErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		primary := &gatedCheckClient{resp: checkResponse(fromPrimary, map[string]v1.ResourceCheckResult_Membership{"foo": member})}
		behind := &gatedCheckClient{err: status.Error(codes.Unavailable, "revision not available")}
		unsupported := &gatedCheckClient{err: status.Error(codes.FailedPrecondition, "permission not watched")}
		d := newCompareDispatcher(t, primary, map[string]*gatedCheckClient{
			"cmp-err-behind":      behind,
			"cmp-err-unsupported": unsupported,
		}, "compare(['cmp-err-behind', 'cmp-err-unsupported'])")
		grown := compareCounts(
			[3]string{"cmp-err-behind", compareUnavailable, codes.Unavailable.String()},
			[3]string{"cmp-err-unsupported", compareSecondaryError, codes.FailedPrecondition.String()},
		)

		resp, err := d.DispatchCheck(t.Context(), testCheckRequest())
		require.NoError(t, err)
		require.Equal(t, fromPrimary, resp.Metadata.DispatchCount)

		require.InDelta(t, 1, grown("cmp-err-behind", compareUnavailable, codes.Unavailable.String()), 0)
		require.InDelta(t, 1, grown("cmp-err-unsupported", compareSecondaryError, codes.FailedPrecondition.String()), 0)
	})
}

func TestCompareCheckReturnsPrimaryError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Unlike hedged, a secondary never answers for a failed primary.
		primary := &gatedCheckClient{err: status.Error(codes.Unavailable, "primary down")}
		secondary := &gatedCheckClient{resp: checkResponse(fromSecondary, map[string]v1.ResourceCheckResult_Membership{"foo": member})}
		d := newCompareDispatcher(t, primary, map[string]*gatedCheckClient{"cmp-primary-err": secondary}, "compare(['cmp-primary-err'])")
		grown := compareCounts([3]string{"cmp-primary-err", comparePrimaryError, codes.Unavailable.String()})

		_, err := d.DispatchCheck(t.Context(), testCheckRequest())
		require.Equal(t, codes.Unavailable, status.Code(err), "error: %v", err)
		require.InDelta(t, 1, grown("cmp-primary-err", comparePrimaryError, codes.Unavailable.String()), 0)
	})
}

func TestCompareExpressionIsCheckOnly(t *testing.T) {
	for _, method := range []string{"check", "lookupresources", "lookupsubjects"} {
		for _, expr := range []string{
			"compare(['a', 'b'])",
			"request.resource_relation.namespace == 'document' ? compare(['a']) : prefer_primary()",
		} {
			t.Run(method+"/"+expr, func(t *testing.T) {
				parsed, err := ParseDispatchExpression(method, expr)
				if method != "check" {
					require.ErrorContains(t, err, "compare: not supported for method")
					return
				}
				require.NoError(t, err)

				plan, err := runDispatchExpr(parsed, testCheckRequestFor("document"))
				require.NoError(t, err)
				require.IsType(t, compareStrategy{}, plan.strategy)
			})
		}
	}
}
