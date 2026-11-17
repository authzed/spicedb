package remote

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	testPrimary   = participant{name: primaryDispatcher, primary: true}
	testSecondary = participant{name: "secondary", maxPrimaryDelay: 5 * time.Millisecond}
	errSecondary  = errors.New("secondary failed")
)

func newTestRaceRequest(t *testing.T, startingDelay time.Duration, participants ...participant) raceRequest {
	t.Helper()
	stats, err := newRaceStats(startingDelay)
	require.NoError(t, err)
	return raceRequest{
		reqKey:       "check",
		resource:     tuple.RR("document", "view"),
		subject:      tuple.RR("user", "..."),
		participants: participants,
		stats:        stats,
	}
}

// warmDigest fills the request's digest so its quantiles equal latency.
func warmDigest(t *testing.T, r raceRequest, latency time.Duration) {
	t.Helper()
	for range 100 {
		r.stats.secondaryDigests[r.reqKey].addResultTime(t.Context(), latency)
	}
}

// startAsync calls policy.start in a goroutine and reports when it returns and how long it
// blocked (in the synctest bubble's fake time).
func startAsync(ctx context.Context, policy racePolicy, p participant) (<-chan time.Duration, func() bool) {
	waited := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		policy.start(ctx, p)
		waited <- time.Since(start)
	}()
	returned := func() bool {
		synctest.Wait()
		return len(waited) > 0
	}
	return waited, returned
}

func TestBasePolicyDefaults(t *testing.T) {
	var p basePolicy
	require.True(t, p.start(t.Context(), testPrimary))
	require.False(t, p.readyToClaim(testSecondary, 0, false))
	require.True(t, p.readyToClaim(testSecondary, 1, false))
	require.True(t, p.readyToClaim(testSecondary, 0, true))

	primaryErr := errors.New("primary failed")
	require.ErrorIs(t, p.noWinnerError(map[string]error{primaryDispatcher: primaryErr, "secondary": errSecondary}), primaryErr)
	require.NoError(t, p.noWinnerError(map[string]error{"secondary": errSecondary}))
}

func TestHedgedSecondariesStartImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRaceRequest(t, time.Hour, testPrimary, testSecondary)
		policy := hedgedStrategy{}.newPolicy(r)

		waited, _ := startAsync(t.Context(), policy, testSecondary)
		require.Zero(t, <-waited)
	})
}

func TestHedgedReleasesOnSecondaryError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRaceRequest(t, time.Hour, testPrimary, testSecondary)
		policy := hedgedStrategy{}.newPolicy(r)

		waited, returned := startAsync(t.Context(), policy, testPrimary)
		require.False(t, returned())

		policy.done(testSecondary, errSecondary)
		require.Zero(t, <-waited)
	})
}

func TestHedgedIgnoresSecondarySuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRaceRequest(t, time.Hour, testPrimary, testSecondary)
		policy := hedgedStrategy{}.newPolicy(r)

		waited, returned := startAsync(t.Context(), policy, testPrimary)
		require.False(t, returned())

		policy.done(testSecondary, nil)
		require.Equal(t, time.Hour, <-waited)
	})
}

func TestHedgedUsesLargestSecondaryDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fast := participant{name: "fast", maxPrimaryDelay: 5 * time.Millisecond}
		slow := participant{name: "slow", maxPrimaryDelay: 10 * time.Millisecond}
		r := newTestRaceRequest(t, time.Hour, testPrimary, fast, slow)
		warmDigest(t, r, 50*time.Millisecond)

		waited, _ := startAsync(t.Context(), hedgedStrategy{}.newPolicy(r), testPrimary)
		require.Equal(t, 10*time.Millisecond, <-waited)
	})
}

func TestHedgedMaxDelayOverridesSecondaryFlag(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		secondary := participant{name: "secondary", maxPrimaryDelay: 20 * time.Millisecond}
		r := newTestRaceRequest(t, time.Hour, testPrimary, secondary)
		warmDigest(t, r, 10*time.Millisecond)

		// Without an override, the wait is the p90 (10ms), below the flag's cap (20ms).
		waited, _ := startAsync(t.Context(), hedgedStrategy{}.newPolicy(r), testPrimary)
		require.Equal(t, 10*time.Millisecond, <-waited)

		// The override replaces the flag's cap.
		waited, _ = startAsync(t.Context(), hedgedStrategy{maxDelay: 3 * time.Millisecond}.newPolicy(r), testPrimary)
		require.Equal(t, 3*time.Millisecond, <-waited)
	})
}

func TestHedgedNoDelayForUnsupportedRelations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRaceRequest(t, time.Hour, testPrimary, testSecondary)
		r.stats.recordSecondaryError(testResourceRelationError{errors.New("unsupported"), "document", "view"})

		waited, _ := startAsync(t.Context(), hedgedStrategy{}.newPolicy(r), testPrimary)
		require.Zero(t, <-waited)
	})
}

func TestPreferSecondarySecondariesStartImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRaceRequest(t, time.Hour, testPrimary, testSecondary)
		policy := preferSecondaryStrategy{deadline: time.Hour}.newPolicy(r)

		waited, _ := startAsync(t.Context(), policy, testSecondary)
		require.Zero(t, <-waited)
	})
}

func TestPreferSecondaryHoldsUntilAllFail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		other := participant{name: "other"}
		r := newTestRaceRequest(t, time.Millisecond, testPrimary, testSecondary, other)
		policy := preferSecondaryStrategy{deadline: time.Hour}.newPolicy(r)
		released := testutil.ToFloat64(primaryReleaseCounter.WithLabelValues("check", "secondaries_failed"))

		waited, returned := startAsync(t.Context(), policy, testPrimary)

		policy.done(testSecondary, errSecondary)
		require.False(t, returned(), "one of two secondaries failed")

		policy.done(other, errSecondary)
		require.Zero(t, <-waited)
		require.InDelta(t, released+1, testutil.ToFloat64(primaryReleaseCounter.WithLabelValues("check", "secondaries_failed")), 0)
	})
}

func TestPreferSecondaryReleasesAtDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRaceRequest(t, time.Millisecond, testPrimary, testSecondary)
		policy := preferSecondaryStrategy{deadline: 50 * time.Millisecond}.newPolicy(r)
		released := testutil.ToFloat64(primaryReleaseCounter.WithLabelValues("check", "deadline"))

		waited, _ := startAsync(t.Context(), policy, testPrimary)
		require.Equal(t, 50*time.Millisecond, <-waited)
		require.InDelta(t, released+1, testutil.ToFloat64(primaryReleaseCounter.WithLabelValues("check", "deadline")), 0)
	})
}

func TestPreferSecondaryIgnoresSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRaceRequest(t, time.Millisecond, testPrimary, testSecondary)
		policy := preferSecondaryStrategy{deadline: 50 * time.Millisecond}.newPolicy(r)

		waited, returned := startAsync(t.Context(), policy, testPrimary)
		policy.done(testSecondary, nil)
		require.False(t, returned())
		require.Equal(t, 50*time.Millisecond, <-waited)
	})
}

func TestPreferSecondaryContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newTestRaceRequest(t, time.Millisecond, testPrimary, testSecondary)
		policy := preferSecondaryStrategy{deadline: time.Hour}.newPolicy(r)
		releasedByDeadline := testutil.ToFloat64(primaryReleaseCounter.WithLabelValues("check", "deadline"))
		releasedByFailures := testutil.ToFloat64(primaryReleaseCounter.WithLabelValues("check", "secondaries_failed"))

		ctx, cancel := context.WithCancel(t.Context())
		waited, returned := startAsync(ctx, policy, testPrimary)
		require.False(t, returned())

		cancel()
		require.Zero(t, <-waited)
		require.InDelta(t, releasedByDeadline, testutil.ToFloat64(primaryReleaseCounter.WithLabelValues("check", "deadline")), 0)
		require.InDelta(t, releasedByFailures, testutil.ToFloat64(primaryReleaseCounter.WithLabelValues("check", "secondaries_failed")), 0)
	})
}
