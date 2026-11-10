package querysignals_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/querysignals"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/signals/branchoutcome"
	"github.com/authzed/spicedb/pkg/query/signals/contextfield"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestCollectorFlushIsIdempotent(t *testing.T) {
	v := querysignalstest.View(t, querysignalstest.Owner(t, resultsize.New()), "a", 1)
	collector := querysignals.NewCollector(v, querysignals.CollectorOptions{})
	k := signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}
	collector.ObserveExecution(query.ExecutionObservation{Invocation: 1, Key: k, Count: 7, Status: signal.Complete, Started: time.Now()})
	collector.Flush()
	collector.Flush()
	e, err := signal.Lookup(v, resultsize.Ref, k)
	require.NoError(t, err)
	require.Equal(t, uint64(1), e.Value.Samples)
	require.InDelta(t, 7.0, e.Value.Mean, 1e-9)
}

func TestContextFingerprintOrderAndTypes(t *testing.T) {
	a, err := querysignals.Fingerprint(map[string]any{"a": true, "b": float64(1)})
	require.NoError(t, err)
	b, err := querysignals.Fingerprint(map[string]any{"b": float64(1), "a": true})
	require.NoError(t, err)
	require.Equal(t, a, b)
	null, _ := querysignals.Fingerprint(nil)
	number, _ := querysignals.Fingerprint(float64(1))
	text, _ := querysignals.Fingerprint("1")
	require.NotEqual(t, number, text)
	require.NotEqual(t, null, text)
}

func TestMissingNullAndPresentFieldObservations(t *testing.T) {
	v := querysignalstest.View(t, querysignalstest.Owner(t, contextfield.New()), "a", 1)
	querysignals.ObserveRequestContext(
		v,
		map[string]any{"null": nil, "present": true},
		querysignals.ContextOptions{Fields: []string{"missing", "null", "present"}},
	)

	for _, field := range []string{"missing", "null", "present"} {
		e, err := signal.Lookup(v, contextfield.Ref, field)
		require.NoError(t, err)
		require.Equal(t, uint64(1), e.Value.Samples)
		expected := uint64(0)

		if field == "missing" {
			expected = 1
		}

		require.Equal(t, expected, e.Value.Missing)
	}
}

func TestCollectorDropsTruncatedAndIncompleteObservations(t *testing.T) {
	v := querysignalstest.View(t, querysignalstest.Owner(t, resultsize.New()), "a", 1)
	k := signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}
	collector := querysignals.NewCollector(v, querysignals.CollectorOptions{MaxObservations: 2})

	for _, status := range []signal.Terminal{signal.ConsumerStopped, signal.Canceled, signal.Complete} {
		collector.ObserveExecution(query.ExecutionObservation{Key: k, Count: 1, Status: status, Started: time.Now()})
	}

	collector.Flush()
	require.Equal(t, uint64(1), collector.Dropped())
	e, err := signal.Lookup(v, resultsize.Ref, k)
	require.NoError(t, err)
	require.Equal(t, signal.Missing, e.Availability)
}

func TestContextTruncationIsNotMissingness(t *testing.T) {
	v := querysignalstest.View(t, querysignalstest.Owner(t, contextfield.New()), "a", 1)
	stats := querysignals.ObserveRequestContext(
		v,
		map[string]any{"a": "too large"},
		querysignals.ContextOptions{Fields: []string{"a", "a", "b", "c"}, MaxFields: 2, MaxBytes: 1},
	)
	require.Equal(t, 1, stats.Observed)
	require.Equal(t, 2, stats.Skipped)
	a, _ := signal.Lookup(v, contextfield.Ref, "a")
	require.Equal(t, signal.Missing, a.Availability)
	b, _ := signal.Lookup(v, contextfield.Ref, "b")
	require.Equal(t, uint64(1), b.Value.Missing)
	absent, _ := signal.Lookup(v, contextfield.Ref, "c")
	require.Equal(t, signal.Missing, absent.Availability)
}

func TestFailedBatchKeepsUnknownAttemptsSeparate(t *testing.T) {
	v := querysignalstest.View(t, querysignalstest.Owner(t, branchoutcome.New()), "a", 1)
	k := signal.ExecutionKey{Identity: "x", Operation: signal.Check, Mode: "many-subjects"}
	collector := querysignals.NewCollector(v, querysignals.CollectorOptions{})
	collector.ObserveExecution(query.ExecutionObservation{Key: k, Status: signal.Errored, BatchSize: 7, Started: time.Now()})
	collector.Flush()
	e, err := signal.Lookup(v, branchoutcome.Ref, k)
	require.NoError(t, err)
	require.Zero(t, e.Value.Attempted)
	require.Zero(t, e.Value.Completed)
	require.Equal(t, uint64(1), e.Value.UnresolvedBatches)
	require.Equal(t, uint64(7), e.Value.UnresolvedAttemptUpperBound)
}
