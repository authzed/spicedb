package proxy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
)

// fakeOptimizedRevisionDatastore is a minimal datastore whose OptimizedRevision
// is driven by a swappable function. Only OptimizedRevision is ever invoked by
// the proxy under test; all other Datastore methods are inherited from the nil
// embedded interface and will panic if called.
type fakeOptimizedRevisionDatastore struct {
	datastore.Datastore
	fn func(ctx context.Context) (datastore.Revision, time.Duration, string, error)
}

func (f *fakeOptimizedRevisionDatastore) OptimizedRevision(ctx context.Context) (datastore.Revision, time.Duration, string, error) {
	return f.fn(ctx)
}

func newOptimizedRevisionProxyForTest(d datastore.Datastore, maxStaleness time.Duration) *optimizedRevisionProxy {
	return NewOptimizedRevisionProxy(d, maxStaleness).(*optimizedRevisionProxy)
}

var (
	one   = revisions.NewForTransactionID(1)
	two   = revisions.NewForTransactionID(2)
	three = revisions.NewForTransactionID(3)
)

func cand(revs ...datastore.Revision) []datastore.Revision {
	return revs
}

func TestOptimizedRevisionCache(t *testing.T) {
	type revisionResponse struct {
		rev      datastore.Revision
		validFor time.Duration
	}

	testCases := []struct {
		name                  string
		maxStaleness          time.Duration
		expectedCallResponses []revisionResponse
		expectedRevisions     [][]datastore.Revision
	}{
		{
			"single request",
			0,
			[]revisionResponse{
				{one, 0},
			},
			[][]datastore.Revision{cand(one)},
		},
		{
			"simple no caching request",
			0,
			[]revisionResponse{
				{one, 0},
				{two, 0},
				{three, 0},
			},
			[][]datastore.Revision{cand(one), cand(two), cand(three)},
		},
		{
			"simple cached once",
			0,
			[]revisionResponse{
				{one, 7 * time.Millisecond},
				{two, 0},
			},
			[][]datastore.Revision{cand(one), cand(one), cand(two)},
		},
		{
			"cached by staleness",
			7 * time.Millisecond,
			[]revisionResponse{
				{one, 0},
				{two, 100 * time.Millisecond},
			},
			[][]datastore.Revision{cand(one), cand(one, two), cand(two), cand(two)},
		},
		{
			"cached by staleness and validity",
			2 * time.Millisecond,
			[]revisionResponse{
				{one, 4 * time.Millisecond},
				{two, 100 * time.Millisecond},
			},
			[][]datastore.Revision{cand(one), cand(one, two), cand(two)},
		},
		{
			"cached for a while",
			0,
			[]revisionResponse{
				{one, 28 * time.Millisecond},
				{two, 0},
			},
			[][]datastore.Revision{cand(one), cand(one), cand(one), cand(one), cand(one), cand(one), cand(two)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			mockDS := &proxy_test.MockDatastore{}
			or := newOptimizedRevisionProxyForTest(mockDS, tc.maxStaleness)
			mockTime := clock.NewMock()
			or.clock = mockTime

			for _, callSpec := range tc.expectedCallResponses {
				mockDS.On("OptimizedRevision").Return(callSpec.rev, callSpec.validFor, "", nil).Once()
			}

			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()

			for _, expectedRevSet := range tc.expectedRevisions {
				awaitingRevisions := make(map[datastore.Revision]struct{}, len(expectedRevSet))
				for _, rev := range expectedRevSet {
					awaitingRevisions[rev] = struct{}{}
				}

				require.Eventually(func() bool {
					revision, _, _, err := or.OptimizedRevision(ctx)
					require.NoError(err)
					printableRevSet := slicez.Map(expectedRevSet, func(val datastore.Revision) string {
						return val.String()
					})
					require.Contains(expectedRevSet, revision, "must return the proper revision, allowed set %#v, received %s", printableRevSet, revision)

					delete(awaitingRevisions, revision)
					return len(awaitingRevisions) == 0
				}, 1*time.Second, 1*time.Microsecond)

				mockTime.Add(5 * time.Millisecond)
			}

			mockDS.AssertExpectations(t)
		})
	}
}

func TestOptimizedRevisionCacheSingleFlight(t *testing.T) {
	require := require.New(t)

	mockDS := &proxy_test.MockDatastore{}
	or := newOptimizedRevisionProxyForTest(mockDS, 0)

	mockDS.
		On("OptimizedRevision").
		Return(one, time.Duration(0), "", nil).
		After(50 * time.Millisecond).
		Once()

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	g := errgroup.Group{}
	for range 10 {
		g.Go(func() error {
			revision, _, _, err := or.OptimizedRevision(ctx)
			if err != nil {
				return err
			}
			require.True(one.Equal(revision), "must return the proper revision %s != %s", one, revision)
			return nil
		})
		time.Sleep(1 * time.Millisecond)
	}

	err := g.Wait()
	require.NoError(err)

	mockDS.AssertExpectations(t)
}

func BenchmarkOptimizedRevisions(b *testing.B) {
	b.SetParallelism(1024)

	quantization := 1 * time.Millisecond
	fake := &fakeOptimizedRevisionDatastore{
		fn: func(_ context.Context) (datastore.Revision, time.Duration, string, error) {
			nowNS := time.Now().UnixNano()
			validForNS := nowNS % quantization.Nanoseconds()
			roundedNS := nowNS - validForNS
			// This should be non-negative.
			uintRoundedNs := safecast.RequireConvert[uint64](b, roundedNS)
			rev := revisions.NewForTransactionID(uintRoundedNs)
			return rev, time.Duration(validForNS) * time.Nanosecond, "", nil
		},
	}
	or := newOptimizedRevisionProxyForTest(fake, quantization)

	ctx := b.Context()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			if _, _, _, err := or.OptimizedRevision(ctx); err != nil {
				b.FailNow()
			}
		}
	})
}

func TestSingleFlightError(t *testing.T) {
	req := require.New(t)

	mockDS := &proxy_test.MockDatastore{}
	or := newOptimizedRevisionProxyForTest(mockDS, 0)

	// The shared attempt fails, and the direct retry (on the caller's context)
	// fails too, so the call returns an error. Both attempts invoke the function.
	mockDS.
		On("OptimizedRevision").
		Return(one, time.Duration(0), "", errors.New("fail")).
		Twice()

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	_, _, _, err := or.OptimizedRevision(ctx)
	req.Error(err)
	mockDS.AssertExpectations(t)
}

// TestOptimizedRevisionRetriesAfterSharedFailure ensures that when the shared,
// singleflighted computation fails, the request is retried directly so it can
// still succeed rather than surfacing the transient failure to the caller.
func TestOptimizedRevisionRetriesAfterSharedFailure(t *testing.T) {
	req := require.New(t)

	var calls atomic.Int32
	fake := &fakeOptimizedRevisionDatastore{
		fn: func(_ context.Context) (datastore.Revision, time.Duration, string, error) {
			// Fail the first (shared) attempt; succeed on the direct retry.
			if calls.Add(1) == 1 {
				return datastore.NoRevision, 0, "", errors.New("transient failure")
			}
			return one, 0, "", nil
		},
	}
	or := newOptimizedRevisionProxyForTest(fake, 0)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	res, _, _, err := or.OptimizedRevision(ctx)
	req.NoError(err)
	req.True(one.Equal(res), "expected the direct retry to succeed")
	req.Equal(int32(2), calls.Load(), "expected one shared attempt and one direct retry")
}

// TestOptimizedRevisionTimeout ensures that a datastore revision call that hangs
// cannot wedge OptimizedRevision indefinitely. The shared attempt is bounded by
// the (low) optimized-revision timeout, and the direct retry is bounded by the
// caller's deadline (or the fallback timeout for deadline-less callers), so the
// call always returns rather than blocking forever.
func TestOptimizedRevisionTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		req := require.New(t)

		fake := &fakeOptimizedRevisionDatastore{
			fn: func(ctx context.Context) (datastore.Revision, time.Duration, string, error) {
				// Simulate a hung datastore call that only unblocks when its context is
				// cancelled (as pgx does once a deadline is present on the context).
				<-ctx.Done()
				return datastore.NoRevision, 0, "", ctx.Err()
			},
		}
		or := newOptimizedRevisionProxyForTest(fake, 0)
		or.sharedTimeout = 10 * time.Millisecond
		or.fallbackTimeout = 50 * time.Millisecond

		var calls atomic.Int32
		baseFn := fake.fn
		fake.fn = func(ctx context.Context) (datastore.Revision, time.Duration, string, error) {
			calls.Add(1)
			return baseFn(ctx)
		}

		// The caller intentionally has no deadline of its own; the shared timeout and
		// the fallback timeout must together bound the call. If they fail to, every
		// goroutine in the bubble is durably blocked and synctest fails the test.
		_, _, _, err := or.OptimizedRevision(t.Context())
		req.Error(err, "hung revision call must return an error rather than block forever")

		// Both the shared attempt and the direct retry must have been attempted.
		req.GreaterOrEqual(calls.Load(), int32(2))

		// The singleflight key must have been released so a subsequent call computes a
		// fresh result rather than re-attaching to the dead one.
		fake.fn = func(_ context.Context) (datastore.Revision, time.Duration, string, error) {
			return one, 0, "", nil
		}

		res, _, _, err := or.OptimizedRevision(t.Context())
		req.NoError(err)
		req.True(one.Equal(res), "expected a fresh successful call after the hung call timed out")
	})
}
