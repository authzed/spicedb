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
	fn func(ctx context.Context) (datastore.RevisionWithSchemaHashAndValidity, error)
}

func (f *fakeOptimizedRevisionDatastore) OptimizedRevision(ctx context.Context) (datastore.RevisionWithSchemaHashAndValidity, error) {
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
			// staleness lets a barely-expired candidate keep being served for a while
			"cached by staleness",
			7 * time.Millisecond,
			[]revisionResponse{
				{one, 1 * time.Millisecond},
				{two, 100 * time.Millisecond},
			},
			[][]datastore.Revision{cand(one), cand(one, two), cand(two), cand(two)},
		},
		{
			// a revision with no validity is accurate only when it is read, so staleness
			// must not keep it around: every call has to go back to the datastore
			"no validity is never cached, even with staleness",
			7 * time.Millisecond,
			[]revisionResponse{
				{one, 0},
				{two, 0},
				{three, 0},
			},
			[][]datastore.Revision{cand(one), cand(two), cand(three)},
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
				mockDS.On("OptimizedRevision").Return(datastore.RevisionWithSchemaHashAndValidity{Revision: callSpec.rev, ValidFor: callSpec.validFor}, nil).Once()
			}

			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()

			for _, expectedRevSet := range tc.expectedRevisions {
				awaitingRevisions := make(map[datastore.Revision]struct{}, len(expectedRevSet))
				for _, rev := range expectedRevSet {
					awaitingRevisions[rev] = struct{}{}
				}

				require.Eventually(func() bool {
					revisionResult, err := or.OptimizedRevision(ctx)
					revision := revisionResult.Revision
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
		Return(datastore.RevisionWithSchemaHashAndValidity{Revision: one}, nil).
		After(50 * time.Millisecond).
		Once()

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	g := errgroup.Group{}
	for range 10 {
		g.Go(func() error {
			revisionResult, err := or.OptimizedRevision(ctx)
			revision := revisionResult.Revision
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
		fn: func(_ context.Context) (datastore.RevisionWithSchemaHashAndValidity, error) {
			nowNS := time.Now().UnixNano()
			validForNS := nowNS % quantization.Nanoseconds()
			roundedNS := nowNS - validForNS
			// This should be non-negative.
			uintRoundedNs := safecast.RequireConvert[uint64](b, roundedNS)
			rev := revisions.NewForTransactionID(uintRoundedNs)
			return datastore.RevisionWithSchemaHashAndValidity{Revision: rev, ValidFor: time.Duration(validForNS) * time.Nanosecond}, nil
		},
	}
	or := newOptimizedRevisionProxyForTest(fake, quantization)

	ctx := b.Context()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			if _, err := or.OptimizedRevision(ctx); err != nil {
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
		Return(datastore.RevisionWithSchemaHashAndValidity{Revision: one}, errors.New("fail")).
		Twice()

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	_, err := or.OptimizedRevision(ctx)
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
		fn: func(_ context.Context) (datastore.RevisionWithSchemaHashAndValidity, error) {
			// Fail the first (shared) attempt; succeed on the direct retry.
			if calls.Add(1) == 1 {
				return datastore.RevisionWithSchemaHashAndValidity{}, errors.New("transient failure")
			}
			return datastore.RevisionWithSchemaHashAndValidity{Revision: one}, nil
		},
	}
	or := newOptimizedRevisionProxyForTest(fake, 0)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	resResult, err := or.OptimizedRevision(ctx)
	res := resResult.Revision
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
			fn: func(ctx context.Context) (datastore.RevisionWithSchemaHashAndValidity, error) {
				// Simulate a hung datastore call that only unblocks when its context is
				// cancelled (as pgx does once a deadline is present on the context).
				<-ctx.Done()
				return datastore.RevisionWithSchemaHashAndValidity{}, ctx.Err()
			},
		}
		or := newOptimizedRevisionProxyForTest(fake, 0)
		or.sharedTimeout = 10 * time.Millisecond
		or.fallbackTimeout = 50 * time.Millisecond

		var calls atomic.Int32
		baseFn := fake.fn
		fake.fn = func(ctx context.Context) (datastore.RevisionWithSchemaHashAndValidity, error) {
			calls.Add(1)
			return baseFn(ctx)
		}

		// The caller intentionally has no deadline of its own; the shared timeout and
		// the fallback timeout must together bound the call. If they fail to, every
		// goroutine in the bubble is durably blocked and synctest fails the test.
		_, err := or.OptimizedRevision(t.Context())
		req.Error(err, "hung revision call must return an error rather than block forever")

		// Both the shared attempt and the direct retry must have been attempted.
		req.GreaterOrEqual(calls.Load(), int32(2))

		// The singleflight key must have been released so a subsequent call computes a
		// fresh result rather than re-attaching to the dead one.
		fake.fn = func(_ context.Context) (datastore.RevisionWithSchemaHashAndValidity, error) {
			return datastore.RevisionWithSchemaHashAndValidity{Revision: one}, nil
		}

		resResult, err := or.OptimizedRevision(t.Context())
		res := resResult.Revision
		req.NoError(err)
		req.True(one.Equal(res), "expected a fresh successful call after the hung call timed out")
	})
}

// TestOptimizedRevisionDirectRetrySeesCallerDeadlineThroughSingleflightProxy
// stacks the proxies the way pkg/cmd/server does (singleflightProxy outermost)
// and checks that the direct retry after a failed shared attempt still runs
// under the caller's own deadline. If the outer proxy singleflighted
// OptimizedRevision, the caller's deadline would be stripped before reaching
// the cache and the retry would only be bounded by the fallback timeout.
func TestOptimizedRevisionDirectRetrySeesCallerDeadlineThroughSingleflightProxy(t *testing.T) {
	req := require.New(t)

	const callerTimeout = time.Second
	const fallbackTimeout = time.Hour

	var calls atomic.Int32
	var retryDeadline time.Time
	var retryHasDeadline bool
	fake := &fakeOptimizedRevisionDatastore{
		fn: func(ctx context.Context) (datastore.RevisionWithSchemaHashAndValidity, error) {
			if calls.Add(1) == 1 {
				return datastore.RevisionWithSchemaHashAndValidity{}, errors.New("transient failure")
			}
			retryDeadline, retryHasDeadline = ctx.Deadline()
			return datastore.RevisionWithSchemaHashAndValidity{Revision: one}, nil
		},
	}
	or := newOptimizedRevisionProxyForTest(fake, 0)
	or.fallbackTimeout = fallbackTimeout
	ds := NewSingleflightDatastoreProxy(or)

	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), callerTimeout)
	defer cancel()

	resResult, err := ds.OptimizedRevision(ctx)
	res := resResult.Revision
	req.NoError(err)
	req.True(one.Equal(res))
	req.Equal(int32(2), calls.Load(), "expected one shared attempt and one direct retry")

	req.True(retryHasDeadline, "direct retry must carry a deadline")
	req.WithinDuration(start.Add(callerTimeout), retryDeadline, 500*time.Millisecond,
		"direct retry must run under the caller's deadline, not the fallback timeout")
}
