package revisions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ccoveille/go-safecast"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/pkg/datastore"
)

type trackingRevisionFunction struct {
	mock.Mock
}

func (m *trackingRevisionFunction) optimizedRevisionFunc(_ context.Context) (datastore.Revision, time.Duration, error) {
	args := m.Called()
	return args.Get(0).(datastore.Revision), args.Get(1).(time.Duration), args.Error(2)
}

var (
	one   = NewForTransactionID(1)
	two   = NewForTransactionID(2)
	three = NewForTransactionID(3)
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			or := NewCachedOptimizedRevisions(tc.maxStaleness)
			mockTime := clock.NewMock()
			or.clockFn = mockTime
			mock := trackingRevisionFunction{}
			or.SetOptimizedRevisionFunc(mock.optimizedRevisionFunc)

			for _, callSpec := range tc.expectedCallResponses {
				mock.On("optimizedRevisionFunc").Return(callSpec.rev, callSpec.validFor, nil).Once()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			for _, expectedRevSet := range tc.expectedRevisions {
				awaitingRevisions := make(map[datastore.Revision]struct{}, len(expectedRevSet))
				for _, rev := range expectedRevSet {
					awaitingRevisions[rev] = struct{}{}
				}

				require.Eventually(func() bool {
					revision, err := or.OptimizedRevision(ctx)
					require.NoError(err)
					printableRevSet := lo.Map(expectedRevSet, func(val datastore.Revision, index int) string {
						return val.String()
					})
					require.Contains(expectedRevSet, revision, "must return the proper revision, allowed set %#v, received %s", printableRevSet, revision)

					delete(awaitingRevisions, revision)
					return len(awaitingRevisions) == 0
				}, 1*time.Second, 1*time.Microsecond)

				mockTime.Add(5 * time.Millisecond)
			}

			mock.AssertExpectations(t)
		})
	}
}

func TestOptimizedRevisionCacheSingleFlight(t *testing.T) {
	require := require.New(t)

	or := NewCachedOptimizedRevisions(0)
	mock := trackingRevisionFunction{}
	or.SetOptimizedRevisionFunc(mock.optimizedRevisionFunc)

	mock.
		On("optimizedRevisionFunc").
		Return(one, time.Duration(0), nil).
		After(50 * time.Millisecond).
		Once()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	g := errgroup.Group{}
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			revision, err := or.OptimizedRevision(ctx)
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

	mock.AssertExpectations(t)
}

func BenchmarkOptimizedRevisions(b *testing.B) {
	b.SetParallelism(1024)

	quantization := 1 * time.Millisecond
	or := NewCachedOptimizedRevisions(quantization)

	or.SetOptimizedRevisionFunc(func(ctx context.Context) (datastore.Revision, time.Duration, error) {
		nowNS := time.Now().UnixNano()
		validForNS := nowNS % quantization.Nanoseconds()
		roundedNS := nowNS - validForNS
		// This should be non-negative.
		uintRoundedNs, _ := safecast.ToUint64(roundedNS)
		rev := NewForTransactionID(uintRoundedNs)
		return rev, time.Duration(validForNS) * time.Nanosecond, nil
	})

	ctx := context.Background()
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

	or := NewCachedOptimizedRevisions(0)
	mock := trackingRevisionFunction{}
	or.SetOptimizedRevisionFunc(mock.optimizedRevisionFunc)

	mock.
		On("optimizedRevisionFunc").
		Return(one, time.Duration(0), errors.New("fail")).
		Once()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_, err := or.OptimizedRevision(ctx)
	req.Error(err)
	mock.AssertExpectations(t)
}
