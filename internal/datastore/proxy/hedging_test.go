package proxy

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	slowQueryTime  = 5 * time.Millisecond
	maxSampleCount = uint64(1_000_000)
	quantile       = 0.95

	errKnown             = errors.New("known error")
	errAnotherKnown      = errors.New("another known error")
	nsKnown              = "namespace_name"
	revisionKnown        = revisions.NewForTransactionID(1)
	anotherRevisionKnown = revisions.NewForTransactionID(2)

	emptyIterator = common.NewSliceRelationshipIterator(nil)
)

type testFunc func(t *testing.T, proxy datastore.Datastore, expectFirst bool)

func TestDatastoreRequestHedging(t *testing.T) {
	testCases := []struct {
		methodName        string
		useSnapshotReader bool
		arguments         []interface{}
		firstCallResults  []interface{}
		secondCallResults []interface{}
		f                 testFunc
	}{
		{
			"ReadNamespaceByName",
			true,
			[]interface{}{nsKnown},
			[]interface{}{&core.NamespaceDefinition{}, revisionKnown, errKnown},
			[]interface{}{&core.NamespaceDefinition{}, anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				_, rev, err := proxy.SnapshotReader(datastore.NoRevision).ReadNamespaceByName(context.Background(), nsKnown)
				require.ErrorIs(errKnown, err)
				if expectFirst {
					require.Equal(revisionKnown, rev)
				} else {
					require.Equal(anotherRevisionKnown, rev)
				}
			},
		},
		{
			"OptimizedRevision",
			false,
			[]interface{}{mock.Anything, mock.Anything},
			[]interface{}{revisionKnown, errKnown},
			[]interface{}{anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				rev, err := proxy.OptimizedRevision(context.Background())
				require.ErrorIs(errKnown, err)
				if expectFirst {
					require.Equal(revisionKnown, rev)
				} else {
					require.Equal(anotherRevisionKnown, rev)
				}
			},
		},
		{
			"HeadRevision",
			false,
			[]interface{}{mock.Anything},
			[]interface{}{revisionKnown, errKnown},
			[]interface{}{anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				rev, err := proxy.HeadRevision(context.Background())
				require.ErrorIs(errKnown, err)
				if expectFirst {
					require.Equal(revisionKnown, rev)
				} else {
					require.Equal(anotherRevisionKnown, rev)
				}
			},
		},
		{
			"QueryRelationships",
			true,
			[]interface{}{mock.Anything, mock.Anything},
			[]interface{}{emptyIterator, errKnown},
			[]interface{}{emptyIterator, errAnotherKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				_, err := proxy.
					SnapshotReader(datastore.NoRevision).
					QueryRelationships(context.Background(), datastore.RelationshipsFilter{})
				if expectFirst {
					require.ErrorIs(errKnown, err)
				} else {
					require.ErrorIs(errAnotherKnown, err)
				}
			},
		},
		{
			"ReverseQueryRelationships",
			true,
			[]interface{}{mock.Anything, mock.Anything},
			[]interface{}{emptyIterator, errKnown},
			[]interface{}{emptyIterator, errAnotherKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				_, err := proxy.
					SnapshotReader(datastore.NoRevision).
					ReverseQueryRelationships(context.Background(), datastore.SubjectsFilter{})
				if expectFirst {
					require.ErrorIs(errKnown, err)
				} else {
					require.ErrorIs(errAnotherKnown, err)
				}
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.methodName, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/authzed/spicedb/internal/datastore/proxy.autoAdvance.func1"), goleak.IgnoreCurrent())
			mockTime := clock.NewMock()
			delegateDS := &proxy_test.MockDatastore{}
			proxy, err := newHedgingProxyWithTimeSource(
				delegateDS, slowQueryTime, maxSampleCount, quantile, mockTime,
			)
			require.NoError(t, err)

			delegate := &delegateDS.Mock

			if tc.useSnapshotReader {
				readerMock := &proxy_test.MockReader{}
				delegate.On("SnapshotReader", mock.Anything).Return(readerMock)
				delegate = &readerMock.Mock
			}

			delegate.
				On(tc.methodName, tc.arguments...).
				Return(tc.firstCallResults...).
				Once()

			tc.f(t, proxy, true)
			delegate.AssertExpectations(t)

			delegate.
				On(tc.methodName, tc.arguments...).
				WaitUntil(mockTime.After(5 * slowQueryTime)).
				Return(tc.firstCallResults...).
				Once()
			delegate.
				On(tc.methodName, tc.arguments...).
				Return(tc.secondCallResults...).
				Once()

			done := autoAdvance(mockTime, slowQueryTime, 6*slowQueryTime)

			tc.f(t, proxy, false)
			delegate.AssertExpectations(t)

			<-done

			delegate.
				On(tc.methodName, tc.arguments...).
				WaitUntil(mockTime.After(7 * slowQueryTime)).
				Return(tc.firstCallResults...).
				Once()
			delegate.
				On(tc.methodName, tc.arguments...).
				WaitUntil(mockTime.After(8 * slowQueryTime)).
				Return(tc.secondCallResults...).
				Once()

			autoAdvance(mockTime, slowQueryTime, 9*slowQueryTime)

			tc.f(t, proxy, true)
			delegate.AssertExpectations(t)
		})
	}
}

func TestDigestRollover(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	mockTime := clock.NewMock()
	proxy := &hedgingProxy{
		Datastore:          delegate,
		headRevisionHedger: newHedger(mockTime, slowQueryTime, 100, 0.9999999999),
	}

	// Simulate a request that starts off fast enough
	delegate.
		On("HeadRevision", mock.Anything).
		WaitUntil(mockTime.After(slowQueryTime/2)).
		Return(datastore.NoRevision, errKnown).
		Once()

	done := autoAdvance(mockTime, slowQueryTime/4, slowQueryTime*2)

	_, err := proxy.HeadRevision(context.Background())
	require.ErrorIs(err, errKnown)
	delegate.AssertExpectations(t)

	<-done

	for i := time.Duration(0); i < 205; i++ {
		delegate.
			On("HeadRevision", mock.Anything).
			WaitUntil(mockTime.After(i*100*time.Microsecond)).
			Return(datastore.NoRevision, errKnown).
			Once()
	}

	done = autoAdvance(mockTime, 100*time.Microsecond, 205*100*time.Microsecond)

	for i := 0; i < 200; i++ {
		_, err := proxy.HeadRevision(context.Background())
		require.ErrorIs(err, errKnown)
	}
	<-done

	delegate.ExpectedCalls = nil

	// Now run a request which previously would have been fast enough and ensure that the
	// request is hedged
	delegate.
		On("HeadRevision", mock.Anything).
		WaitUntil(mockTime.After(slowQueryTime/2)).
		Return(datastore.NoRevision, errKnown).
		Once()
	delegate.
		On("HeadRevision", mock.Anything).
		WaitUntil(mockTime.After(100*time.Microsecond)).
		Return(datastore.NoRevision, errAnotherKnown).
		Once()

	autoAdvance(mockTime, 100*time.Microsecond, slowQueryTime)

	_, err = proxy.HeadRevision(context.Background())
	require.ErrorIs(errAnotherKnown, err)
	delegate.AssertExpectations(t)
}

func TestBadArgs(t *testing.T) {
	require := require.New(t)
	delegate := &proxy_test.MockDatastore{}

	_, err := NewHedgingProxy(delegate, -1*time.Millisecond, maxSampleCount, quantile)
	require.Error(err)

	_, err = NewHedgingProxy(delegate, 10*time.Millisecond, 10, quantile)
	require.Error(err)

	_, err = NewHedgingProxy(delegate, 10*time.Millisecond, 1000, 0.0)
	require.Error(err)

	_, err = NewHedgingProxy(delegate, 10*time.Millisecond, 1000, 1.0)
	require.Error(err)
}

func TestDatastoreE2E(t *testing.T) {
	require := require.New(t)

	delegateDatastore := &proxy_test.MockDatastore{}
	delegateReader := &proxy_test.MockReader{}
	mockTime := clock.NewMock()

	proxy, err := newHedgingProxyWithTimeSource(
		delegateDatastore, slowQueryTime, maxSampleCount, quantile, mockTime,
	)
	require.NoError(err)

	expectedRels := []tuple.Relationship{
		tuple.MustParse("document:first#viewer@user:bob"),
		tuple.MustParse("document:second#viewer@user:alice"),
	}

	delegateDatastore.On("SnapshotReader", mock.Anything).Return(delegateReader)

	delegateReader.
		On("QueryRelationships", mock.Anything, mock.Anything).
		Return(common.NewSliceRelationshipIterator(expectedRels), nil).
		WaitUntil(mockTime.After(2 * slowQueryTime)).
		Once()
	delegateReader.
		On("QueryRelationships", mock.Anything, mock.Anything).
		Return(common.NewSliceRelationshipIterator(expectedRels), nil).
		Once()

	autoAdvance(mockTime, slowQueryTime/2, 2*slowQueryTime)

	it, err := proxy.SnapshotReader(revisionKnown).QueryRelationships(
		context.Background(), datastore.RelationshipsFilter{
			OptionalResourceType: "document",
		},
	)
	require.NoError(err)

	slice, err := datastore.IteratorToSlice(it)
	require.NoError(err)
	require.Equal(expectedRels, slice)

	delegateDatastore.AssertExpectations(t)
	delegateReader.AssertExpectations(t)
}

func TestContextCancellation(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	mockTime := clock.NewMock()
	proxy, err := newHedgingProxyWithTimeSource(
		delegate, slowQueryTime, maxSampleCount, quantile, mockTime,
	)
	require.NoError(err)

	delegate.
		On("HeadRevision", mock.Anything).
		Return(datastore.NoRevision, errKnown).
		WaitUntil(mockTime.After(500 * time.Microsecond)).
		Once()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		mockTime.Sleep(100 * time.Microsecond)
		cancel()
	}()

	autoAdvance(mockTime, 150*time.Microsecond, 1*time.Millisecond)

	_, err = proxy.HeadRevision(ctx)
	require.Error(err)
}

func autoAdvance(timeSource *clock.Mock, step time.Duration, totalTime time.Duration) <-chan time.Time {
	done := make(chan time.Time)

	go func() {
		defer close(done)

		endTime := timeSource.Now().Add(totalTime)

		runtime.Gosched()
		for now := timeSource.Now(); now.Before(endTime); now = timeSource.Now() {
			timeSource.Add(step)
			runtime.Gosched()
		}

		done <- timeSource.Now()
	}()

	return done
}
