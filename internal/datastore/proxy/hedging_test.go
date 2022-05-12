package proxy

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/benbjohnson/clock"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/datastore/proxy/proxy_test"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var (
	slowQueryTime  = 5 * time.Millisecond
	maxSampleCount = uint64(1_000_000)
	quantile       = 0.95

	errKnown             = errors.New("known error")
	errAnotherKnown      = errors.New("another known error")
	nsKnown              = "namespace_name"
	revisionKnown        = decimal.NewFromInt(1)
	anotherRevisionKnown = decimal.NewFromInt(2)

	emptyIterator = datastore.NewSliceRelationshipIterator(nil)
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
			"ReadNamespace",
			true,
			[]interface{}{nsKnown},
			[]interface{}{&core.NamespaceDefinition{}, revisionKnown, errKnown},
			[]interface{}{&core.NamespaceDefinition{}, anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				_, rev, err := proxy.SnapshotReader(datastore.NoRevision).ReadNamespace(context.Background(), nsKnown)
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
					QueryRelationships(context.Background(), &v1.RelationshipFilter{})
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
					ReverseQueryRelationships(context.Background(), &v1.SubjectFilter{})
				if expectFirst {
					require.ErrorIs(errKnown, err)
				} else {
					require.ErrorIs(errAnotherKnown, err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.methodName, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/authzed/spicedb/internal/datastore/proxy.autoAdvance.func1"), goleak.IgnoreCurrent())
			mockTime := clock.NewMock()
			delegateDS := &proxy_test.MockDatastore{}
			proxy := newHedgingProxyWithTimeSource(
				delegateDS, slowQueryTime, maxSampleCount, quantile, mockTime,
			)

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
				WaitUntil(mockTime.After(3 * slowQueryTime)).
				Return(tc.firstCallResults...).
				Once()
			delegate.
				On(tc.methodName, tc.arguments...).
				Return(tc.secondCallResults...).
				Once()

			done := autoAdvance(mockTime, slowQueryTime, 5*slowQueryTime)

			tc.f(t, proxy, false)
			delegate.AssertExpectations(t)

			<-done

			delegate.
				On(tc.methodName, tc.arguments...).
				WaitUntil(mockTime.After(3 * slowQueryTime)).
				Return(tc.firstCallResults...).
				Once()
			delegate.
				On(tc.methodName, tc.arguments...).
				WaitUntil(mockTime.After(4 * slowQueryTime)).
				Return(tc.secondCallResults...).
				Once()

			autoAdvance(mockTime, slowQueryTime, 8*slowQueryTime)

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

	badInitialThreshold := func() {
		NewHedgingProxy(delegate, -1*time.Millisecond, maxSampleCount, quantile)
	}
	require.Panics(badInitialThreshold)

	maxRequestsTooSmall := func() {
		NewHedgingProxy(delegate, 10*time.Millisecond, 10, quantile)
	}
	require.Panics(maxRequestsTooSmall)

	invalidQuantileTooSmall := func() {
		NewHedgingProxy(delegate, 10*time.Millisecond, 1000, 0.0)
	}
	require.Panics(invalidQuantileTooSmall)

	invalidQuantileTooLarge := func() {
		NewHedgingProxy(delegate, 10*time.Millisecond, 1000, 1.0)
	}
	require.Panics(invalidQuantileTooLarge)
}

func TestDatastoreE2E(t *testing.T) {
	require := require.New(t)

	delegateDatastore := &proxy_test.MockDatastore{}
	delegateReader := &proxy_test.MockReader{}
	mockTime := clock.NewMock()

	proxy := newHedgingProxyWithTimeSource(
		delegateDatastore, slowQueryTime, maxSampleCount, quantile, mockTime,
	)

	expectedTuples := []*core.RelationTuple{
		{
			ObjectAndRelation: &core.ObjectAndRelation{
				Namespace: "test",
				ObjectId:  "test",
				Relation:  "test",
			},
			User: &core.User{
				UserOneof: &core.User_Userset{
					Userset: &core.ObjectAndRelation{
						Namespace: "test",
						ObjectId:  "test",
						Relation:  "test",
					},
				},
			},
		},
	}

	delegateDatastore.On("SnapshotReader", mock.Anything).Return(delegateReader)

	delegateReader.
		On("QueryRelationships", mock.Anything, mock.Anything).
		Return(datastore.NewSliceRelationshipIterator(expectedTuples), nil).
		WaitUntil(mockTime.After(2 * slowQueryTime)).
		Once()
	delegateReader.
		On("QueryRelationships", mock.Anything, mock.Anything).
		Return(datastore.NewSliceRelationshipIterator(expectedTuples), nil).
		Once()

	autoAdvance(mockTime, slowQueryTime/2, 2*slowQueryTime)

	it, err := proxy.SnapshotReader(revisionKnown).QueryRelationships(
		context.Background(), &v1.RelationshipFilter{
			ResourceType: "test",
		},
	)
	require.NoError(err)

	only := it.Next()
	require.Equal(expectedTuples[0], only)

	require.Nil(it.Next())
	require.NoError(it.Err())

	delegateDatastore.AssertExpectations(t)
	delegateReader.AssertExpectations(t)
}

func TestContextCancellation(t *testing.T) {
	require := require.New(t)

	delegate := &proxy_test.MockDatastore{}
	mockTime := clock.NewMock()
	proxy := newHedgingProxyWithTimeSource(
		delegate, slowQueryTime, maxSampleCount, quantile, mockTime,
	)

	delegate.
		On("HeadRevision", mock.Anything).
		Return(decimal.Zero, errKnown).
		WaitUntil(mockTime.After(500 * time.Microsecond)).
		Once()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		mockTime.Sleep(100 * time.Microsecond)
		cancel()
	}()

	autoAdvance(mockTime, 150*time.Microsecond, 1*time.Millisecond)

	_, err := proxy.HeadRevision(ctx)

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
