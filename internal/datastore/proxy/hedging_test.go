package proxy

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/benbjohnson/clock"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/test"
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
)

type testFunc func(t *testing.T, proxy datastore.Datastore, expectFirst bool)

func TestDatastoreRequestHedging(t *testing.T) {
	testCases := []struct {
		methodName        string
		arguments         []interface{}
		firstCallResults  []interface{}
		secondCallResults []interface{}
		f                 testFunc
	}{
		{
			"ReadNamespace",
			[]interface{}{mock.Anything, nsKnown, datastore.NoRevision},
			[]interface{}{&v0.NamespaceDefinition{}, revisionKnown, errKnown},
			[]interface{}{&v0.NamespaceDefinition{}, anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				_, rev, err := proxy.ReadNamespace(context.Background(), nsKnown, datastore.NoRevision)
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
			[]interface{}{mock.Anything},
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
	}

	for _, tc := range testCases {
		t.Run(tc.methodName, func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/authzed/spicedb/internal/datastore/proxy.autoAdvance.func1"), goleak.IgnoreCurrent())
			mockTime := clock.NewMock()
			delegate := &test.MockedDatastore{}
			proxy := newHedgingProxyWithTimeSource(
				delegate, slowQueryTime, maxSampleCount, quantile, mockTime,
			)

			delegate.
				On(tc.methodName, tc.arguments...).
				Return(tc.firstCallResults...).
				Once()

			tc.f(t, proxy, true)
			delegate.AssertExpectations(t)

			delegate.
				On(tc.methodName, tc.arguments...).
				WaitUntil(mockTime.After(2 * slowQueryTime)).
				Return(tc.firstCallResults...).
				Once()
			delegate.
				On(tc.methodName, tc.arguments...).
				Return(tc.secondCallResults...).
				Once()

			done := autoAdvance(mockTime, slowQueryTime, 3*slowQueryTime)

			tc.f(t, proxy, false)
			delegate.AssertExpectations(t)

			<-done

			delegate.
				On(tc.methodName, tc.arguments...).
				WaitUntil(mockTime.After(2 * slowQueryTime)).
				Return(tc.firstCallResults...).
				Once()
			delegate.
				On(tc.methodName, tc.arguments...).
				WaitUntil(mockTime.After(3 * slowQueryTime)).
				Return(tc.secondCallResults...).
				Once()

			autoAdvance(mockTime, slowQueryTime, 4*slowQueryTime)

			tc.f(t, proxy, true)
			delegate.AssertExpectations(t)
		})
	}
}

type isMock interface {
	On(methodName string, arguments ...interface{}) *mock.Call

	AssertExpectations(t mock.TestingT) bool
}

func runQueryTest(t *testing.T, delegate isMock, mockTime *clock.Mock, exec tupleExecutor) {
	defer goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/authzed/spicedb/internal/datastore/proxy.autoAdvance.func1"), goleak.IgnoreCurrent())
	require := require.New(t)

	delegate.
		On("Execute", mock.Anything).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()

	_, err := exec(context.Background())
	require.ErrorIs(errKnown, err)
	delegate.AssertExpectations(t)

	delegate.
		On("Execute", mock.Anything).
		WaitUntil(mockTime.After(2*slowQueryTime)).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()
	delegate.
		On("Execute", mock.Anything).
		Return(datastore.NewSliceTupleIterator(nil), errAnotherKnown).
		Once()

	done := autoAdvance(mockTime, slowQueryTime, 3*slowQueryTime)

	_, err = exec(context.Background())
	require.ErrorIs(errAnotherKnown, err)
	delegate.AssertExpectations(t)
	<-done

	delegate.
		On("Execute", mock.Anything).
		WaitUntil(mockTime.After(2*slowQueryTime)).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()
	delegate.
		On("Execute", mock.Anything).
		WaitUntil(mockTime.After(3*slowQueryTime)).
		Return(datastore.NewSliceTupleIterator(nil), errAnotherKnown).
		Once()

	autoAdvance(mockTime, slowQueryTime, 4*slowQueryTime)

	_, err = exec(context.Background())
	require.ErrorIs(errKnown, err)
	delegate.AssertExpectations(t)
}

func TestTupleQueryRequestHedging(t *testing.T) {
	delegate := &test.MockedTupleQuery{}
	mockTime := clock.NewMock()
	proxy := &hedgingTupleQuery{
		delegate,
		newHedger(mockTime, slowQueryTime, maxSampleCount, quantile),
	}

	runQueryTest(t, delegate, mockTime, proxy.Execute)
}

func TestReverseTupleQueryRequestHedging(t *testing.T) {
	delegate := &test.MockedReverseTupleQuery{}
	mockTime := clock.NewMock()
	proxy := &hedgingReverseTupleQuery{
		delegate,
		newHedger(mockTime, slowQueryTime, maxSampleCount, quantile),
	}

	runQueryTest(t, delegate, mockTime, proxy.Execute)
}

func TestCommonTupleQueryRequestHedging(t *testing.T) {
	delegate := &test.MockedCommonTupleQuery{}
	mockTime := clock.NewMock()
	proxy := &hedgingCommonTupleQuery{
		delegate,
		newHedger(mockTime, slowQueryTime, maxSampleCount, quantile),
	}

	runQueryTest(t, delegate, mockTime, proxy.Execute)
}

func TestDigestRollover(t *testing.T) {
	require := require.New(t)

	delegate := &test.MockedCommonTupleQuery{}
	mockTime := clock.NewMock()
	proxy := &hedgingCommonTupleQuery{
		delegate,
		newHedger(mockTime, slowQueryTime, 100, 0.9999999999),
	}

	// Simulate a request that starts off fast enough
	delegate.
		On("Execute", mock.Anything).
		WaitUntil(mockTime.After(slowQueryTime/2)).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()

	done := autoAdvance(mockTime, slowQueryTime/4, slowQueryTime*2)

	_, err := proxy.Execute(context.Background())
	require.ErrorIs(err, errKnown)
	delegate.AssertExpectations(t)

	<-done

	for i := time.Duration(0); i < 205; i++ {
		delegate.
			On("Execute", mock.Anything).
			WaitUntil(mockTime.After(i*100*time.Microsecond)).
			Return(datastore.NewSliceTupleIterator(nil), errKnown).
			Once()
	}

	done = autoAdvance(mockTime, 100*time.Microsecond, 205*100*time.Microsecond)

	for i := 0; i < 200; i++ {
		_, err := proxy.Execute(context.Background())
		require.ErrorIs(err, errKnown)
	}
	<-done

	delegate.ExpectedCalls = nil

	// Now run a request which previously would have been fast enough and ensure that the
	// request is hedged
	delegate.
		On("Execute", mock.Anything).
		WaitUntil(mockTime.After(slowQueryTime/2)).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()
	delegate.
		On("Execute", mock.Anything).
		WaitUntil(mockTime.After(100*time.Microsecond)).
		Return(datastore.NewSliceTupleIterator(nil), errAnotherKnown).
		Once()

	autoAdvance(mockTime, 100*time.Microsecond, slowQueryTime)

	_, err = proxy.Execute(context.Background())
	require.ErrorIs(errAnotherKnown, err)
	delegate.AssertExpectations(t)
}

func TestBadArgs(t *testing.T) {
	require := require.New(t)
	delegate := &test.MockedDatastore{}

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

	delegateDatastore := &test.MockedDatastore{}
	delegateQuery := &test.MockedTupleQuery{}
	mockTime := clock.NewMock()

	proxy := newHedgingProxyWithTimeSource(
		delegateDatastore, slowQueryTime, maxSampleCount, quantile, mockTime,
	)

	delegateDatastore.
		On("QueryTuples", mock.Anything, mock.Anything).
		Return(delegateQuery).
		Once()

	expectedTuples := []*v0.RelationTuple{
		{
			ObjectAndRelation: &v0.ObjectAndRelation{
				Namespace: "test",
				ObjectId:  "test",
				Relation:  "test",
			},
			User: &v0.User{
				UserOneof: &v0.User_Userset{
					Userset: &v0.ObjectAndRelation{
						Namespace: "test",
						ObjectId:  "test",
						Relation:  "test",
					},
				},
			},
		},
	}
	delegateQuery.
		On("Execute", mock.Anything).
		Return(datastore.NewSliceTupleIterator(expectedTuples), nil).
		WaitUntil(mockTime.After(2 * slowQueryTime)).
		Once()
	delegateQuery.
		On("Execute", mock.Anything).
		Return(datastore.NewSliceTupleIterator(expectedTuples), nil).
		Once()

	autoAdvance(mockTime, slowQueryTime/2, 2*slowQueryTime)

	it, err := proxy.QueryTuples(datastore.TupleQueryResourceFilter{
		ResourceType: "test",
	}, revisionKnown).Execute(context.Background())
	require.NoError(err)

	only := it.Next()
	require.Equal(expectedTuples[0], only)

	require.Nil(it.Next())
	require.NoError(it.Err())

	delegateDatastore.AssertExpectations(t)
	delegateQuery.AssertExpectations(t)
}

func TestContextCancellation(t *testing.T) {
	require := require.New(t)

	delegate := &test.MockedDatastore{}
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
