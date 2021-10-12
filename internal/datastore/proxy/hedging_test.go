package proxy

import (
	"context"
	"errors"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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
			[]interface{}{mock.Anything, nsKnown},
			[]interface{}{&v0.NamespaceDefinition{}, revisionKnown, errKnown},
			[]interface{}{&v0.NamespaceDefinition{}, anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				_, rev, err := proxy.ReadNamespace(context.Background(), nsKnown)
				require.ErrorIs(errKnown, err)
				if expectFirst {
					require.Equal(revisionKnown, rev)
				} else {
					require.Equal(anotherRevisionKnown, rev)
				}
			},
		},
		{
			"Revision",
			[]interface{}{mock.Anything},
			[]interface{}{revisionKnown, errKnown},
			[]interface{}{anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				rev, err := proxy.Revision(context.Background())
				require.ErrorIs(errKnown, err)
				if expectFirst {
					require.Equal(revisionKnown, rev)
				} else {
					require.Equal(anotherRevisionKnown, rev)
				}
			},
		},
		{
			"SyncRevision",
			[]interface{}{mock.Anything},
			[]interface{}{revisionKnown, errKnown},
			[]interface{}{anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				rev, err := proxy.SyncRevision(context.Background())
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
			delegate := &test.MockedDatastore{}
			proxy := NewHedgingProxy(delegate, slowQueryTime, maxSampleCount, quantile)

			delegate.
				On(tc.methodName, tc.arguments...).
				Return(tc.firstCallResults...).
				Once()

			tc.f(t, proxy, true)
			delegate.AssertExpectations(t)

			delegate.
				On(tc.methodName, tc.arguments...).
				After(2 * slowQueryTime).
				Return(tc.firstCallResults...).
				Once()
			delegate.
				On(tc.methodName, tc.arguments...).
				Return(tc.secondCallResults...).
				Once()

			tc.f(t, proxy, false)
			delegate.AssertExpectations(t)

			delegate.
				On(tc.methodName, tc.arguments...).
				After(2 * slowQueryTime).
				Return(tc.firstCallResults...).
				Once()
			delegate.
				On(tc.methodName, tc.arguments...).
				After(2 * slowQueryTime).
				Return(tc.secondCallResults...).
				Once()

			tc.f(t, proxy, true)
			delegate.AssertExpectations(t)
		})
	}
}

func TestTupleQueryRequestHedging(t *testing.T) {
	require := require.New(t)

	delegate := &test.MockedTupleQuery{}
	proxy := &hedgingTupleQuery{
		delegate,
		newHedger(slowQueryTime, maxSampleCount, quantile),
	}

	delegate.
		On("Execute", mock.Anything).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()

	_, err := proxy.Execute(context.Background())
	require.ErrorIs(errKnown, err)
	delegate.AssertExpectations(t)

	delegate.
		On("Execute", mock.Anything).
		After(2*slowQueryTime).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()
	delegate.
		On("Execute", mock.Anything).
		Return(datastore.NewSliceTupleIterator(nil), errAnotherKnown).
		Once()

	_, err = proxy.Execute(context.Background())
	require.ErrorIs(errAnotherKnown, err)
	delegate.AssertExpectations(t)

	delegate.
		On("Execute", mock.Anything).
		After(2*slowQueryTime).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()
	delegate.
		On("Execute", mock.Anything).
		After(2*slowQueryTime).
		Return(datastore.NewSliceTupleIterator(nil), errAnotherKnown).
		Once()

	_, err = proxy.Execute(context.Background())
	require.ErrorIs(errKnown, err)
	delegate.AssertExpectations(t)
}

func TestReverseTupleQueryRequestHedging(t *testing.T) {
	require := require.New(t)

	delegate := &test.MockedReverseTupleQuery{}
	proxy := &hedgingReverseTupleQuery{
		delegate,
		newHedger(slowQueryTime, maxSampleCount, quantile),
	}

	delegate.
		On("Execute", mock.Anything).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()

	_, err := proxy.Execute(context.Background())
	require.ErrorIs(errKnown, err)
	delegate.AssertExpectations(t)

	delegate.
		On("Execute", mock.Anything).
		After(2*slowQueryTime).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()
	delegate.
		On("Execute", mock.Anything).
		Return(datastore.NewSliceTupleIterator(nil), errAnotherKnown).
		Once()

	_, err = proxy.Execute(context.Background())
	require.ErrorIs(errAnotherKnown, err)
	delegate.AssertExpectations(t)

	delegate.
		On("Execute", mock.Anything).
		After(2*slowQueryTime).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()
	delegate.
		On("Execute", mock.Anything).
		After(2*slowQueryTime).
		Return(datastore.NewSliceTupleIterator(nil), errAnotherKnown).
		Once()

	_, err = proxy.Execute(context.Background())
	require.ErrorIs(errKnown, err)
	delegate.AssertExpectations(t)
}

func TestCommonTupleQueryRequestHedging(t *testing.T) {
	require := require.New(t)

	delegate := &test.MockedCommonTupleQuery{}
	proxy := &hedgingCommonTupleQuery{
		delegate,
		newHedger(slowQueryTime, maxSampleCount, quantile),
	}

	delegate.
		On("Execute", mock.Anything).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()

	_, err := proxy.Execute(context.Background())
	require.ErrorIs(errKnown, err)
	delegate.AssertExpectations(t)

	delegate.
		On("Execute", mock.Anything).
		After(2*slowQueryTime).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()
	delegate.
		On("Execute", mock.Anything).
		Return(datastore.NewSliceTupleIterator(nil), errAnotherKnown).
		Once()

	_, err = proxy.Execute(context.Background())
	require.ErrorIs(errAnotherKnown, err)
	delegate.AssertExpectations(t)

	delegate.
		On("Execute", mock.Anything).
		After(2*slowQueryTime).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()
	delegate.
		On("Execute", mock.Anything).
		After(2*slowQueryTime).
		Return(datastore.NewSliceTupleIterator(nil), errAnotherKnown).
		Once()

	_, err = proxy.Execute(context.Background())
	require.ErrorIs(errKnown, err)
	delegate.AssertExpectations(t)
}

func TestDigestRollover(t *testing.T) {
	require := require.New(t)

	delegate := &test.MockedCommonTupleQuery{}
	proxy := &hedgingCommonTupleQuery{
		delegate,
		newHedger(slowQueryTime, 100, 0.9999999999),
	}

	// Simulate a request that starts off fast enough
	delegate.
		On("Execute", mock.Anything).
		After(slowQueryTime/2).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()

	_, err := proxy.Execute(context.Background())
	require.ErrorIs(err, errKnown)
	delegate.AssertExpectations(t)

	delegate.
		On("Execute", mock.Anything).
		After(100*time.Microsecond).
		Return(datastore.NewSliceTupleIterator(nil), errKnown)

	for i := 0; i < 200; i++ {
		_, err := proxy.Execute(context.Background())
		require.ErrorIs(err, errKnown)
	}
	delegate.AssertExpectations(t)

	delegate.ExpectedCalls = nil

	// Now run a request which previously would have been fast enough and ensure that the
	// request is hedged
	delegate.
		On("Execute", mock.Anything).
		After(slowQueryTime/2).
		Return(datastore.NewSliceTupleIterator(nil), errKnown).
		Once()
	delegate.
		On("Execute", mock.Anything).
		After(100*time.Microsecond).
		Return(datastore.NewSliceTupleIterator(nil), errAnotherKnown).
		Once()

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
