package proxy

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/mocks"
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

func setupDatastoreExpectation(ds *mocks.MockDatastore, methodName string, args []any, results []any, waitChan <-chan time.Time) {
	call := ds.EXPECT()
	var mockCall *gomock.Call

	switch methodName {
	case "OptimizedRevision":
		mockCall = call.OptimizedRevision(args[0]).Return(results[0].(datastore.Revision), results[1].(error))
	case "HeadRevision":
		mockCall = call.HeadRevision(args[0]).Return(results[0].(datastore.Revision), results[1].(error))
	}

	if waitChan != nil {
		mockCall.Do(func(any) { <-waitChan })
	}
}

func setupReaderExpectation(reader *mocks.MockReader, methodName string, args []any, results []any, waitChan <-chan time.Time) {
	call := reader.EXPECT()
	var mockCall *gomock.Call

	switch methodName {
	case "ReadNamespaceByName":
		// ReadNamespaceByName(ctx, nsName) - args[0] is nsName, we need to match ctx with gomock.Any()
		mockCall = call.ReadNamespaceByName(gomock.Any(), args[0]).Return(results[0].(*core.NamespaceDefinition), results[1].(datastore.Revision), results[2].(error))
	case "QueryRelationships":
		mockCall = call.QueryRelationships(args[0], args[1]).Return(results[0].(datastore.RelationshipIterator), results[1].(error))
	case "ReverseQueryRelationships":
		mockCall = call.ReverseQueryRelationships(args[0], args[1]).Return(results[0].(datastore.RelationshipIterator), results[1].(error))
	}

	if waitChan != nil {
		mockCall.Do(func(any, any) { <-waitChan })
	}
}

func TestDatastoreRequestHedging(t *testing.T) {
	testCases := []struct {
		methodName        string
		useSnapshotReader bool
		arguments         []any
		firstCallResults  []any
		secondCallResults []any
		f                 testFunc
	}{
		{
			"ReadNamespaceByName",
			true,
			[]any{nsKnown},
			[]any{&core.NamespaceDefinition{}, revisionKnown, errKnown},
			[]any{&core.NamespaceDefinition{}, anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				_, rev, err := proxy.SnapshotReader(datastore.NoRevision).ReadNamespaceByName(t.Context(), nsKnown)
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
			[]any{gomock.Any(), gomock.Any()},
			[]any{revisionKnown, errKnown},
			[]any{anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				rev, err := proxy.OptimizedRevision(t.Context())
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
			[]any{gomock.Any()},
			[]any{revisionKnown, errKnown},
			[]any{anotherRevisionKnown, errKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				rev, err := proxy.HeadRevision(t.Context())
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
			[]any{gomock.Any(), gomock.Any()},
			[]any{emptyIterator, errKnown},
			[]any{emptyIterator, errAnotherKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				_, err := proxy.
					SnapshotReader(datastore.NoRevision).
					QueryRelationships(t.Context(), datastore.RelationshipsFilter{})
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
			[]any{gomock.Any(), gomock.Any()},
			[]any{emptyIterator, errKnown},
			[]any{emptyIterator, errAnotherKnown},
			func(t *testing.T, proxy datastore.Datastore, expectFirst bool) {
				require := require.New(t)
				_, err := proxy.
					SnapshotReader(datastore.NoRevision).
					ReverseQueryRelationships(t.Context(), datastore.SubjectsFilter{})
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
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockTime := clock.NewMock()
			delegateDS := mocks.NewMockDatastore(ctrl)
			proxy, err := newHedgingProxyWithTimeSource(
				delegateDS, slowQueryTime, maxSampleCount, quantile, mockTime,
			)
			require.NoError(t, err)

			var readerMock *mocks.MockReader
			if tc.useSnapshotReader {
				readerMock = mocks.NewMockReader(ctrl)
				delegateDS.EXPECT().SnapshotReader(gomock.Any()).Return(readerMock).AnyTimes()
			}

			// First call - immediate response
			if tc.useSnapshotReader {
				setupReaderExpectation(readerMock, tc.methodName, tc.arguments, tc.firstCallResults, nil)
			} else {
				setupDatastoreExpectation(delegateDS, tc.methodName, tc.arguments, tc.firstCallResults, nil)
			}

			tc.f(t, proxy, true)

			// Second set of calls - first call slow, second call fast (hedged request)
			if tc.useSnapshotReader {
				setupReaderExpectation(readerMock, tc.methodName, tc.arguments, tc.firstCallResults, mockTime.After(5*slowQueryTime))
				setupReaderExpectation(readerMock, tc.methodName, tc.arguments, tc.secondCallResults, nil)
			} else {
				setupDatastoreExpectation(delegateDS, tc.methodName, tc.arguments, tc.firstCallResults, mockTime.After(5*slowQueryTime))
				setupDatastoreExpectation(delegateDS, tc.methodName, tc.arguments, tc.secondCallResults, nil)
			}

			done := autoAdvance(mockTime, slowQueryTime, 6*slowQueryTime)

			tc.f(t, proxy, false)

			<-done

			// Third set of calls - both slow
			if tc.useSnapshotReader {
				setupReaderExpectation(readerMock, tc.methodName, tc.arguments, tc.firstCallResults, mockTime.After(7*slowQueryTime))
				setupReaderExpectation(readerMock, tc.methodName, tc.arguments, tc.secondCallResults, mockTime.After(8*slowQueryTime))
			} else {
				setupDatastoreExpectation(delegateDS, tc.methodName, tc.arguments, tc.firstCallResults, mockTime.After(7*slowQueryTime))
				setupDatastoreExpectation(delegateDS, tc.methodName, tc.arguments, tc.secondCallResults, mockTime.After(8*slowQueryTime))
			}

			autoAdvance(mockTime, slowQueryTime, 9*slowQueryTime)

			tc.f(t, proxy, true)
		})
	}
}

func TestDigestRollover(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegate := mocks.NewMockDatastore(ctrl)
	mockTime := clock.NewMock()
	proxy := &hedgingProxy{
		Datastore:          delegate,
		headRevisionHedger: newHedger(mockTime, slowQueryTime, 100, 0.9999999999),
	}

	// Simulate a request that starts off fast enough
	waitChan := mockTime.After(slowQueryTime / 2)
	delegate.EXPECT().
		HeadRevision(gomock.Any()).
		DoAndReturn(func(any) (datastore.Revision, error) {
			<-waitChan
			return datastore.NoRevision, errKnown
		})

	done := autoAdvance(mockTime, slowQueryTime/4, slowQueryTime*2)

	_, err := proxy.HeadRevision(t.Context())
	require.ErrorIs(err, errKnown)

	<-done

	for i := time.Duration(0); i < 205; i++ {
		waitChan := mockTime.After(i * 100 * time.Microsecond)
		delegate.EXPECT().
			HeadRevision(gomock.Any()).
			DoAndReturn(func(any) (datastore.Revision, error) {
				<-waitChan
				return datastore.NoRevision, errKnown
			})
	}

	done = autoAdvance(mockTime, 100*time.Microsecond, 205*100*time.Microsecond)

	for i := 0; i < 200; i++ {
		_, err := proxy.HeadRevision(t.Context())
		require.ErrorIs(err, errKnown)
	}
	<-done

	// Now run a request which previously would have been fast enough and ensure that the
	// request is hedged
	waitChan1 := mockTime.After(slowQueryTime / 2)
	delegate.EXPECT().
		HeadRevision(gomock.Any()).
		DoAndReturn(func(any) (datastore.Revision, error) {
			<-waitChan1
			return datastore.NoRevision, errKnown
		})

	waitChan2 := mockTime.After(100 * time.Microsecond)
	delegate.EXPECT().
		HeadRevision(gomock.Any()).
		DoAndReturn(func(any) (datastore.Revision, error) {
			<-waitChan2
			return datastore.NoRevision, errAnotherKnown
		})

	autoAdvance(mockTime, 100*time.Microsecond, slowQueryTime)

	_, err = proxy.HeadRevision(t.Context())
	require.ErrorIs(errAnotherKnown, err)
}

func TestBadArgs(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegate := mocks.NewMockDatastore(ctrl)

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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegateDatastore := mocks.NewMockDatastore(ctrl)
	delegateReader := mocks.NewMockReader(ctrl)
	mockTime := clock.NewMock()

	proxy, err := newHedgingProxyWithTimeSource(
		delegateDatastore, slowQueryTime, maxSampleCount, quantile, mockTime,
	)
	require.NoError(err)

	expectedRels := []tuple.Relationship{
		tuple.MustParse("document:first#viewer@user:bob"),
		tuple.MustParse("document:second#viewer@user:alice"),
	}

	delegateDatastore.EXPECT().SnapshotReader(gomock.Any()).Return(delegateReader).AnyTimes()

	waitChan := mockTime.After(2 * slowQueryTime)
	delegateReader.EXPECT().
		QueryRelationships(gomock.Any(), gomock.Any()).
		DoAndReturn(func(any, any) (datastore.RelationshipIterator, error) {
			<-waitChan
			return common.NewSliceRelationshipIterator(expectedRels), nil
		})

	delegateReader.EXPECT().
		QueryRelationships(gomock.Any(), gomock.Any()).
		Return(common.NewSliceRelationshipIterator(expectedRels), nil)

	autoAdvance(mockTime, slowQueryTime/2, 2*slowQueryTime)

	it, err := proxy.SnapshotReader(revisionKnown).QueryRelationships(
		t.Context(), datastore.RelationshipsFilter{
			OptionalResourceType: "document",
		},
	)
	require.NoError(err)

	slice, err := datastore.IteratorToSlice(it)
	require.NoError(err)
	require.Equal(expectedRels, slice)
}

func TestContextCancellation(t *testing.T) {
	require := require.New(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	delegate := mocks.NewMockDatastore(ctrl)
	mockTime := clock.NewMock()
	proxy, err := newHedgingProxyWithTimeSource(
		delegate, slowQueryTime, maxSampleCount, quantile, mockTime,
	)
	require.NoError(err)

	waitChan := mockTime.After(500 * time.Microsecond)
	delegate.EXPECT().
		HeadRevision(gomock.Any()).
		DoAndReturn(func(any) (datastore.Revision, error) {
			<-waitChan
			return datastore.NoRevision, errKnown
		})

	ctx, cancel := context.WithCancel(t.Context())
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
