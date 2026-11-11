package remote

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/datalayer"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// The fake services mark their responses with their dispatchCount, so tests can tell which
// participant answered.
const (
	fromPrimary   = uint32(1)
	fromSecondary = uint32(2)
)

// requestKind runs one kind of dispatch against a dispatcher and returns the dispatchCount of
// the first result, which identifies the participant that answered.
type requestKind struct {
	name     string // defaults to method
	method   string
	setError func(svc *fakeDispatchSvc, err error)
	run      func(t *testing.T, d dispatch.Dispatcher) (uint32, error)
}

var requestKinds = []requestKind{
	{
		method:   "check",
		setError: func(svc *fakeDispatchSvc, err error) { svc.errorOnCheck = err },
		run: func(t *testing.T, d dispatch.Dispatcher) (uint32, error) {
			resp, err := d.DispatchCheck(t.Context(), testCheckRequest())
			if err != nil {
				return 0, err
			}
			return resp.Metadata.DispatchCount, nil
		},
	},
	{
		method:   "lookupsubjects",
		setError: func(svc *fakeDispatchSvc, err error) { svc.errorOnLS = err },
		run: func(t *testing.T, d dispatch.Dispatcher) (uint32, error) {
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
			if err := d.DispatchLookupSubjects(testLSRequest(), stream); err != nil {
				return 0, err
			}
			require.NotEmpty(t, stream.Results())
			return stream.Results()[0].Metadata.DispatchCount, nil
		},
	},
	{
		name:     "lookupresources2",
		method:   "lookupresources",
		setError: func(svc *fakeDispatchSvc, err error) { svc.errorOnLR2 = err },
		run: func(t *testing.T, d dispatch.Dispatcher) (uint32, error) {
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](t.Context())
			if err := d.DispatchLookupResources2(testLR2Request(nil), stream); err != nil {
				return 0, err
			}
			require.NotEmpty(t, stream.Results())
			return stream.Results()[0].Metadata.DispatchCount, nil
		},
	},
	{
		name:   "lookupresources3",
		method: "lookupresources",
		// The fake fails LR3 with the LR2 error.
		setError: func(svc *fakeDispatchSvc, err error) { svc.errorOnLR2 = err },
		run: func(t *testing.T, d dispatch.Dispatcher) (uint32, error) {
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources3Response](t.Context())
			if err := d.DispatchLookupResources3(testLR3Request(), stream); err != nil {
				return 0, err
			}
			require.NotEmpty(t, stream.Results())
			require.NotEmpty(t, stream.Results()[0].Items)

			// The fake's resource IDs are "<dispatchCount>-<index>".
			source, _, ok := strings.Cut(stream.Results()[0].Items[0].ResourceId, "-")
			require.True(t, ok)
			count, err := strconv.ParseUint(source, 10, 32)
			require.NoError(t, err)
			return uint32(count), nil
		},
	},
}

// label is the request kind's name in subtests.
func (k requestKind) label() string {
	if k.name != "" {
		return k.name
	}
	return k.method
}

func testCheckRequest() *v1.DispatchCheckRequest {
	return testCheckRequestFor("sometype")
}

func testCheckRequestFor(namespace string) *v1.DispatchCheckRequest {
	return &v1.DispatchCheckRequest{
		ResourceRelation: &corev1.RelationReference{Namespace: namespace, Relation: "somerel"},
		ResourceIds:      []string{"foo"},
		Subject:          &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
		Metadata: &v1.ResolverMeta{
			DepthRemaining: 50,
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	}
}

func testLSRequest() *v1.DispatchLookupSubjectsRequest {
	return &v1.DispatchLookupSubjectsRequest{
		ResourceRelation: &corev1.RelationReference{Namespace: "somenamespace", Relation: "somerelation"},
		SubjectRelation:  &corev1.RelationReference{Namespace: "somenamespace", Relation: "somerelation"},
		ResourceIds:      []string{"foo"},
		Metadata: &v1.ResolverMeta{
			DepthRemaining: 50,
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	}
}

func testLR3Request() *v1.DispatchLookupResources3Request {
	return &v1.DispatchLookupResources3Request{
		ResourceRelation: &corev1.RelationReference{Namespace: "somenamespace", Relation: "somerelation"},
		SubjectRelation:  &corev1.RelationReference{Namespace: "somenamespace", Relation: "somerelation"},
		SubjectIds:       []string{"foo"},
		TerminalSubject:  &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
		Metadata: &v1.ResolverMeta{
			DepthRemaining: 50,
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	}
}

func testLR2Request(cursorSections []string) *v1.DispatchLookupResources2Request {
	req := &v1.DispatchLookupResources2Request{
		ResourceRelation: &corev1.RelationReference{Namespace: "somenamespace", Relation: "somerelation"},
		SubjectRelation:  &corev1.RelationReference{Namespace: "somenamespace", Relation: "somerelation"},
		SubjectIds:       []string{"foo"},
		TerminalSubject:  &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
		Metadata: &v1.ResolverMeta{
			DepthRemaining: 50,
			SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
		},
	}
	if cursorSections != nil {
		req.OptionalCursor = &v1.Cursor{Sections: cursorSections, DispatchVersion: 1}
	}
	return req
}

// newStrategyTestDispatcher creates a dispatcher with one secondary, named "secondary", and the
// given dispatch expression for method. maxPrimaryDelay is the secondary's
// MaximumPrimaryHedgingDelay.
func newStrategyTestDispatcher(t *testing.T, primary, secondary *fakeDispatchSvc, method, expr string, startingDelay, maxPrimaryDelay time.Duration) dispatch.Dispatcher {
	t.Helper()
	return newMultiSecondaryTestDispatcher(t, primary, map[string]*fakeDispatchSvc{"secondary": secondary}, method, expr, startingDelay, maxPrimaryDelay)
}

// newMultiSecondaryTestDispatcher creates a dispatcher with the named secondaries and the given
// dispatch expression for method.
func newMultiSecondaryTestDispatcher(t *testing.T, primary *fakeDispatchSvc, secondaries map[string]*fakeDispatchSvc, method, expr string, startingDelay, maxPrimaryDelay time.Duration) dispatch.Dispatcher {
	t.Helper()

	conn := connectionForDispatching(t, primary)
	secondaryDispatch := make(map[string]SecondaryDispatch, len(secondaries))
	for name, svc := range secondaries {
		secondaryDispatch[name] = SecondaryDispatch{
			Name:                       name,
			Client:                     v1.NewDispatchServiceClient(connectionForDispatching(t, svc)),
			MaximumPrimaryHedgingDelay: maxPrimaryDelay,
		}
	}

	parsed, err := ParseDispatchExpression(method, expr)
	require.NoError(t, err)

	d, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
		KeyHandler:                  &keys.DirectKeyHandler{},
		DispatchOverallTimeout:      30 * time.Second,
		SecondaryDispatch:           secondaryDispatch,
		SecondaryDispatchExprs:      map[string]*DispatchExpr{method: parsed},
		StartingPrimaryHedgingDelay: startingDelay,
	})
	require.NoError(t, err)
	return d
}

// gatedCheckClient answers DispatchCheck with resp or err. It closes called on its first call
// and, if after is set, answers only once after is closed.
type gatedCheckClient struct {
	ClusterClient
	resp   *v1.DispatchCheckResponse
	err    error
	called chan struct{}
	after  <-chan struct{}
	calls  atomic.Int32
}

func (c *gatedCheckClient) DispatchCheck(ctx context.Context, _ *v1.DispatchCheckRequest, _ ...grpc.CallOption) (*v1.DispatchCheckResponse, error) {
	if c.calls.Add(1) == 1 && c.called != nil {
		close(c.called)
	}
	if c.after != nil {
		select {
		case <-c.after:
		case <-ctx.Done():
			return &v1.DispatchCheckResponse{}, ctx.Err()
		}
	}
	if c.err != nil {
		return &v1.DispatchCheckResponse{}, c.err
	}
	return c.resp, nil
}

func TestCheckPrimaryErrorWaitsForSecondary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The secondary answers only after the primary has failed, whatever the scheduling.
		primaryCalled := make(chan struct{})
		primary := &gatedCheckClient{err: errors.New("primary failed"), called: primaryCalled}
		secondary := &gatedCheckClient{
			resp:  &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: fromSecondary}},
			after: primaryCalled,
		}

		expr, err := ParseDispatchExpression("check", "['secondary']")
		require.NoError(t, err)
		d, err := NewClusterDispatcher(primary, nil, ClusterDispatcherConfig{
			DispatchOverallTimeout: time.Hour,
			SecondaryDispatch: map[string]SecondaryDispatch{
				"secondary": {Name: "secondary", Client: secondary, MaximumPrimaryHedgingDelay: time.Millisecond},
			},
			SecondaryDispatchExprs:      map[string]*DispatchExpr{"check": expr},
			StartingPrimaryHedgingDelay: time.Millisecond,
		})
		require.NoError(t, err)

		resp, err := d.DispatchCheck(t.Context(), testCheckRequest())
		require.NoError(t, err)
		require.Equal(t, fromSecondary, resp.Metadata.DispatchCount)
		require.Equal(t, int32(1), primary.calls.Load())
	})
}

func TestCursorLockedSecondaryErrorStatusIsReturned(t *testing.T) {
	// A continuation whose cursor is locked to a secondary runs on that secondary only. When it
	// fails, its status must reach the caller rather than a generic error.
	primary := &fakeDispatchSvc{dispatchCount: fromPrimary}
	secondary := &fakeDispatchSvc{dispatchCount: fromSecondary, errorOnLR2: status.Error(codes.ResourceExhausted, "secondary overloaded")}
	d := newStrategyTestDispatcher(t, primary, secondary, "lookupresources", "prefer_secondary(['secondary'], duration('10s'))", time.Millisecond, 5*time.Millisecond)

	cursorLocked := testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("lookupresources", cursorLockedStrategyName))

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](t.Context())
	err := d.DispatchLookupResources2(testLR2Request([]string{secondaryCursorPrefix + "secondary", "rest"}), stream)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "error: %v", err)
	require.Equal(t, uint32(0), primary.calls.Load())

	// Counted as cursor_locked, not as the expression's strategy.
	require.InDelta(t, cursorLocked+1, testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("lookupresources", cursorLockedStrategyName)), 0)
}

func TestRepeatedSecondaryNameDispatchesOnce(t *testing.T) {
	primary := &fakeDispatchSvc{dispatchCount: fromPrimary, sleepTime: time.Second}
	secondary := &fakeDispatchSvc{dispatchCount: fromSecondary}
	d := newStrategyTestDispatcher(t, primary, secondary, "check", "['secondary', 'secondary']", time.Hour, time.Hour)

	resp, err := d.DispatchCheck(t.Context(), testCheckRequest())
	require.NoError(t, err)
	require.Equal(t, fromSecondary, resp.Metadata.DispatchCount)
	require.Equal(t, uint32(1), secondary.calls.Load())
}

func TestSecondaryOpenErrorIsRecorded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// A secondary that cannot even open its stream reports the relation as unsupported.
		primary := &lsStreamClient{results: []*v1.DispatchLookupSubjectsResponse{
			{Metadata: &v1.ResponseMeta{DispatchCount: fromPrimary}},
		}}
		secondary := &lsStreamClient{openErr: testResourceRelationError{errors.New("unsupported"), "somenamespace", "somerelation"}}

		expr, err := ParseDispatchExpression("lookupsubjects", "['secondary']")
		require.NoError(t, err)
		d, err := NewClusterDispatcher(primary, nil, ClusterDispatcherConfig{
			DispatchOverallTimeout: time.Hour,
			SecondaryDispatch: map[string]SecondaryDispatch{
				"secondary": {Name: "secondary", Client: secondary, MaximumPrimaryHedgingDelay: time.Hour},
			},
			SecondaryDispatchExprs: map[string]*DispatchExpr{"lookupsubjects": expr},
		})
		require.NoError(t, err)

		stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
		require.NoError(t, d.DispatchLookupSubjects(testLSRequest(), stream))

		stats := d.(*clusterDispatcher).stats
		require.True(t, stats.unsupported.isUnsupported(tuple.RR("somenamespace", "somerelation"), tuple.RR("other", "...")))
	})
}

func TestCheckPrimaryErrorReturnedWhenAllSecondariesFail(t *testing.T) {
	primary := &fakeDispatchSvc{dispatchCount: fromPrimary, errorOnCheck: errors.New("primary failed")}
	secondary := &fakeDispatchSvc{dispatchCount: fromSecondary, sleepTime: 10 * time.Millisecond, errorOnCheck: errors.New("secondary failed")}
	d := newStrategyTestDispatcher(t, primary, secondary, "check", "['secondary']", time.Millisecond, 5*time.Millisecond)

	_, err := d.DispatchCheck(t.Context(), testCheckRequest())
	require.ErrorContains(t, err, "primary failed")
	require.NotContains(t, err.Error(), "secondary failed")
}

func TestHedgedMaxDelayInExpression(t *testing.T) {
	for _, kind := range requestKinds {
		t.Run(kind.label(), func(t *testing.T) {
			// With a warm digest whose p90 is 300ms and a per-secondary flag of 1s, the primary
			// would wait 300ms. The expression caps the wait at 10ms instead, so the primary
			// answers long before the slow secondary and long before 300ms.
			primary := &fakeDispatchSvc{dispatchCount: fromPrimary}
			secondary := &fakeDispatchSvc{dispatchCount: fromSecondary, sleepTime: 500 * time.Millisecond}
			d := newStrategyTestDispatcher(t, primary, secondary, kind.method, "hedged(['secondary'], duration('10ms'))", time.Millisecond, time.Second)

			cd := d.(*clusterDispatcher)
			for range 100 {
				cd.stats.secondaryDigests[kind.method].addResultTime(t.Context(), 300*time.Millisecond)
			}

			start := time.Now()
			source, err := kind.run(t, d)
			require.NoError(t, err)
			require.Equal(t, fromPrimary, source)
			require.Less(t, time.Since(start), 250*time.Millisecond)
		})
	}
}

func TestPreferSecondary(t *testing.T) {
	type testCase struct {
		name                 string
		expr                 string
		primarySleep         time.Duration
		secondarySleep       time.Duration
		secondaryFails       bool
		expectedSource       uint32
		expectedPrimaryCalls uint32
		maxDuration          time.Duration
	}

	for _, tc := range []testCase{
		{
			name:                 "secondary succeeds, primary never runs",
			expr:                 "prefer_secondary(['secondary'], duration('10s'))",
			secondarySleep:       5 * time.Millisecond,
			expectedSource:       fromSecondary,
			expectedPrimaryCalls: 0,
			maxDuration:          5 * time.Second,
		},
		{
			name:                 "secondary fails, primary runs at once",
			expr:                 "prefer_secondary(['secondary'], duration('10s'))",
			secondaryFails:       true,
			expectedSource:       fromPrimary,
			expectedPrimaryCalls: 1,
			maxDuration:          5 * time.Second,
		},
		{
			name:                 "deadline passes, primary wins",
			expr:                 "prefer_secondary(['secondary'], duration('20ms'))",
			secondarySleep:       time.Second,
			expectedSource:       fromPrimary,
			expectedPrimaryCalls: 1,
			maxDuration:          900 * time.Millisecond,
		},
		{
			name:                 "deadline passes, secondary still wins",
			expr:                 "prefer_secondary(['secondary'], duration('20ms'))",
			primarySleep:         time.Second,
			secondarySleep:       100 * time.Millisecond,
			expectedSource:       fromSecondary,
			expectedPrimaryCalls: 1,
			maxDuration:          900 * time.Millisecond,
		},
	} {
		for _, kind := range requestKinds {
			t.Run(tc.name+"/"+kind.label(), func(t *testing.T) {
				primary := &fakeDispatchSvc{dispatchCount: fromPrimary, sleepTime: tc.primarySleep}
				secondary := &fakeDispatchSvc{dispatchCount: fromSecondary, sleepTime: tc.secondarySleep}
				if tc.secondaryFails {
					kind.setError(secondary, errors.New("secondary failed"))
				}
				d := newStrategyTestDispatcher(t, primary, secondary, kind.method, tc.expr, time.Millisecond, 5*time.Millisecond)

				start := time.Now()
				source, err := kind.run(t, d)
				require.NoError(t, err)
				require.Equal(t, tc.expectedSource, source)
				require.Less(t, time.Since(start), tc.maxDuration)
				require.Equal(t, tc.expectedPrimaryCalls, primary.calls.Load())
			})
		}
	}
}

func TestPreferSecondaryCursorLockedContinuation(t *testing.T) {
	expr := "prefer_secondary(['secondary'], duration('10s'))"

	t.Run("cursor from secondary goes to that secondary only", func(t *testing.T) {
		primary := &fakeDispatchSvc{dispatchCount: fromPrimary}
		secondary := &fakeDispatchSvc{dispatchCount: fromSecondary}
		d := newStrategyTestDispatcher(t, primary, secondary, "lookupresources", expr, time.Millisecond, 5*time.Millisecond)

		stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](t.Context())
		err := d.DispatchLookupResources2(testLR2Request([]string{secondaryCursorPrefix + "secondary", "rest"}), stream)
		require.NoError(t, err)
		require.Equal(t, fromSecondary, stream.Results()[0].Metadata.DispatchCount)
		require.Equal(t, uint32(0), primary.calls.Load())
	})

	t.Run("cursor from primary goes to the primary only", func(t *testing.T) {
		primary := &fakeDispatchSvc{dispatchCount: fromPrimary}
		secondary := &fakeDispatchSvc{dispatchCount: fromSecondary}
		d := newStrategyTestDispatcher(t, primary, secondary, "lookupresources", expr, time.Millisecond, 5*time.Millisecond)

		stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](t.Context())
		err := d.DispatchLookupResources2(testLR2Request([]string{"rest"}), stream)
		require.NoError(t, err)
		require.Equal(t, fromPrimary, stream.Results()[0].Metadata.DispatchCount)
		require.Equal(t, uint32(0), secondary.calls.Load())
	})
}

// lsStreamClient is a ClusterClient whose DispatchLookupSubjects either fails to open, or
// returns a fixed stream.
type lsStreamClient struct {
	ClusterClient
	openErr error
	results []*v1.DispatchLookupSubjectsResponse
}

func (c *lsStreamClient) DispatchLookupSubjects(context.Context, *v1.DispatchLookupSubjectsRequest, ...grpc.CallOption) (v1.DispatchService_DispatchLookupSubjectsClient, error) {
	if c.openErr != nil {
		return nil, c.openErr
	}
	return &lsStream{results: c.results}, nil
}

type lsStream struct {
	grpc.ClientStream
	results []*v1.DispatchLookupSubjectsResponse
}

func (s *lsStream) Recv() (*v1.DispatchLookupSubjectsResponse, error) {
	if len(s.results) == 0 {
		return nil, io.EOF
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result, nil
}

func TestStreamingSecondaryOpenErrorReleasesPrimary(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		primary := &lsStreamClient{results: []*v1.DispatchLookupSubjectsResponse{
			{Metadata: &v1.ResponseMeta{DispatchCount: fromPrimary}},
		}}
		secondary := &lsStreamClient{openErr: errors.New("cannot open stream")}

		expr, err := ParseDispatchExpression("lookupsubjects", "['secondary']")
		require.NoError(t, err)
		d, err := NewClusterDispatcher(primary, nil, ClusterDispatcherConfig{
			DispatchOverallTimeout: 2 * time.Hour,
			SecondaryDispatch: map[string]SecondaryDispatch{
				"secondary": {Name: "secondary", Client: secondary, MaximumPrimaryHedgingDelay: time.Hour},
			},
			SecondaryDispatchExprs:      map[string]*DispatchExpr{"lookupsubjects": expr},
			StartingPrimaryHedgingDelay: time.Hour,
		})
		require.NoError(t, err)

		start := time.Now()
		stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](t.Context())
		require.NoError(t, d.DispatchLookupSubjects(testLSRequest(), stream))
		require.Equal(t, fromPrimary, stream.Results()[0].Metadata.DispatchCount)

		// Without the release, the primary would have waited out the hour-long starting delay.
		require.Less(t, time.Since(start), time.Minute)
	})
}

func TestStrategyChosenPerRequest(t *testing.T) {
	// One expression routes "offloaded" resources with prefer_secondary, everything else with
	// hedged. The secondary is slower than the primary, so the strategy decides who answers.
	expr := "request.resource_relation.namespace == 'offloaded' " +
		"? prefer_secondary(['secondary'], duration('10s')) " +
		": hedged(['secondary'])"

	primary := &fakeDispatchSvc{dispatchCount: fromPrimary}
	secondary := &fakeDispatchSvc{dispatchCount: fromSecondary, sleepTime: 100 * time.Millisecond}
	d := newStrategyTestDispatcher(t, primary, secondary, "check", expr, time.Millisecond, 5*time.Millisecond)

	preferSecondaryCount := testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", "prefer_secondary"))
	hedgedCount := testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", "hedged"))

	// prefer_secondary: the primary waits for the secondary, which answers.
	resp, err := d.DispatchCheck(t.Context(), testCheckRequestFor("offloaded"))
	require.NoError(t, err)
	require.Equal(t, fromSecondary, resp.Metadata.DispatchCount)
	require.Equal(t, uint32(0), primary.calls.Load())

	// hedged: the primary waits only the 1ms starting delay, and answers first.
	resp, err = d.DispatchCheck(t.Context(), testCheckRequestFor("other"))
	require.NoError(t, err)
	require.Equal(t, fromPrimary, resp.Metadata.DispatchCount)
	require.Equal(t, uint32(1), primary.calls.Load())

	require.InDelta(t, preferSecondaryCount+1, testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", "prefer_secondary")), 0)
	require.InDelta(t, hedgedCount+1, testutil.ToFloat64(secondaryStrategyCounter.WithLabelValues("check", "hedged")), 0)
}

func TestInvalidStrategyArgumentFallsBackToPrimary(t *testing.T) {
	// Literal durations of zero or less are rejected at parse time, so these are computed.
	for _, expr := range []string{
		"prefer_secondary(['secondary'], duration('1s') - duration('1s'))",
		"prefer_secondary(['secondary'], duration('0s') - duration('1s'))",
		"hedged(['secondary'], duration('1s') - duration('1s'))",
		"hedged(['secondary'], duration('0s') - duration('1s'))",
	} {
		for _, kind := range requestKinds {
			t.Run(expr+"/"+kind.label(), func(t *testing.T) {
				// The expression parses, but fails when evaluated: the request goes to the
				// primary only, and the secondary is never called.
				primary := &fakeDispatchSvc{dispatchCount: fromPrimary}
				secondary := &fakeDispatchSvc{dispatchCount: fromSecondary}
				d := newStrategyTestDispatcher(t, primary, secondary, kind.method, expr, time.Hour, time.Hour)

				evalFailures := testutil.ToFloat64(exprEvalErrorsCounter.WithLabelValues(kind.method))

				source, err := kind.run(t, d)
				require.NoError(t, err)
				require.Equal(t, fromPrimary, source)
				require.Equal(t, uint32(0), secondary.calls.Load())
				require.InDelta(t, evalFailures+1, testutil.ToFloat64(exprEvalErrorsCounter.WithLabelValues(kind.method)), 0)
			})
		}
	}
}

func TestPreferSecondaryWithSeveralSecondaries(t *testing.T) {
	const (
		fromFirst  = uint32(2)
		fromSecond = uint32(3)
	)
	expr := "prefer_secondary(['first', 'second'], duration('10s'))"

	for _, kind := range requestKinds {
		t.Run("one fails, the other answers/"+kind.label(), func(t *testing.T) {
			primary := &fakeDispatchSvc{dispatchCount: fromPrimary}
			first := &fakeDispatchSvc{dispatchCount: fromFirst}
			kind.setError(first, errors.New("first failed"))
			second := &fakeDispatchSvc{dispatchCount: fromSecond, sleepTime: 50 * time.Millisecond}
			d := newMultiSecondaryTestDispatcher(t, primary, map[string]*fakeDispatchSvc{"first": first, "second": second}, kind.method, expr, time.Millisecond, 5*time.Millisecond)

			// One failure out of two does not release the primary.
			source, err := kind.run(t, d)
			require.NoError(t, err)
			require.Equal(t, fromSecond, source)
			require.Equal(t, uint32(0), primary.calls.Load())
		})

		t.Run("all fail, the primary answers/"+kind.label(), func(t *testing.T) {
			primary := &fakeDispatchSvc{dispatchCount: fromPrimary}
			first := &fakeDispatchSvc{dispatchCount: fromFirst}
			kind.setError(first, errors.New("first failed"))
			second := &fakeDispatchSvc{dispatchCount: fromSecond, sleepTime: 20 * time.Millisecond}
			kind.setError(second, errors.New("second failed"))
			d := newMultiSecondaryTestDispatcher(t, primary, map[string]*fakeDispatchSvc{"first": first, "second": second}, kind.method, expr, time.Millisecond, 5*time.Millisecond)

			// Released by the failures, long before the 10s deadline.
			start := time.Now()
			source, err := kind.run(t, d)
			require.NoError(t, err)
			require.Equal(t, fromPrimary, source)
			require.Less(t, time.Since(start), 5*time.Second)
		})
	}
}
