package remote

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/caio/go-tdigest/v4"
	"github.com/ccoveille/go-safecast"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/grpchelpers"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type fakeDispatchSvc struct {
	v1.UnimplementedDispatchServiceServer

	resultCount uint32

	sleepTime     time.Duration
	dispatchCount uint32
	errorOnLR2    error
	errorOnLS     error
	errorOnCheck  error
	raisePanic    bool
}

func (fds *fakeDispatchSvc) DispatchCheck(ctx context.Context, _ *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	time.Sleep(fds.sleepTime)

	select {
	case <-ctx.Done():
		return &v1.DispatchCheckResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: 0,
			},
		}, ctx.Err()

	default:
		if fds.raisePanic {
			panic("panic raised")
		}
	}

	if fds.errorOnCheck != nil {
		return &v1.DispatchCheckResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: 0,
			},
		}, fds.errorOnCheck
	}

	return &v1.DispatchCheckResponse{
		Metadata: &v1.ResponseMeta{
			DispatchCount: fds.dispatchCount,
		},
	}, nil
}

func (fds *fakeDispatchSvc) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, srv v1.DispatchService_DispatchLookupSubjectsServer) error {
	if fds.errorOnLS != nil {
		return fds.errorOnLS
	}

	if fds.resultCount == 0 {
		fds.resultCount = 2
	}

	for i := range fds.resultCount {
		time.Sleep(fds.sleepTime)
		if err := srv.Send(&v1.DispatchLookupSubjectsResponse{
			FoundSubjectsByResourceId: map[string]*v1.FoundSubjects{
				req.ResourceIds[0]: {
					FoundSubjects: []*v1.FoundSubject{
						{
							SubjectId: fmt.Sprintf("%d", i),
						},
					},
				},
			},
			Metadata: &v1.ResponseMeta{
				DispatchCount: fds.dispatchCount,
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (fds *fakeDispatchSvc) DispatchLookupResources2(_ *v1.DispatchLookupResources2Request, srv v1.DispatchService_DispatchLookupResources2Server) error {
	if fds.errorOnLR2 != nil {
		return fds.errorOnLR2
	}

	if fds.resultCount == 0 {
		fds.resultCount = 2
	}

	for i := range fds.resultCount {
		time.Sleep(fds.sleepTime)
		if err := srv.Send(&v1.DispatchLookupResources2Response{
			Resource: &v1.PossibleResource{ResourceId: fmt.Sprintf("%d", i)},
			Metadata: &v1.ResponseMeta{
				DispatchCount: fds.dispatchCount,
			},
			AfterResponseCursor: &v1.Cursor{
				Sections:        nil,
				DispatchVersion: 1,
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func TestDispatchTimeout(t *testing.T) {
	for _, tc := range []struct {
		timeout   time.Duration
		sleepTime time.Duration
	}{
		{
			10 * time.Millisecond,
			20 * time.Millisecond,
		},
		{
			100 * time.Millisecond,
			20 * time.Millisecond,
		},
	} {
		tc := tc
		t.Run(fmt.Sprintf("%v", tc.timeout > tc.sleepTime), func(t *testing.T) {
			// Configure a fake dispatcher service and an associated buffconn-based
			// connection to it.
			listener := bufconn.Listen(humanize.MiByte)
			s := grpc.NewServer()

			fakeDispatch := &fakeDispatchSvc{sleepTime: tc.sleepTime}
			v1.RegisterDispatchServiceServer(s, fakeDispatch)

			go func() {
				// Ignore any errors
				_ = s.Serve(listener)
			}()

			conn, err := grpchelpers.DialAndWait(
				context.Background(),
				"",
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return listener.Dial()
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			require.NoError(t, err)

			t.Cleanup(func() {
				conn.Close()
				listener.Close()
				s.Stop()
			})

			// Configure a dispatcher with a very low timeout.
			dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
				KeyHandler:             &keys.DirectKeyHandler{},
				DispatchOverallTimeout: tc.timeout,
			}, nil, nil, 0*time.Second)
			require.NoError(t, err)
			require.True(t, dispatcher.ReadyState().IsReady)

			// Invoke a dispatched "check" and ensure it times out, as the fake dispatch will wait
			// longer than the configured timeout.
			resp, err := dispatcher.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
				ResourceRelation: &corev1.RelationReference{Namespace: "sometype", Relation: "somerel"},
				ResourceIds:      []string{"foo"},
				Metadata:         &v1.ResolverMeta{DepthRemaining: 50},
				Subject:          &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
			})
			if tc.sleepTime > tc.timeout {
				require.Error(t, err)
				require.ErrorContains(t, err, "context deadline exceeded")
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.GreaterOrEqual(t, resp.Metadata.DispatchCount, uint32(1))
			}

			// Invoke a dispatched "LookupSubjects" and test as well.
			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](context.Background())
			err = dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: &corev1.RelationReference{Namespace: "sometype", Relation: "somerel"},
				ResourceIds:      []string{"foo"},
				Metadata:         &v1.ResolverMeta{DepthRemaining: 50},
				SubjectRelation:  &corev1.RelationReference{Namespace: "sometype", Relation: "somerel"},
			}, stream)
			if tc.sleepTime > tc.timeout {
				require.Error(t, err)
				require.ErrorContains(t, err, "context deadline exceeded")
			} else {
				require.NoError(t, err)
				require.NotEmpty(t, stream.Results())
				require.GreaterOrEqual(t, stream.Results()[0].Metadata.DispatchCount, uint32(1))
			}
		})
	}
}

func TestCheckSecondaryDispatch(t *testing.T) {
	for _, tc := range []struct {
		name             string
		expr             string
		request          *v1.DispatchCheckRequest
		primarySleepTime time.Duration
		expectedResult   uint32
	}{
		{
			"no multidispatch",
			"['invalid']",
			&v1.DispatchCheckRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				ResourceIds: []string{"foo"},
				Metadata:    &v1.ResolverMeta{DepthRemaining: 50},
				Subject:     &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
			},
			0 * time.Millisecond,
			1,
		},
		{
			"basic multidispatch",
			"['secondary']",
			&v1.DispatchCheckRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				ResourceIds: []string{"foo"},
				Metadata:    &v1.ResolverMeta{DepthRemaining: 50},
				Subject:     &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
			},
			1 * time.Second,
			2,
		},
		{
			"basic multidispatch, expr doesn't call secondary",
			"['notconfigured']",
			&v1.DispatchCheckRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				ResourceIds: []string{"foo"},
				Metadata:    &v1.ResolverMeta{DepthRemaining: 50},
				Subject:     &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
			},
			1 * time.Second,
			1,
		},
		{
			"expr matches request",
			"request.resource_relation.namespace == 'somenamespace' ? ['secondary'] : []",
			&v1.DispatchCheckRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				ResourceIds: []string{"foo"},
				Metadata:    &v1.ResolverMeta{DepthRemaining: 50},
				Subject:     &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
			},
			1 * time.Second,
			2,
		},
		{
			"expr does not match request",
			"request.resource_relation.namespace == 'somenamespace' ? ['secondary'] : []",
			&v1.DispatchCheckRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "someothernamespace",
					Relation:  "somerelation",
				},
				ResourceIds: []string{"foo"},
				Metadata:    &v1.ResolverMeta{DepthRemaining: 50},
				Subject:     &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
			},
			1 * time.Second,
			1,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 1, sleepTime: tc.primarySleepTime})
			secondaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 2, sleepTime: 0 * time.Millisecond})

			parsed, err := ParseDispatchExpression("check", tc.expr)
			require.NoError(t, err)

			dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
				KeyHandler:             &keys.DirectKeyHandler{},
				DispatchOverallTimeout: 30 * time.Second,
			}, map[string]SecondaryDispatch{
				"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
			}, map[string]*DispatchExpr{
				"check": parsed,
			}, 0*time.Second)
			require.NoError(t, err)
			require.True(t, dispatcher.ReadyState().IsReady)

			resp, err := dispatcher.DispatchCheck(context.Background(), tc.request)
			require.NoError(t, err)
			require.Equal(t, tc.expectedResult, resp.Metadata.DispatchCount)
		})
	}
}

func TestLRSecondaryDispatch(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		expr                  string
		request               *v1.DispatchLookupResources2Request
		expectedDispatchCount uint32
		expectedError         bool
	}{
		{
			"no multidispatch",
			"['invalid']",
			&v1.DispatchLookupResources2Request{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectIds: []string{"foo"},
				TerminalSubject: &corev1.ObjectAndRelation{
					Namespace: "foo",
					ObjectId:  "bar",
					Relation:  "...",
				},
				Metadata: &v1.ResolverMeta{DepthRemaining: 50},
			},
			1,
			false,
		},
		{
			"valid multidispatch",
			"['secondary']",
			&v1.DispatchLookupResources2Request{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectIds: []string{"foo"},
				TerminalSubject: &corev1.ObjectAndRelation{
					Namespace: "foo",
					ObjectId:  "bar",
					Relation:  "...",
				},
				Metadata: &v1.ResolverMeta{DepthRemaining: 50},
			},
			2,
			false,
		},
		{
			"cursored multidispatch to invalid secondary",
			"['secondary']",
			&v1.DispatchLookupResources2Request{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectIds: []string{"foo"},
				TerminalSubject: &corev1.ObjectAndRelation{
					Namespace: "foo",
					ObjectId:  "bar",
					Relation:  "...",
				},
				OptionalCursor: &v1.Cursor{
					Sections:        []string{"somethingelse"},
					DispatchVersion: 1,
				},
				Metadata: &v1.ResolverMeta{DepthRemaining: 50},
			},
			2, // Falls back to the default secondary.
			false,
		},
		{
			"cursored multidispatch to cursored secondary",
			"['secondary']",
			&v1.DispatchLookupResources2Request{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectIds: []string{"foo"},
				TerminalSubject: &corev1.ObjectAndRelation{
					Namespace: "foo",
					ObjectId:  "bar",
					Relation:  "...",
				},
				OptionalCursor: &v1.Cursor{
					Sections:        []string{secondaryCursorPrefix + "tertiary"},
					DispatchVersion: 1,
				},
				Metadata: &v1.ResolverMeta{DepthRemaining: 50},
			},
			3,
			false,
		},
		{
			"cursored multidispatch to cursored secondary that raises an error",
			"['secondary']",
			&v1.DispatchLookupResources2Request{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectIds: []string{"foo"},
				TerminalSubject: &corev1.ObjectAndRelation{
					Namespace: "foo",
					ObjectId:  "bar",
					Relation:  "...",
				},
				OptionalCursor: &v1.Cursor{
					Sections:        []string{secondaryCursorPrefix + "error"},
					DispatchVersion: 1,
				},
				Metadata: &v1.ResolverMeta{DepthRemaining: 50},
			},
			1,
			true, // since the secondary was in the cursor, if it errors, the operation fails.
		},
		{
			"cursored multidispatch to default secondary that raises an error",
			"['error']",
			&v1.DispatchLookupResources2Request{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectIds: []string{"foo"},
				TerminalSubject: &corev1.ObjectAndRelation{
					Namespace: "foo",
					ObjectId:  "bar",
					Relation:  "...",
				},
				Metadata: &v1.ResolverMeta{DepthRemaining: 50},
			},
			1,
			false,
		},
		{
			"cursored multidispatch to unknown secondary",
			"['error']",
			&v1.DispatchLookupResources2Request{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectIds: []string{"foo"},
				TerminalSubject: &corev1.ObjectAndRelation{
					Namespace: "foo",
					ObjectId:  "bar",
					Relation:  "...",
				},
				OptionalCursor: &v1.Cursor{
					Sections:        []string{secondaryCursorPrefix + "unknown"},
					DispatchVersion: 1,
				},
				Metadata: &v1.ResolverMeta{DepthRemaining: 50},
			},
			0,
			true,
		},
		{
			"cursored multidispatch to default secondary",
			"['secondary', 'tertiary']",
			&v1.DispatchLookupResources2Request{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectIds: []string{"foo"},
				TerminalSubject: &corev1.ObjectAndRelation{
					Namespace: "foo",
					ObjectId:  "bar",
					Relation:  "...",
				},
				OptionalCursor: &v1.Cursor{
					Sections:        []string{secondaryCursorPrefix + "secondary"},
					DispatchVersion: 1,
				},
				Metadata: &v1.ResolverMeta{DepthRemaining: 50},
			},
			2,
			false,
		},
		{
			"cursored multidispatch to non-default secondary",
			"['tertiary', 'secondary']",
			&v1.DispatchLookupResources2Request{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectIds: []string{"foo"},
				TerminalSubject: &corev1.ObjectAndRelation{
					Namespace: "foo",
					ObjectId:  "bar",
					Relation:  "...",
				},
				OptionalCursor: &v1.Cursor{
					Sections:        []string{secondaryCursorPrefix + "secondary"},
					DispatchVersion: 1,
				},
				Metadata: &v1.ResolverMeta{DepthRemaining: 50},
			},
			2,
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// NOTE: we configure the primary to be slow, so that the secondaries are used when applicable.
			conn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 1, sleepTime: 100 * time.Millisecond})
			secondaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 2, sleepTime: 0 * time.Millisecond})
			tertiaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 3, sleepTime: 0 * time.Millisecond})
			errorConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 4, sleepTime: 0 * time.Millisecond, errorOnLR2: fmt.Errorf("error")})

			parsed, err := ParseDispatchExpression("lookupresources", tc.expr)
			require.NoError(t, err)

			dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
				KeyHandler:             &keys.DirectKeyHandler{},
				DispatchOverallTimeout: 30 * time.Second,
			}, map[string]SecondaryDispatch{
				"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
				"tertiary":  {Name: "tertiary", Client: v1.NewDispatchServiceClient(tertiaryConn)},
				"error":     {Name: "error", Client: v1.NewDispatchServiceClient(errorConn)},
			}, map[string]*DispatchExpr{
				"lookupresources": parsed,
			}, 0*time.Second)
			require.NoError(t, err)
			require.True(t, dispatcher.ReadyState().IsReady)

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](context.Background())
			err = dispatcher.DispatchLookupResources2(tc.request, stream)

			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, len(stream.Results()))
				require.Equal(t, tc.expectedDispatchCount, stream.Results()[0].Metadata.DispatchCount)
			}
		})
	}
}

// TestLRDispatchFallbackToPrimary tests the case where the secondary dispatch returns
// an error, and the primary dispatch is used as a fallback, even though it is slower.
func TestLRDispatchFallbackToPrimary(t *testing.T) {
	results := uint32(10)
	conn := connectionForDispatching(t, &fakeDispatchSvc{resultCount: results, dispatchCount: 1, sleepTime: 1 * time.Millisecond})
	secondaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 2, sleepTime: 0 * time.Millisecond, errorOnLR2: grpcstatus.Errorf(codes.NotFound, "not available")})

	parsed, err := ParseDispatchExpression("lookupresources", "['secondary']")
	require.NoError(t, err)

	dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
		KeyHandler:             &keys.DirectKeyHandler{},
		DispatchOverallTimeout: 30 * time.Second,
	}, map[string]SecondaryDispatch{
		"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
	}, map[string]*DispatchExpr{
		"lookupresources": parsed,
	}, 0*time.Second)
	require.NoError(t, err)
	require.True(t, dispatcher.ReadyState().IsReady)

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](context.Background())
	err = dispatcher.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
		ResourceRelation: &corev1.RelationReference{
			Namespace: "somenamespace",
			Relation:  "somerelation",
		},
		SubjectRelation: &corev1.RelationReference{
			Namespace: "somenamespace",
			Relation:  "somerelation",
		},
		SubjectIds: []string{"foo"},
		TerminalSubject: &corev1.ObjectAndRelation{
			Namespace: "foo",
			ObjectId:  "bar",
			Relation:  "...",
		},
		Metadata: &v1.ResolverMeta{DepthRemaining: 50},
	}, stream)
	require.NoError(t, err)

	require.Len(t, stream.Results(), int(results))
	require.Equal(t, uint32(1), stream.Results()[0].Metadata.DispatchCount)
	require.Equal(t, "0", stream.Results()[0].Resource.ResourceId)
}

func TestLSSecondaryDispatch(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		expr                  string
		request               *v1.DispatchLookupSubjectsRequest
		expectedDispatchCount uint32
		expectedError         bool
	}{
		{
			"no multidispatch",
			"['invalid']",
			&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				ResourceIds: []string{"foo"},
				Metadata:    &v1.ResolverMeta{DepthRemaining: 50},
			},
			1,
			false,
		},
		{
			"valid multidispatch",
			"['secondary']",
			&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				SubjectRelation: &corev1.RelationReference{
					Namespace: "somenamespace",
					Relation:  "somerelation",
				},
				ResourceIds: []string{"foo"},
				Metadata:    &v1.ResolverMeta{DepthRemaining: 50},
			},
			2,
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// NOTE: we configure the primary to be slow, so that the secondaries are used when applicable.
			conn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 1, sleepTime: 100 * time.Millisecond})
			secondaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 2, sleepTime: 0 * time.Millisecond})
			tertiaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 3, sleepTime: 0 * time.Millisecond})
			errorConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 4, sleepTime: 0 * time.Millisecond, errorOnLS: fmt.Errorf("error")})

			parsed, err := ParseDispatchExpression("lookupsubjects", tc.expr)
			require.NoError(t, err)

			dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
				KeyHandler:             &keys.DirectKeyHandler{},
				DispatchOverallTimeout: 30 * time.Second,
			}, map[string]SecondaryDispatch{
				"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
				"tertiary":  {Name: "tertiary", Client: v1.NewDispatchServiceClient(tertiaryConn)},
				"error":     {Name: "error", Client: v1.NewDispatchServiceClient(errorConn)},
			}, map[string]*DispatchExpr{
				"lookupsubjects": parsed,
			}, 0*time.Second)
			require.NoError(t, err)
			require.True(t, dispatcher.ReadyState().IsReady)

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](context.Background())
			err = dispatcher.DispatchLookupSubjects(tc.request, stream)

			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, len(stream.Results()))
				require.Equal(t, tc.expectedDispatchCount, stream.Results()[0].Metadata.DispatchCount)
			}
		})
	}
}

// TestLSDispatchFallbackToPrimary tests the case where the secondary dispatch returns
// an error, and the primary dispatch is used as a fallback, even though it is slower.
func TestLSDispatchFallbackToPrimary(t *testing.T) {
	results := uint32(10)
	conn := connectionForDispatching(t, &fakeDispatchSvc{resultCount: results, dispatchCount: 1, sleepTime: 1 * time.Millisecond})
	secondaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 2, sleepTime: 0 * time.Millisecond, errorOnLS: grpcstatus.Errorf(codes.NotFound, "not available")})

	parsed, err := ParseDispatchExpression("lookupsubjects", "['secondary']")
	require.NoError(t, err)

	dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
		KeyHandler:             &keys.DirectKeyHandler{},
		DispatchOverallTimeout: 30 * time.Second,
	}, map[string]SecondaryDispatch{
		"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
	}, map[string]*DispatchExpr{
		"lookupsubjects": parsed,
	}, 0*time.Second)
	require.NoError(t, err)
	require.True(t, dispatcher.ReadyState().IsReady)

	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](context.Background())
	err = dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
		ResourceRelation: &corev1.RelationReference{
			Namespace: "somenamespace",
			Relation:  "somerelation",
		},
		SubjectRelation: &corev1.RelationReference{
			Namespace: "somenamespace",
			Relation:  "somerelation",
		},
		ResourceIds: []string{"foo"},
		Metadata:    &v1.ResolverMeta{DepthRemaining: 50},
	}, stream)
	require.NoError(t, err)

	require.Len(t, stream.Results(), int(results))
	require.Equal(t, uint32(1), stream.Results()[0].Metadata.DispatchCount)
	require.Equal(t, "0", stream.Results()[0].FoundSubjectsByResourceId["foo"].FoundSubjects[0].SubjectId)
}

func TestCheckUsesDelayByDefaultForPrimary(t *testing.T) {
	conn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 1, sleepTime: 3 * time.Millisecond, raisePanic: true})
	secondaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 2, sleepTime: 3 * time.Millisecond})

	parsed, err := ParseDispatchExpression("check", "['secondary']")
	require.NoError(t, err)

	dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
		KeyHandler:             &keys.DirectKeyHandler{},
		DispatchOverallTimeout: 30 * time.Second,
	}, map[string]SecondaryDispatch{
		"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
	}, map[string]*DispatchExpr{
		"check": parsed,
	}, 0*time.Second)
	require.NoError(t, err)
	require.True(t, dispatcher.ReadyState().IsReady)

	// Dispatch the check, which should (since it is the first request) add a delay of ~5ms to
	// the primary, thus ensuring the secondary is used without any timeout for either,.
	resp, err := dispatcher.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
		ResourceRelation: &corev1.RelationReference{Namespace: "sometype", Relation: "somerel"},
		ResourceIds:      []string{"foo"},
		Metadata:         &v1.ResolverMeta{DepthRemaining: 50},
		Subject:          &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
	})
	require.NoError(t, err)
	require.Equal(t, uint32(2), resp.Metadata.DispatchCount)

	// Ensure the digest for the check was updated.
	cast := dispatcher.(*clusterDispatcher)
	require.Equal(t, uint64(1), cast.secondaryInitialResponseDigests["check"].digest.Count())
	require.GreaterOrEqual(t, cast.secondaryInitialResponseDigests["check"].digest.Quantile(0.99), float64(3))
}

func TestStreamingDispatchDelayByDefaultForPrimary(t *testing.T) {
	conn := connectionForDispatching(t, &fakeDispatchSvc{resultCount: 10, dispatchCount: 1, sleepTime: 3 * time.Millisecond})
	secondaryConn := connectionForDispatching(t, &fakeDispatchSvc{resultCount: 2, dispatchCount: 2, sleepTime: 3 * time.Millisecond})

	parsed, err := ParseDispatchExpression("lookupsubjects", "['secondary']")
	require.NoError(t, err)

	dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
		KeyHandler:             &keys.DirectKeyHandler{},
		DispatchOverallTimeout: 30 * time.Second,
	}, map[string]SecondaryDispatch{
		"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
	}, map[string]*DispatchExpr{
		"lookupsubjects": parsed,
	}, 0*time.Second)
	require.NoError(t, err)
	require.True(t, dispatcher.ReadyState().IsReady)

	// Dispatch the lookupsubjects, which should (since it is the first request) add a delay of ~5ms to
	// the primary, thus ensuring the secondary is used without any timeout for either,.
	stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](context.Background())
	err = dispatcher.DispatchLookupSubjects(&v1.DispatchLookupSubjectsRequest{
		ResourceRelation: &corev1.RelationReference{
			Namespace: "somenamespace",
			Relation:  "somerelation",
		},
		SubjectRelation: &corev1.RelationReference{
			Namespace: "somenamespace",
			Relation:  "somerelation",
		},
		ResourceIds: []string{"foo"},
		Metadata:    &v1.ResolverMeta{DepthRemaining: 50},
	}, stream)
	require.NoError(t, err)

	require.Equal(t, uint32(2), stream.Results()[0].Metadata.DispatchCount)
	require.Len(t, stream.Results(), 2)

	// Ensure the digest for the lookupsubjects was updated.
	cast := dispatcher.(*clusterDispatcher)
	require.Equal(t, uint64(1), cast.secondaryInitialResponseDigests["lookupsubjects"].digest.Count())
	require.GreaterOrEqual(t, cast.secondaryInitialResponseDigests["lookupsubjects"].digest.Quantile(0.99), float64(3))
}

func TestGetPrimaryWaitTime(t *testing.T) {
	conn := connectionForDispatching(t, &fakeDispatchSvc{resultCount: 10, dispatchCount: 1, sleepTime: 3 * time.Millisecond})
	secondaryConn := connectionForDispatching(t, &fakeDispatchSvc{resultCount: 2, dispatchCount: 2, sleepTime: 3 * time.Millisecond})

	parsed, err := ParseDispatchExpression("check", "['secondary']")
	require.NoError(t, err)

	d, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
		KeyHandler:             &keys.DirectKeyHandler{},
		DispatchOverallTimeout: 30 * time.Second,
	}, map[string]SecondaryDispatch{
		"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
	}, map[string]*DispatchExpr{
		"check": parsed,
	}, 0*time.Second)
	require.NoError(t, err)
	require.True(t, d.ReadyState().IsReady)

	dispatcher := d.(*clusterDispatcher)

	// Add a bunch of times to the secondary.
	for i := 0; i < 100; i++ {
		dispatcher.secondaryInitialResponseDigests["check"].addResultTime(2 * time.Millisecond)
	}

	// Ensure the primary wait time is ~=2ms.
	require.GreaterOrEqual(t, dispatcher.getPrimaryWaitTime("check", tuple.RR("document", "viewer"), tuple.RR("user", "...")), 2*time.Millisecond)

	// Mark document#viewer as unsupported.
	dispatcher.supportedResourceSubjectTracker.updateForError(testResourceRelationError{fmt.Errorf("foo"), "document", "viewer"})

	// Ensure the primary wait time for document#viewer is now 0ms.
	require.Equal(t, 0*time.Millisecond, dispatcher.getPrimaryWaitTime("check", tuple.RR("document", "viewer"), tuple.RR("user", "...")))

	// Ensure the primary wait time for document#editor is still ~=2ms.
	require.GreaterOrEqual(t, dispatcher.getPrimaryWaitTime("check", tuple.RR("document", "editor"), tuple.RR("user", "...")), 2*time.Millisecond)

	// Mark document#viewer as supported again.
	dispatcher.supportedResourceSubjectTracker.updateForSuccess(tuple.RR("document", "viewer"), tuple.RR("user", "..."))

	// Ensure the primary wait time for document#viewer is now ~=2ms.
	require.GreaterOrEqual(t, dispatcher.getPrimaryWaitTime("check", tuple.RR("document", "viewer"), tuple.RR("user", "...")), 2*time.Millisecond)
}

func connectionForDispatching(t *testing.T, svc v1.DispatchServiceServer) *grpc.ClientConn {
	listener := bufconn.Listen(humanize.MiByte)
	s := grpc.NewServer()

	v1.RegisterDispatchServiceServer(s, svc)

	go func() {
		// Ignore any errors
		_ = s.Serve(listener)
	}()

	conn, err := grpchelpers.DialAndWait(
		context.Background(),
		"",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
		listener.Close()
		s.Stop()
	})

	return conn
}

func TestSupportedResourceSubjectTracker(t *testing.T) {
	tracker := newSupportedResourceSubjectTracker()

	// Ensure nothing is unsupported.
	require.False(t, tracker.isUnsupported(tuple.RR("document", "viewer"), tuple.RR("user", "...")))

	// Add a random error, which should change nothing.
	tracker.updateForError(fmt.Errorf("some random error"))

	require.False(t, tracker.isUnsupported(tuple.RR("document", "viewer"), tuple.RR("user", "...")))

	// Add an unsupported resource, which should be marked as such.
	unsupportedResourceError := testResourceRelationError{fmt.Errorf("foo"), "document", "viewer"}
	tracker.updateForError(unsupportedResourceError)

	require.True(t, tracker.isUnsupported(tuple.RR("document", "viewer"), tuple.RR("user", "...")))

	// Mark the resource as supported, which should remove the error.
	tracker.updateForSuccess(tuple.RR("document", "viewer"), tuple.RR("user", "..."))

	require.False(t, tracker.isUnsupported(tuple.RR("document", "viewer"), tuple.RR("user", "...")))

	// Mark a subject as unsupported, which should be marked as such.
	unsupportedSubjectError := testSubjectRelationError{fmt.Errorf("foo"), "user", "..."}
	tracker.updateForError(unsupportedSubjectError)

	require.True(t, tracker.isUnsupported(tuple.RR("document", "viewer"), tuple.RR("user", "...")))

	// Mark another resource as unsupported, which should be marked as such.
	unsupportedResourceError2 := testResourceRelationError{fmt.Errorf("foo"), "document", "editor"}
	tracker.updateForError(unsupportedResourceError2)

	require.True(t, tracker.isUnsupported(tuple.RR("document", "viewer"), tuple.RR("user", "...")))
	require.True(t, tracker.isUnsupported(tuple.RR("document", "editor"), tuple.RR("user", "...")))
	require.True(t, tracker.isUnsupported(tuple.RR("document", "editor"), tuple.RR("user2", "...")))
	require.False(t, tracker.isUnsupported(tuple.RR("document", "viewer"), tuple.RR("user2", "...")))

	// Mark the subject as supported, which should remove the error.
	tracker.updateForSuccess(tuple.RR("document", "viewer"), tuple.RR("user", "..."))

	require.False(t, tracker.isUnsupported(tuple.RR("document", "viewer"), tuple.RR("user", "...")))
	require.True(t, tracker.isUnsupported(tuple.RR("document", "editor"), tuple.RR("user", "...")))
	require.True(t, tracker.isUnsupported(tuple.RR("document", "editor"), tuple.RR("user2", "...")))
	require.False(t, tracker.isUnsupported(tuple.RR("document", "viewer"), tuple.RR("user2", "...")))
}

func TestSupportedResourceSubjectTrackerParallelUpdates(t *testing.T) {
	tracker := newSupportedResourceSubjectTracker()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < 100; i++ {
			tracker.updateForError(testResourceRelationError{fmt.Errorf("foo"), "document", "viewer"})
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < 100; i++ {
			tracker.updateForSuccess(tuple.RR("document", "editor"), tuple.RR("user", "..."))
		}
	}()

	wg.Wait()

	require.True(t, tracker.isUnsupported(tuple.RR("document", "viewer"), tuple.RR("user", "...")))
	require.False(t, tracker.isUnsupported(tuple.RR("document", "editor"), tuple.RR("user", "...")))
}

func TestCheckToUnsupportedRemovesHedgingDelay(t *testing.T) {
	conn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 1, sleepTime: 1 * time.Millisecond})
	secondaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 2, sleepTime: 0 * time.Millisecond, errorOnCheck: testResourceRelationError{
		fmt.Errorf("foo"), "sometype", "somerel",
	}})

	parsed, err := ParseDispatchExpression("check", "['secondary']")
	require.NoError(t, err)

	dispatcher, err := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
		KeyHandler:             &keys.DirectKeyHandler{},
		DispatchOverallTimeout: 30 * time.Second,
	}, map[string]SecondaryDispatch{
		"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
	}, map[string]*DispatchExpr{
		"check": parsed,
	}, 25*time.Millisecond) // NOTE: We use 25ms to reduce the risk of flakiness
	require.NoError(t, err)
	require.True(t, dispatcher.ReadyState().IsReady)

	// Dispatch the check, which should (since it is the first request) add a delay of ~25ms to
	// the primary, but fallback to the primary on the error.
	startTime := time.Now()
	resp, err := dispatcher.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
		ResourceRelation: &corev1.RelationReference{Namespace: "sometype", Relation: "somerel"},
		ResourceIds:      []string{"foo"},
		Metadata:         &v1.ResolverMeta{DepthRemaining: 50},
		Subject:          &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
	})
	endTime := time.Now()
	require.NoError(t, err)
	require.Equal(t, uint32(1), resp.Metadata.DispatchCount)
	require.GreaterOrEqual(t, endTime.Sub(startTime), 25*time.Millisecond)

	// Ensure the resource relation was marked as unsupported.
	cast := dispatcher.(*clusterDispatcher)
	require.True(t, cast.supportedResourceSubjectTracker.isUnsupported(tuple.RR("sometype", "somerel"), tuple.RR("foo", "bar")))
	require.False(t, cast.supportedResourceSubjectTracker.isUnsupported(tuple.RR("someothertype", "somerel"), tuple.RR("foo", "bar")))

	// Dispatch again, which should hit the primary without any delay.
	startTime = time.Now()
	resp, err = dispatcher.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
		ResourceRelation: &corev1.RelationReference{Namespace: "sometype", Relation: "somerel"},
		ResourceIds:      []string{"foo"},
		Metadata:         &v1.ResolverMeta{DepthRemaining: 50},
		Subject:          &corev1.ObjectAndRelation{Namespace: "foo", ObjectId: "bar", Relation: "..."},
	})
	endTime = time.Now()
	require.NoError(t, err)
	require.Equal(t, uint32(1), resp.Metadata.DispatchCount)
	require.LessOrEqual(t, endTime.Sub(startTime), 25*time.Millisecond)
}

type testResourceRelationError struct {
	error

	definitionName string
	relationName   string
}

func (err testResourceRelationError) GRPCStatus() *grpcstatus.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.FailedPrecondition,
		&errdetails.ErrorInfo{
			Reason: "UNSUPPORTED_RESOURCE_RELATION",
			Domain: "somedomain",
			Metadata: map[string]string{
				"definition_name": err.definitionName,
				"relation_name":   err.relationName,
			},
		},
	)
}

type testSubjectRelationError struct {
	error

	definitionName string
	relationName   string
}

func (err testSubjectRelationError) GRPCStatus() *grpcstatus.Status {
	return spiceerrors.WithCodeAndDetails(
		err,
		codes.FailedPrecondition,
		&errdetails.ErrorInfo{
			Reason: "UNSUPPORTED_SUBJECT_RELATION",
			Domain: "somedomain",
			Metadata: map[string]string{
				"definition_name": err.definitionName,
				"relation_name":   err.relationName,
			},
		},
	)
}

func TestDALCount(t *testing.T) {
	digest, err := tdigest.New(tdigest.Compression(1000))
	require.NoError(t, err)
	dal := &digestAndLock{
		digest: digest,
		lock:   sync.RWMutex{},
	}

	for i := 0; i < minimumDigestCount-1; i++ {
		uintValue, err := safecast.ToUint64(i + 1)
		require.NoError(t, err)

		dal.addResultTime(3 * time.Millisecond)
		require.Equal(t, uintValue, dal.digest.Count())
		require.Equal(t, dal.startingPrimaryHedgingDelay, dal.getWaitTime())
	}

	// Add the next result, which pushes it over the minimum count and now uses the quantile.
	dal.addResultTime(3 * time.Millisecond)
	require.Equal(t, uint64(minimumDigestCount), dal.digest.Count())
	require.Equal(t, 3*time.Millisecond, dal.getWaitTime())
}

func BenchmarkDAL(b *testing.B) {
	digest, err := tdigest.New(tdigest.Compression(1000))
	require.NoError(b, err)
	dal := &digestAndLock{
		digest: digest,
		lock:   sync.RWMutex{},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dal.addResultTime(time.Duration(i) * time.Millisecond)
	}
}
