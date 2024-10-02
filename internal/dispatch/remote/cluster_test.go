package remote

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/dispatch"

	humanize "github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/internal/grpchelpers"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type fakeDispatchSvc struct {
	v1.UnimplementedDispatchServiceServer

	sleepTime     time.Duration
	dispatchCount uint32
}

func (fds *fakeDispatchSvc) DispatchCheck(context.Context, *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	time.Sleep(fds.sleepTime)
	return &v1.DispatchCheckResponse{
		Metadata: &v1.ResponseMeta{
			DispatchCount: fds.dispatchCount,
		},
	}, nil
}

func (fds *fakeDispatchSvc) DispatchLookupSubjects(_ *v1.DispatchLookupSubjectsRequest, srv v1.DispatchService_DispatchLookupSubjectsServer) error {
	time.Sleep(fds.sleepTime)
	return srv.Send(&v1.DispatchLookupSubjectsResponse{
		Metadata: emptyMetadata,
	})
}

func (fds *fakeDispatchSvc) DispatchLookupResources2(_ *v1.DispatchLookupResources2Request, srv v1.DispatchService_DispatchLookupResources2Server) error {
	if fds.dispatchCount == 999 {
		return fmt.Errorf("error")
	}

	time.Sleep(fds.sleepTime)
	return srv.Send(&v1.DispatchLookupResources2Response{
		Metadata: &v1.ResponseMeta{
			DispatchCount: fds.dispatchCount,
		},
		AfterResponseCursor: &v1.Cursor{
			Sections:        nil,
			DispatchVersion: 1,
		},
	})
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
			dispatcher := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
				KeyHandler:             &keys.DirectKeyHandler{},
				DispatchOverallTimeout: tc.timeout,
			}, nil, nil)
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

			dispatcher := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
				KeyHandler:             &keys.DirectKeyHandler{},
				DispatchOverallTimeout: 30 * time.Second,
			}, map[string]SecondaryDispatch{
				"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
			}, map[string]*DispatchExpr{
				"check": parsed,
			})
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			conn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 1, sleepTime: 0 * time.Millisecond})
			secondaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 2, sleepTime: 0 * time.Millisecond})
			tertiaryConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 3, sleepTime: 0 * time.Millisecond})
			errorConn := connectionForDispatching(t, &fakeDispatchSvc{dispatchCount: 999, sleepTime: 0 * time.Millisecond})

			parsed, err := ParseDispatchExpression("lookupresources", tc.expr)
			require.NoError(t, err)

			dispatcher := NewClusterDispatcher(v1.NewDispatchServiceClient(conn), conn, ClusterDispatcherConfig{
				KeyHandler:             &keys.DirectKeyHandler{},
				DispatchOverallTimeout: 30 * time.Second,
			}, map[string]SecondaryDispatch{
				"secondary": {Name: "secondary", Client: v1.NewDispatchServiceClient(secondaryConn)},
				"tertiary":  {Name: "tertiary", Client: v1.NewDispatchServiceClient(tertiaryConn)},
				"error":     {Name: "error", Client: v1.NewDispatchServiceClient(errorConn)},
			}, map[string]*DispatchExpr{
				"lookupresources": parsed,
			})
			require.True(t, dispatcher.ReadyState().IsReady)

			stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](context.Background())
			err = dispatcher.DispatchLookupResources2(tc.request, stream)

			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, len(stream.Results()))
				require.Equal(t, tc.expectedDispatchCount, stream.Results()[0].Metadata.DispatchCount)
			}
		})
	}
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
