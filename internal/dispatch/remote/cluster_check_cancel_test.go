package remote

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/pkg/datalayer"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// hedgedCheckClient answers with resp or err, or, when neither is set, blocks until its
// context is done and fails the way a gRPC client does.
type hedgedCheckClient struct {
	ClusterClient
	resp    *v1.DispatchCheckResponse
	err     error
	started chan struct{}
	calls   atomic.Int32
}

func (c *hedgedCheckClient) DispatchCheck(ctx context.Context, _ *v1.DispatchCheckRequest, _ ...grpc.CallOption) (*v1.DispatchCheckResponse, error) {
	c.calls.Add(1)
	c.started <- struct{}{}
	if c.resp != nil || c.err != nil {
		return c.resp, c.err
	}
	<-ctx.Done()
	return &v1.DispatchCheckResponse{}, status.FromContextError(ctx.Err()).Err()
}

func TestHedgedCheckReturnsContextErrorWhenNoDispatcherAnswers(t *testing.T) {
	primaryErr := errors.New("primary failed")
	for _, tc := range []struct {
		name             string
		callerCancel     bool
		callerTimeout    time.Duration
		dispatchTimeout  time.Duration
		primary          *hedgedCheckClient
		secondary        *hedgedCheckClient
		expectedError    error
		expectedPrimary  int32
		expectedDispatch uint32
	}{
		{
			name:          "caller canceled",
			callerCancel:  true,
			expectedError: context.Canceled,
		},
		{
			name:          "caller deadline exceeded",
			callerTimeout: time.Second,
			expectedError: context.DeadlineExceeded,
		},
		{
			name:            "dispatch timeout exceeded",
			dispatchTimeout: time.Second,
			expectedError:   context.DeadlineExceeded,
		},
		{
			name:             "secondary answers",
			secondary:        &hedgedCheckClient{resp: &v1.DispatchCheckResponse{Metadata: &v1.ResponseMeta{DispatchCount: 2}}},
			expectedDispatch: 2,
		},
		{
			name:            "primary error returned after secondary error",
			dispatchTimeout: 2 * time.Hour,
			primary:         &hedgedCheckClient{err: primaryErr},
			secondary:       &hedgedCheckClient{err: errors.New("secondary failed")},
			expectedError:   primaryErr,
			expectedPrimary: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				primary, secondary := tc.primary, tc.secondary
				if primary == nil {
					primary = &hedgedCheckClient{}
				}
				if secondary == nil {
					secondary = &hedgedCheckClient{}
				}
				primary.started = make(chan struct{}, 1)
				secondary.started = make(chan struct{}, 1)

				dispatchTimeout := tc.dispatchTimeout
				if dispatchTimeout == 0 {
					dispatchTimeout = time.Hour
				}
				expr, err := ParseDispatchExpression("check", "['secondary']")
				require.NoError(t, err)
				d, err := NewClusterDispatcher(primary, nil, ClusterDispatcherConfig{
					DispatchOverallTimeout: dispatchTimeout,
				}, map[string]SecondaryDispatch{
					"secondary": {Name: "secondary", Client: secondary, MaximumPrimaryHedgingDelay: time.Hour},
				}, map[string]*DispatchExpr{"check": expr}, time.Hour)
				require.NoError(t, err)

				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tc.callerTimeout > 0 {
					ctx, cancel = context.WithTimeout(ctx, tc.callerTimeout)
					defer cancel()
				}
				if tc.callerCancel {
					go func() {
						<-secondary.started
						cancel()
					}()
				}

				resp, err := d.DispatchCheck(ctx, &v1.DispatchCheckRequest{
					ResourceRelation: &corev1.RelationReference{Namespace: "document", Relation: "view"},
					ResourceIds:      []string{"doc"},
					Subject:          &corev1.ObjectAndRelation{Namespace: "user", ObjectId: "tom", Relation: "..."},
					Metadata:         &v1.ResolverMeta{DepthRemaining: 50, SchemaHash: []byte(datalayer.NoSchemaHashForTesting)},
				})
				if tc.expectedError != nil {
					require.ErrorIs(t, err, tc.expectedError)
				} else {
					require.NoError(t, err)
					require.Equal(t, tc.expectedDispatch, resp.Metadata.DispatchCount)
				}
				require.Equal(t, int32(1), secondary.calls.Load())
				synctest.Wait()
				require.Equal(t, tc.expectedPrimary, primary.calls.Load())
			})
		})
	}
}
