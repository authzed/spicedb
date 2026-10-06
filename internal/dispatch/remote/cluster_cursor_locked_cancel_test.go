package remote

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/datalayer"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestCursorLockedLookupResources2CanceledBeforeResultsReturnsContextError(t *testing.T) {
	for _, tc := range []struct {
		name            string
		overallTimeout  time.Duration
		callerDeadline  time.Duration
		cancelOnStarted bool
		expectedError   error
	}{
		{"caller_canceled", time.Hour, 0, true, context.Canceled},
		{"caller_deadline_exceeded", time.Hour, time.Second, false, context.DeadlineExceeded},
		{"dispatch_overall_timeout", time.Second, 0, false, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				primary := &blockingLR2Client{}
				secondary := &blockingLR2Client{started: make(chan struct{}, 1)}

				expr, err := ParseDispatchExpression("lookupresources", "['secondary']")
				require.NoError(t, err)
				remote, err := NewClusterDispatcher(primary, nil, ClusterDispatcherConfig{
					KeyHandler:             &keys.DirectKeyHandler{},
					DispatchOverallTimeout: tc.overallTimeout,
				}, map[string]SecondaryDispatch{
					"secondary": {Name: "secondary", Client: secondary},
				}, map[string]*DispatchExpr{"lookupresources": expr}, time.Millisecond)
				require.NoError(t, err)

				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tc.callerDeadline > 0 {
					ctx, cancel = context.WithTimeout(ctx, tc.callerDeadline)
					defer cancel()
				}
				if tc.cancelOnStarted {
					go func() {
						<-secondary.started
						cancel()
					}()
				}

				stream := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupResources2Response](ctx)
				err = remote.DispatchLookupResources2(&v1.DispatchLookupResources2Request{
					ResourceRelation: &corev1.RelationReference{Namespace: "document", Relation: "view"},
					SubjectRelation:  &corev1.RelationReference{Namespace: "user", Relation: "..."},
					SubjectIds:       []string{"subject"},
					TerminalSubject:  &corev1.ObjectAndRelation{Namespace: "user", ObjectId: "subject", Relation: "..."},
					Metadata: &v1.ResolverMeta{
						AtRevision: "1", DepthRemaining: 50, SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					OptionalCursor: &v1.Cursor{Sections: []string{secondaryCursorPrefix + "secondary"}, DispatchVersion: 1},
				}, stream)
				require.ErrorIs(t, err, tc.expectedError)
				require.Empty(t, stream.Results())
				require.Equal(t, 1, secondary.calls)
				require.Zero(t, primary.calls, "a cursor locked to a secondary must not dispatch to the primary")
			})
		})
	}
}

// blockingLR2Client returns a stream that yields no results until its context is done.
type blockingLR2Client struct {
	ClusterClient
	started chan struct{}
	calls   int
}

func (c *blockingLR2Client) DispatchLookupResources2(ctx context.Context, _ *v1.DispatchLookupResources2Request, _ ...grpc.CallOption) (v1.DispatchService_DispatchLookupResources2Client, error) {
	c.calls++
	if c.started != nil {
		c.started <- struct{}{}
	}
	return &blockingLR2Receiver{ctx: ctx}, nil
}

type blockingLR2Receiver struct {
	grpc.ClientStream
	ctx context.Context
}

func (r *blockingLR2Receiver) Recv() (*v1.DispatchLookupResources2Response, error) {
	<-r.ctx.Done()
	return nil, status.FromContextError(r.ctx.Err()).Err()
}
