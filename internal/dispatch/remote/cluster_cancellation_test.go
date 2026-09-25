package remote

import (
	"context"
	"io"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/datalayer"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestLookupResources3CancellationDoesNotCachePartialResults(t *testing.T) {
	const (
		batchSize   = 500
		resultCount = 834
	)
	recvError := status.Error(codes.Canceled, "receive canceled")
	for _, winningDispatcher := range []string{"primary", "secondary"} {
		for _, tc := range []struct {
			name          string
			expectedError error
			expectedRecvs int
		}{
			{"internal_deadline_between_receives", context.DeadlineExceeded, 1},
			{"parent_cancel_between_receives", context.Canceled, 1},
			{"parent_cancel_before_dispatch", context.Canceled, 0},
			{"recv_error", recvError, 2},
			{"successful_eof", nil, 3},
		} {
			t.Run(winningDispatcher+"/"+tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					loserStarted := make(chan context.Context, 1)
					winner := &cancellationLR3Client{
						waitForLoser: loserStarted,
						batchSize:    batchSize,
						resultCount:  resultCount,
					}
					if tc.name == "recv_error" {
						winner.recvError = recvError
					}
					loser := &cancellationLR3Client{started: loserStarted}
					primary, secondary := winner, loser
					if winningDispatcher == "secondary" {
						primary, secondary = loser, winner
					}

					expr, err := ParseDispatchExpression("lookupresources", "['secondary']")
					require.NoError(t, err)
					remote, err := NewClusterDispatcher(primary, nil, ClusterDispatcherConfig{
						DispatchOverallTimeout: time.Second,
					}, map[string]SecondaryDispatch{
						"secondary": {Name: "secondary", Client: secondary},
					}, map[string]*DispatchExpr{"lookupresources": expr}, time.Millisecond)
					require.NoError(t, err)
					cache := caching.DispatchTestCache(t)
					defer cache.Close()
					cached, err := caching.NewCachingDispatcher(cache, dispatch.MetricsOptions{}, &keys.DirectKeyHandler{})
					require.NoError(t, err)
					cached.SetDelegate(remote)

					req := &v1.DispatchLookupResources3Request{
						ResourceRelation: &corev1.RelationReference{Namespace: "document", Relation: "view"},
						SubjectRelation:  &corev1.RelationReference{Namespace: "user", Relation: "..."},
						SubjectIds:       []string{"subject"},
						TerminalSubject:  &corev1.ObjectAndRelation{Namespace: "user", ObjectId: "subject", Relation: "..."},
						Metadata: &v1.ResolverMeta{
							AtRevision: "1", DepthRemaining: 50, SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
						},
						OptionalLimit: 10000,
					}
					key, err := (&keys.DirectKeyHandler{}).LookupResources3CacheKey(t.Context(), req)
					require.NoError(t, err)
					parent, cancel := context.WithCancel(t.Context())
					defer cancel()
					if tc.name == "parent_cancel_before_dispatch" {
						cancel()
					}
					var firstIDs []string
					first := dispatch.NewHandlingDispatchStream(parent, func(response *v1.DispatchLookupResources3Response) error {
						for _, item := range response.Items {
							firstIDs = append(firstIDs, item.ResourceId)
						}
						// Cancellation occurs inside Publish, so the next receive has not begun.
						switch tc.name {
						case "internal_deadline_between_receives":
							<-winner.handlerContext.Done()
						case "parent_cancel_between_receives":
							cancel()
						}
						return nil
					})
					err = cached.DispatchLookupResources3(req, first)
					_, found := cache.Get(key)
					if tc.expectedError != nil {
						require.ErrorIs(t, err, tc.expectedError)
						if tc.expectedRecvs == 0 {
							require.Empty(t, firstIDs)
						} else {
							require.Len(t, firstIDs, batchSize)
						}
						require.False(t, found, "an incomplete stream must not be cached")
					} else {
						require.NoError(t, err)
						require.Len(t, firstIDs, resultCount)
						require.True(t, found, "a stream completed by EOF should be cached")
					}
					require.Equal(t, tc.expectedRecvs, winner.recvs)
					expectedCalls := 1
					if tc.name == "parent_cancel_before_dispatch" {
						expectedCalls = 0
						require.Zero(t, loser.calls)
					} else {
						require.ErrorIs(t, winner.loserContext.Err(), context.Canceled)
					}
					require.Equal(t, expectedCalls, winner.calls)
					if tc.name == "parent_cancel_between_receives" || tc.name == "parent_cancel_before_dispatch" {
						require.ErrorIs(t, parent.Err(), context.Canceled)
					} else {
						require.NoError(t, parent.Err(), "internal failures must leave the parent healthy")
					}

					expectedIDs := make([]string, resultCount)
					for i := range expectedIDs {
						expectedIDs[i] = strconv.Itoa(i)
					}
					if tc.expectedError != nil {
						expectedCalls++
					}
					// A fresh request retries a failed stream; the next request replays only
					// the complete cached result, without another backend call.
					for range 2 {
						var resultIDs []string
						fresh := dispatch.NewHandlingDispatchStream(t.Context(), func(response *v1.DispatchLookupResources3Response) error {
							for _, item := range response.Items {
								resultIDs = append(resultIDs, item.ResourceId)
							}
							return nil
						})
						require.NoError(t, cached.DispatchLookupResources3(req, fresh))
						require.Equal(t, expectedIDs, resultIDs)
						require.Equal(t, expectedCalls, winner.calls)
					}
				})
			})
		}
	}
}

// cancellationLR3Client either streams two batches or waits for cancellation.
// Its winner waits for the losing dispatcher to start, ensuring every test exercises
// cancellation of an active losing dispatcher as well as the winner's outcome.
type cancellationLR3Client struct {
	ClusterClient
	started        chan<- context.Context
	waitForLoser   <-chan context.Context
	batchSize      int
	resultCount    int
	recvError      error
	calls          int
	recvs          int
	handlerContext context.Context
	loserContext   context.Context
}

func (c *cancellationLR3Client) DispatchLookupResources3(ctx context.Context, _ *v1.DispatchLookupResources3Request, _ ...grpc.CallOption) (v1.DispatchService_DispatchLookupResources3Client, error) {
	c.calls++
	c.handlerContext = ctx
	if c.started != nil {
		c.started <- ctx
	}
	return &cancellationLR3Receiver{ctx: ctx, client: c}, nil
}

type cancellationLR3Receiver struct {
	grpc.ClientStream
	ctx    context.Context
	client *cancellationLR3Client
	index  int
}

func (r *cancellationLR3Receiver) Recv() (*v1.DispatchLookupResources3Response, error) {
	r.client.recvs++
	if r.client.started != nil {
		<-r.ctx.Done()
		return nil, r.ctx.Err()
	}
	if r.index == 0 {
		r.client.loserContext = <-r.client.waitForLoser
	} else if r.client.calls == 1 && r.client.recvError != nil {
		return nil, r.client.recvError
	}
	if r.index >= r.client.resultCount {
		return nil, io.EOF
	}
	count := min(r.client.batchSize, r.client.resultCount-r.index)
	items := make([]*v1.LR3Item, 0, count)
	for range count {
		items = append(items, &v1.LR3Item{
			ResourceId:                  strconv.Itoa(r.index),
			ForSubjectIds:               []string{"subject"},
			AfterResponseCursorSections: []string{strconv.Itoa(r.index + 1)},
		})
		r.index++
	}
	return &v1.DispatchLookupResources3Response{Items: items}, nil
}
