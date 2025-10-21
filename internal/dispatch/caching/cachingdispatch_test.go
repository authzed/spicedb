package caching

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type checkRequest struct {
	start             string
	goal              string
	atRevision        decimal.Decimal
	depthRequired     uint32
	depthRemaining    uint32
	expectPassthrough bool
}

func RR(namespaceName string, relationName string) *core.RelationReference {
	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

func TestMaxDepthCaching(t *testing.T) {
	start1 := "document:doc1#read"
	start2 := "document:doc2#read"
	user1 := "user:user1#..."
	user2 := "user:user2#..."

	testCases := []struct {
		name   string
		script []checkRequest
	}{
		{"single request", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
		}},
		{"two requests, hit", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
			{start1, user1, decimal.Zero, 1, 50, false},
		}},
		{"many requests, hit", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
			{start1, user1, decimal.Zero, 1, 50, false},
			{start1, user1, decimal.Zero, 1, 50, false},
			{start1, user1, decimal.Zero, 1, 50, false},
			{start1, user1, decimal.Zero, 1, 50, false},
		}},
		{"multiple keys", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
			{start2, user2, decimal.Zero, 1, 50, true},
		}},
		{"same object, different revisions miss", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
			{start1, user1, decimal.NewFromInt(50), 1, 50, true},
		}},
		{"interleaved objects, hit", []checkRequest{
			{start1, user1, decimal.Zero, 1, 50, true},
			{start2, user2, decimal.Zero, 1, 50, true},
			{start1, user1, decimal.Zero, 1, 50, false},
			{start2, user2, decimal.Zero, 1, 50, false},
		}},
		{"insufficient depth", []checkRequest{
			{start1, user1, decimal.Zero, 21, 50, true},
			{start1, user1, decimal.Zero, 21, 20, true},
		}},
		{"sufficient depth", []checkRequest{
			{start1, user1, decimal.Zero, 1, 40, true},
			{start1, user1, decimal.Zero, 1, 50, false},
		}},
		{"updated cached depth", []checkRequest{
			{start1, user1, decimal.Zero, 21, 50, true},
			{start1, user1, decimal.Zero, 21, 40, false},
			{start1, user1, decimal.Zero, 21, 20, true},
			{start1, user1, decimal.Zero, 21, 50, false},
		}},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			delegate := delegateDispatchMock{&mock.Mock{}}

			for _, step := range tc.script {
				if step.expectPassthrough {
					parsed, err := tuple.ParseONR(step.start)
					require.NoError(err)

					delegate.On("DispatchCheck", &v1.DispatchCheckRequest{
						ResourceRelation: RR(parsed.ObjectType, parsed.Relation),
						ResourceIds:      []string{parsed.ObjectID},
						Subject:          tuple.MustParseSubjectONR(step.goal).ToCoreONR(),
						Metadata: &v1.ResolverMeta{
							AtRevision:     step.atRevision.String(),
							DepthRemaining: step.depthRemaining,
						},
					}).Return(&v1.DispatchCheckResponse{
						ResultsByResourceId: map[string]*v1.ResourceCheckResult{
							parsed.ObjectID: {
								Membership: v1.ResourceCheckResult_MEMBER,
							},
						},
						Metadata: &v1.ResponseMeta{
							DispatchCount: 1,
							DepthRequired: step.depthRequired,
						},
					}, nil).Times(1)
				}
			}

			dispatch, err := NewCachingDispatcher(DispatchTestCache(t), false, "", nil)
			dispatch.SetDelegate(delegate)
			require.NoError(err)
			defer dispatch.Close()

			for _, step := range tc.script {
				parsed, err := tuple.ParseONR(step.start)
				require.NoError(err)

				resp, err := dispatch.DispatchCheck(t.Context(), &v1.DispatchCheckRequest{
					ResourceRelation: RR(parsed.ObjectType, parsed.Relation),
					ResourceIds:      []string{parsed.ObjectID},
					Subject:          tuple.MustParseSubjectONR(step.goal).ToCoreONR(),
					Metadata: &v1.ResolverMeta{
						AtRevision:     step.atRevision.String(),
						DepthRemaining: step.depthRemaining,
					},
				})
				require.NoError(err)
				require.Equal(v1.ResourceCheckResult_MEMBER, resp.ResultsByResourceId[parsed.ObjectID].Membership)

				// We have to sleep a while to let the cache converge
				time.Sleep(10 * time.Millisecond)
			}

			delegate.AssertExpectations(t)
		})
	}
}

func TestConcurrentDebugInfoAccess(t *testing.T) {
	require := require.New(t)

	delegate := delegateDispatchMock{&mock.Mock{}}

	originalReq := &v1.DispatchCheckRequest{
		ResourceIds: []string{"original"},
		Subject: &core.ObjectAndRelation{
			Relation: "original",
		},
	}

	// Have the delegate return one request object for all calls
	delegate.On("DispatchCheck", mock.Anything).
		Return(&v1.DispatchCheckResponse{
			Metadata: &v1.ResponseMeta{
				DispatchCount: 1,
				DebugInfo:     &v1.DebugInformation{Check: &v1.CheckDebugTrace{Request: originalReq}},
			},
		}, nil)

	dispatcher, err := NewCachingDispatcher(DispatchTestCache(t), false, "", nil)
	require.NoError(err)
	dispatcher.SetDelegate(delegate)
	t.Cleanup(func() {
		_ = dispatcher.Close()
	})

	// Spawn multiple goroutines that create requests with slice views of the same backing array
	// This simulates what happens in ComputeBulkCheck with ForEachChunkUntil
	const numGoroutines = 1000
	errors := make(chan error, numGoroutines)

	var wg sync.WaitGroup
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			request := &v1.DispatchCheckRequest{
				ResourceRelation: RR("document", "viewer"),
				ResourceIds:      []string{"doc1"},
				Subject:          tuple.MustParseSubjectONR("user:alice").ToCoreONR(),
				Metadata: &v1.ResolverMeta{
					AtRevision:     decimal.Zero.String(),
					DepthRemaining: 50,
				},
				Debug: v1.DispatchCheckRequest_ENABLE_BASIC_DEBUGGING,
			}

			resp, err := dispatcher.DispatchCheck(context.Background(), request)
			if err != nil {
				errors <- err
				return
			}

			require.NotNil(resp.GetMetadata().GetDebugInfo().GetCheck().GetRequest())

			// we mutate the response to prove that it's not shared across goroutines
			resp.GetMetadata().GetDebugInfo().GetCheck().GetRequest().Subject.Relation = "modified"
			resp.GetMetadata().GetDebugInfo().GetCheck().GetRequest().ResourceIds = []string{"modified"}
		}(i)
	}

	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(err)
	}
}

type delegateDispatchMock struct {
	*mock.Mock
}

func (ddm delegateDispatchMock) DispatchCheck(_ context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	args := ddm.Called(req)
	return args.Get(0).(*v1.DispatchCheckResponse), args.Error(1)
}

func (ddm delegateDispatchMock) DispatchExpand(_ context.Context, _ *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{}, nil
}

func (ddm delegateDispatchMock) DispatchLookupResources2(_ *v1.DispatchLookupResources2Request, _ dispatch.LookupResources2Stream) error {
	return nil
}

func (ddm delegateDispatchMock) DispatchLookupResources3(_ *v1.DispatchLookupResources3Request, _ dispatch.LookupResources3Stream) error {
	return nil
}

func (ddm delegateDispatchMock) DispatchLookupSubjects(_ *v1.DispatchLookupSubjectsRequest, _ dispatch.LookupSubjectsStream) error {
	return nil
}

func (ddm delegateDispatchMock) Close() error {
	return nil
}

func (ddm delegateDispatchMock) ReadyState() dispatch.ReadyState {
	return dispatch.ReadyState{IsReady: true}
}

var _ dispatch.Dispatcher = &delegateDispatchMock{}
