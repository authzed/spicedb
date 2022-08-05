package caching

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
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
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			delegate := delegateDispatchMock{&mock.Mock{}}

			for _, step := range tc.script {
				if step.expectPassthrough {
					delegate.On("DispatchCheck", &v1.DispatchCheckRequest{
						ResourceAndRelation: tuple.ParseONR(step.start),
						Subject:             tuple.ParseSubjectONR(step.goal),
						Metadata: &v1.ResolverMeta{
							AtRevision:     step.atRevision.String(),
							DepthRemaining: step.depthRemaining,
						},
					}).Return(&v1.DispatchCheckResponse{
						Membership: v1.DispatchCheckResponse_MEMBER,
						Metadata: &v1.ResponseMeta{
							DispatchCount: 1,
							DepthRequired: step.depthRequired,
						},
					}, nil).Times(1)
				}
			}

			dispatch, err := NewCachingDispatcher(nil, "", nil)
			dispatch.SetDelegate(delegate)
			require.NoError(err)
			defer dispatch.Close()

			for _, step := range tc.script {
				resp, err := dispatch.DispatchCheck(context.Background(), &v1.DispatchCheckRequest{
					ResourceAndRelation: tuple.ParseONR(step.start),
					Subject:             tuple.ParseSubjectONR(step.goal),
					Metadata: &v1.ResolverMeta{
						AtRevision:     step.atRevision.String(),
						DepthRemaining: step.depthRemaining,
					},
				})
				require.NoError(err)
				require.Equal(v1.DispatchCheckResponse_MEMBER, resp.Membership)

				// We have to sleep a while to let the cache converge:
				// https://github.com/dgraph-io/ristretto/blob/01b9f37dd0fd453225e042d6f3a27cd14f252cd0/cache_test.go#L17
				time.Sleep(10 * time.Millisecond)
			}

			delegate.AssertExpectations(t)
		})
	}
}

type delegateDispatchMock struct {
	*mock.Mock
}

func (ddm delegateDispatchMock) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	args := ddm.Called(req)
	return args.Get(0).(*v1.DispatchCheckResponse), args.Error(1)
}

func (ddm delegateDispatchMock) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{}, nil
}

func (ddm delegateDispatchMock) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	return &v1.DispatchLookupResponse{}, nil
}

func (ddm delegateDispatchMock) DispatchReachableResources(req *v1.DispatchReachableResourcesRequest, stream dispatch.ReachableResourcesStream) error {
	return nil
}

func (ddm delegateDispatchMock) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	return nil
}

func (ddm delegateDispatchMock) Close() error {
	return nil
}

func (ddm delegateDispatchMock) IsReady() bool {
	return true
}

var _ dispatch.Dispatcher = &delegateDispatchMock{}
