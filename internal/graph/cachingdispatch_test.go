package graph

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/tuple"
)

type checkRequest struct {
	start             string
	goal              string
	atRevision        decimal.Decimal
	depthRemaining    uint16
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
			{start1, user1, decimal.Zero, 50, true},
		}},
		{"two requests, hit", []checkRequest{
			{start1, user1, decimal.Zero, 50, true},
			{start1, user1, decimal.Zero, 50, false},
		}},
		{"many requests, hit", []checkRequest{
			{start1, user1, decimal.Zero, 50, true},
			{start1, user1, decimal.Zero, 50, false},
			{start1, user1, decimal.Zero, 50, false},
			{start1, user1, decimal.Zero, 50, false},
			{start1, user1, decimal.Zero, 50, false},
		}},
		{"multiple keys", []checkRequest{
			{start1, user1, decimal.Zero, 50, true},
			{start2, user2, decimal.Zero, 50, true},
		}},
		{"same object, different revisions miss", []checkRequest{
			{start1, user1, decimal.Zero, 50, true},
			{start1, user1, decimal.NewFromInt(50), 50, true},
		}},
		{"interleaved objects, hit", []checkRequest{
			{start1, user1, decimal.Zero, 50, true},
			{start2, user2, decimal.Zero, 50, true},
			{start1, user1, decimal.Zero, 50, false},
			{start2, user2, decimal.Zero, 50, false},
		}},
		{"insufficient depth", []checkRequest{
			{start1, user1, decimal.Zero, 50, true},
			{start1, user1, decimal.Zero, 40, true},
		}},
		{"sufficient depth", []checkRequest{
			{start1, user1, decimal.Zero, 40, true},
			{start1, user1, decimal.Zero, 50, false},
		}},
		{"updated cached depth", []checkRequest{
			{start1, user1, decimal.Zero, 50, true},
			{start1, user1, decimal.Zero, 40, true},
			{start1, user1, decimal.Zero, 40, false},
			{start1, user1, decimal.Zero, 50, false},
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			delegate := delegateDispatchMock{}

			for _, step := range tc.script {
				if step.expectPassthrough {
					delegate.On("Check", CheckRequest{
						Start:          tuple.ScanONR(step.start),
						Goal:           tuple.ScanONR(step.goal),
						AtRevision:     step.atRevision,
						DepthRemaining: step.depthRemaining,
					}).Return(CheckResult{true, nil}).Times(1)
				}
			}

			dispatch, err := NewCachingDispatcher(delegate, nil, DisablePromMetrics)
			require.NoError(err)

			for _, step := range tc.script {
				resp := dispatch.Check(context.Background(), CheckRequest{
					Start:          tuple.ScanONR(step.start),
					Goal:           tuple.ScanONR(step.goal),
					AtRevision:     step.atRevision,
					DepthRemaining: step.depthRemaining,
				})
				require.NoError(resp.Err)
				require.True(resp.IsMember)

				// We have to sleep a while to let the cache converge:
				// https://github.com/dgraph-io/ristretto/blob/01b9f37dd0fd453225e042d6f3a27cd14f252cd0/cache_test.go#L17
				time.Sleep(10 * time.Millisecond)
			}

			delegate.AssertExpectations(t)
		})
	}
}

type delegateDispatchMock struct {
	mock.Mock
}

func (ddm delegateDispatchMock) Check(ctx context.Context, req CheckRequest) CheckResult {
	args := ddm.Called(req)
	return args.Get(0).(CheckResult)
}

func (ddm delegateDispatchMock) Expand(ctx context.Context, req ExpandRequest) ExpandResult {
	return ExpandResult{}
}

func (ddm delegateDispatchMock) Lookup(ctx context.Context, req LookupRequest) LookupResult {
	return LookupResult{}
}
