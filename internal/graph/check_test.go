package graph

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func wellBehavedSlowCheck(waitTime time.Duration, result v1.DispatchCheckResponse_Membership) CheckFunc {
	return func(ctx context.Context) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
		timer := time.NewTimer(waitTime)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, NewCheckFailureErr(errors.New("timed out"), emptyMetadata)
		case <-timer.C:
			return &v1.DispatchCheckResponse{
				Metadata:   emptyMetadata,
				Membership: result,
			}, nil
		}
	}
}

func unresponsiveSlowCheck(waitTime time.Duration, result v1.DispatchCheckResponse_Membership) CheckFunc {
	return func(ctx context.Context) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
		timer := time.NewTimer(waitTime)
		<-timer.C
		return &v1.DispatchCheckResponse{
			Metadata:   emptyMetadata,
			Membership: result,
		}, nil
	}
}

func wellBehavedWaitThenError(waitTime time.Duration) CheckFunc {
	return func(ctx context.Context) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
		timer := time.NewTimer(waitTime)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, NewCheckFailureErr(errors.New("timed out"), emptyMetadata)
		case <-timer.C:
			return nil, NewCheckFailureErr(errors.New("failed"), emptyMetadata)
		}
	}
}

func TestProcessSubrequests(t *testing.T) {
	testCases := []struct {
		name                  string
		requests              []CheckFunc
		defMembership         v1.DispatchCheckResponse_Membership
		earlyDetermination    earlyDeterminationFunc
		expectError           bool
		expectedDetermination v1.DispatchCheckResponse_Membership
	}{
		{
			"empty union",
			[]CheckFunc{},
			v1.DispatchCheckResponse_NOT_MEMBER,
			memberIfAny,
			false,
			v1.DispatchCheckResponse_NOT_MEMBER,
		},
		{
			"empty intersection",
			[]CheckFunc{},
			v1.DispatchCheckResponse_MEMBER,
			notMemberIfAny,
			false,
			v1.DispatchCheckResponse_NOT_MEMBER,
		},
		{
			"one entry union",
			[]CheckFunc{
				alwaysMember,
			},
			v1.DispatchCheckResponse_NOT_MEMBER,
			memberIfAny,
			false,
			v1.DispatchCheckResponse_MEMBER,
		},
		{
			"multiple entry union",
			[]CheckFunc{
				notMember,
				alwaysMember,
				notMember,
			},
			v1.DispatchCheckResponse_NOT_MEMBER,
			memberIfAny,
			false,
			v1.DispatchCheckResponse_MEMBER,
		},
		{
			"one entry intersection",
			[]CheckFunc{
				alwaysMember,
			},
			v1.DispatchCheckResponse_MEMBER,
			notMemberIfAny,
			false,
			v1.DispatchCheckResponse_MEMBER,
		},
		{
			"multiple entry intersection",
			[]CheckFunc{
				alwaysMember,
				alwaysMember,
				alwaysMember,
			},
			v1.DispatchCheckResponse_MEMBER,
			notMemberIfAny,
			false,
			v1.DispatchCheckResponse_MEMBER,
		},
		{
			"multiple entry failed intersection",
			[]CheckFunc{
				notMember,
				alwaysMember,
				notMember,
			},
			v1.DispatchCheckResponse_MEMBER,
			notMemberIfAny,
			false,
			v1.DispatchCheckResponse_NOT_MEMBER,
		},
		{
			"cancel slow branch",
			[]CheckFunc{
				alwaysMember,
				wellBehavedSlowCheck(10*time.Millisecond, v1.DispatchCheckResponse_MEMBER),
			},
			v1.DispatchCheckResponse_NOT_MEMBER,
			memberIfAny,
			false,
			v1.DispatchCheckResponse_MEMBER,
		},
		{
			"wait for slow branch",
			[]CheckFunc{
				alwaysMember,
				wellBehavedSlowCheck(10*time.Millisecond, v1.DispatchCheckResponse_MEMBER),
			},
			v1.DispatchCheckResponse_MEMBER,
			notMemberIfAny,
			false,
			v1.DispatchCheckResponse_MEMBER,
		},
		{
			"slow branch cant stop wont stop",
			[]CheckFunc{
				alwaysMember,
				unresponsiveSlowCheck(10*time.Millisecond, v1.DispatchCheckResponse_MEMBER),
			},
			v1.DispatchCheckResponse_NOT_MEMBER,
			memberIfAny,
			false,
			v1.DispatchCheckResponse_MEMBER,
		},
		{
			"multiple slow branches",
			[]CheckFunc{
				wellBehavedSlowCheck(10*time.Millisecond, v1.DispatchCheckResponse_NOT_MEMBER),
				wellBehavedSlowCheck(12*time.Millisecond, v1.DispatchCheckResponse_MEMBER),
			},
			v1.DispatchCheckResponse_NOT_MEMBER,
			memberIfAny,
			false,
			v1.DispatchCheckResponse_MEMBER,
		},
		{
			"error after a while",
			[]CheckFunc{
				alwaysMember,
				wellBehavedWaitThenError(10 * time.Millisecond),
			},
			v1.DispatchCheckResponse_MEMBER,
			notMemberIfAny,
			true,
			v1.DispatchCheckResponse_UNKNOWN,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			resp, err := processSubrequests(
				context.Background(),
				tc.requests,
				tc.defMembership,
				tc.earlyDetermination,
				-1,
			)

			if tc.expectError {
				require.Error(err)
			} else {
				require.Equal(tc.expectedDetermination, resp.Membership)
			}
		})
	}
}
