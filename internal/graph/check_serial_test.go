package graph

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// An empty exclusion base must not start the excluded branch at concurrency one.
func TestSerialCheckNoSpeculativeChildren(t *testing.T) {
	var extra atomic.Int32
	result := difference(t.Context(), currentRequestContext{}, []int{0, 1}, func(ctx context.Context, _ currentRequestContext, child int) CheckResult {
		if child == 0 {
			for range 100 {
				runtime.Gosched()
			}
			return noMembers()
		}
		extra.Add(1)
		return noMembers()
	}, 1)
	require.NoError(t, result.Err)
	require.Zero(t, extra.Load())
}

func TestSerialCheckSetOperations(t *testing.T) {
	for _, op := range []string{"union", "intersection", "difference"} {
		t.Run(op, func(t *testing.T) {
			var visited []int
			crc := currentRequestContext{resultsSetting: v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT}
			handler := func(_ context.Context, _ currentRequestContext, child int) CheckResult {
				visited = append(visited, child)
				set := NewMembershipSet()
				if op == "union" || (op == "difference" && child == 0) {
					set.AddDirectMember("doc", nil)
				}
				return checkResultsForMembership(set, emptyMetadata)
			}
			var result CheckResult
			switch op {
			case "union":
				result = union(t.Context(), crc, []int{0, 1, 2}, handler, 1)
			case "intersection":
				result = all(t.Context(), crc, []int{0, 1, 2}, handler, 1)
			case "difference":
				result = difference(t.Context(), crc, []int{0, 1, 2}, handler, 1)
			}
			require.NoError(t, result.Err)
			if op == "difference" {
				require.Equal(t, []int{0, 1, 2}, visited)
			} else {
				require.Equal(t, []int{0}, visited)
			}
		})
	}
}
