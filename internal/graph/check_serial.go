package graph

import (
	"context"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type serialOperation uint8

const (
	serialUnion serialOperation = iota
	serialIntersection
	serialDifference
)

// reduceSerial evaluates a branch only after consuming the previous result.
// No cancellation race can start speculative work after a decisive result.
func reduceSerial[T any](ctx context.Context, crc currentRequestContext, children []T, handler func(context.Context, currentRequestContext, T) CheckResult, op serialOperation) CheckResult {
	metadata := emptyMetadata
	members := NewMembershipSet()
	if op != serialUnion {
		crc.resultsSetting = v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS
	}
	for i, child := range children {
		if err := ctx.Err(); err != nil {
			return checkResultError(err, metadata)
		}
		result := handler(ctx, crc, child)
		metadata = combineResponseMetadata(metadata, result.Resp.GetMetadata())
		if result.Err != nil {
			return checkResultError(result.Err, metadata)
		}
		switch {
		case op == serialUnion || i == 0:
			members.UnionWith(result.Resp.ResultsByResourceId)
		case op == serialIntersection:
			members.IntersectWith(result.Resp.ResultsByResourceId)
		case op == serialDifference:
			members.Subtract(result.Resp.ResultsByResourceId)
		}
		if op == serialUnion {
			if members.HasDeterminedMember() && crc.resultsSetting == v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT {
				break
			}
		} else if members.IsEmpty() {
			break
		}
	}
	return checkResultsForMembership(members, metadata)
}
