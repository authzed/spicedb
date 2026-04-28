package dispatch

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// traversalTraceKey is the metadata key used to propagate the traversal trace
// through a MaxDepthExceededError across dispatch hops (including network boundaries).
const traversalTraceKey spiceerrors.MetadataKey = "dispatch_traversal_trace"

// HandleTraversalTrace takes an error and populates the fields of a LookupDebugInfo
// accordingly.
//
// Designed to be called once per dispatchIter level as the error unwinds: each level
// prepends its frame so the outermost caller sees the full path from root to leaf.
// Because the trace is stored in the gRPC ErrorInfo metadata it survives a network hop.
func HandleTraversalTrace(originalErr error, resourceType, relation string, resourceIds []string) error {
	// If we don't have a max depth error, we pass it along.
	if !IsMaxDepthExceeded(originalErr) {
		return originalErr
	}

	var debugInfo *dispatchv1.LookupDebugInfo
	onrStrings := slicez.Map(resourceIds, func(resourceId string) string {
		return fmt.Sprintf("%s:%s#%s", resourceType, resourceId, relation)
	})

	existingInfo := ExtractTraversalTrace(originalErr)
	switch {
	case existingInfo == nil:
		// If there's no existing info, we have a new error, and we populate the candidates.
		debugInfo = &dispatchv1.LookupDebugInfo{CycleCandidates: onrStrings}
	case len(existingInfo.CycleMembers) > 0:
		// If there are already positively identified cycle members, we pass them along.
		debugInfo = existingInfo
	case len(existingInfo.CycleCandidates) > 0:
		{
			// If the intersection of the existing onrStrings with the current onrStrings is nonzero,
			// those are cycle members and we construct a new debugInfo.
			if intersection := mapz.NewSet(onrStrings...).Intersect(mapz.NewSet(existingInfo.CycleCandidates...)); intersection.Len() > 0 {
				debugInfo = &dispatchv1.LookupDebugInfo{CycleMembers: intersection.AsSlice()}
			} else {
				// We can't determine anything at this step, so we pass along the existing.
				debugInfo = existingInfo
			}
		}
	default:
		{
			// The above cases should cover all of the states we can hit, so if we get here it's a bug.
			return spiceerrors.MustBugf("we have a max depth error but cannot determine the cycle members")
		}
	}

	return spiceerrors.AppendDetailsMetadata(originalErr, traversalTraceKey, prototext.Format(debugInfo))
}

// ExtractTraversalTrace reads the accumulated traversal trace from a MaxDepthExceeded
// error. Returns nil if no trace is present or the error is not a MaxDepthExceeded.
func ExtractTraversalTrace(err error) *dispatchv1.LookupDebugInfo {
	s, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, detail := range s.Details() {
		errInfo, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		debugInfoStr, ok := errInfo.Metadata[string(traversalTraceKey)]
		if !ok || debugInfoStr == "" {
			continue
		}
		debugInfo := &dispatchv1.LookupDebugInfo{}
		if err := prototext.Unmarshal([]byte(debugInfoStr), debugInfo); err != nil {
			return nil
		}
		return debugInfo
	}
	return nil
}
