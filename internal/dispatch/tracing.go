package dispatch

import (
	"context"
	"sync"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/authzed/ctxkey"

	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// NewTraversalTracker / SaveTraceToContext / SnapshotLookupDebugTrace work
// together to make the trace result of the unwind approach readable from ctx
// after a dispatch call returns.
//
// In dispatchIter, each time a MaxDepthExceededError is annotated with
// PrependTraversalFrame, SaveTraceToContext is also called so that callers
// accessing ctx directly (e.g., dispatch-layer tests) can read the latest trace.

var traceStoreKey = ctxkey.NewBoxedWithDefault[*traversalTraceStore](nil)

type traversalTraceStore struct {
	mu    sync.Mutex
	trace *dispatchv1.LookupDebugTrace
}

// NewTraversalTracker installs a traversal trace slot into ctx.
// Call this once before dispatching so that SaveTraceToContext / SnapshotLookupDebugTrace
// can be used on the same ctx after the dispatch returns.
func NewTraversalTracker(ctx context.Context) context.Context {
	return traceStoreKey.With(&traversalTraceStore{})(ctx)
}

// SaveTraceToContext writes trace into the tracker installed by NewTraversalTracker.
// No-op when no tracker is present.
func SaveTraceToContext(ctx context.Context, trace *dispatchv1.LookupDebugTrace) {
	store := traceStoreKey.Value(ctx)
	if store == nil {
		return
	}
	store.mu.Lock()
	store.trace = trace
	store.mu.Unlock()
}

// SnapshotLookupDebugTrace returns the last trace saved via SaveTraceToContext,
// or nil if none was saved or no tracker is present.
func SnapshotLookupDebugTrace(ctx context.Context) *dispatchv1.LookupDebugTrace {
	store := traceStoreKey.Value(ctx)
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.trace
}

// traversalTraceKey is the metadata key used to propagate the traversal trace
// through a MaxDepthExceededError across dispatch hops (including network boundaries).
const traversalTraceKey spiceerrors.MetadataKey = "dispatch_traversal_trace"

// PrependTraversalFrame adds a new frame to the front of the traversal trace carried
// by a MaxDepthExceeded error, then returns the updated error. If err is not a
// MaxDepthExceeded condition it is returned unchanged.
//
// Designed to be called once per dispatchIter level as the error unwinds: each level
// prepends its frame so the outermost caller sees the full path from root to leaf.
// Because the trace is stored in the gRPC ErrorInfo metadata it survives a network hop.
func PrependTraversalFrame(err error, resourceType, resourceID, relation string) error {
	if !IsMaxDepthExceeded(err) {
		return err
	}

	node := &dispatchv1.LookupDebugTrace{
		ResourceType: resourceType,
		ResourceId:   resourceID,
		Relation:     relation,
	}
	if existing := ExtractTraversalTrace(err); existing != nil {
		node.SubProblems = []*dispatchv1.LookupDebugTrace{existing}
	}

	return spiceerrors.AppendDetailsMetadata(err, traversalTraceKey, prototext.Format(node))
}

// ExtractTraversalTrace reads the accumulated traversal trace from a MaxDepthExceeded
// error. Returns nil if no trace is present or the error is not a MaxDepthExceeded.
func ExtractTraversalTrace(err error) *dispatchv1.LookupDebugTrace {
	s, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, detail := range s.Details() {
		errInfo, ok := detail.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		traceStr, ok := errInfo.Metadata[string(traversalTraceKey)]
		if !ok || traceStr == "" {
			continue
		}
		trace := &dispatchv1.LookupDebugTrace{}
		if err := prototext.Unmarshal([]byte(traceStr), trace); err != nil {
			return nil
		}
		return trace
	}
	return nil
}
