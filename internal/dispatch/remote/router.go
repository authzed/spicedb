package remote

import (
	"context"
	"time"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// router selects the hashring key of each dispatch and forms the dispatch chunks.
type router interface {
	dispatch.Chunker

	// checkKey returns the hashring key for req and the owner for the latency sample.
	// An empty owner records no latency sample.
	checkKey(ctx context.Context, req *v1.DispatchCheckRequest) (dispatch.RoutingKey, string, error)

	// lookupSubjectsKey returns the hashring key for req and the owner for the latency sample.
	lookupSubjectsKey(ctx context.Context, req *v1.DispatchLookupSubjectsRequest) (dispatch.RoutingKey, string, error)

	// latencyObserver returns a function that records a primary dispatch latency for owner, or nil.
	latencyObserver(owner string) func(time.Duration)

	// Close releases the resources of the router.
	Close()
}

// requestHashRouter routes each dispatch by the hash of its request and chunks by size only.
type requestHashRouter struct {
	keyHandler keys.Handler
}

var _ router = (*requestHashRouter)(nil)

// Chunk splits ids by size only, as dispatch.DefaultChunker does.
func (r *requestHashRouter) Chunk(ctx context.Context, namespace string, ids []string, maxChunkSize uint16) []dispatch.Chunk {
	return dispatch.DefaultChunker.Chunk(ctx, namespace, ids, maxChunkSize)
}

// checkKey returns the request hash of req and an empty owner.
func (r *requestHashRouter) checkKey(ctx context.Context, req *v1.DispatchCheckRequest) (dispatch.RoutingKey, string, error) {
	key, err := r.keyHandler.CheckDispatchKey(ctx, req)
	return key, "", err
}

// lookupSubjectsKey returns the request hash of req and an empty owner.
func (r *requestHashRouter) lookupSubjectsKey(ctx context.Context, req *v1.DispatchLookupSubjectsRequest) (dispatch.RoutingKey, string, error) {
	key, err := r.keyHandler.LookupSubjectsDispatchKey(ctx, req)
	return key, "", err
}

// latencyObserver returns nil because requestHashRouter records no latency.
func (r *requestHashRouter) latencyObserver(string) func(time.Duration) {
	return nil
}

// Close does nothing.
func (r *requestHashRouter) Close() {}
