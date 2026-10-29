package dispatch

import (
	"context"

	"github.com/authzed/ctxkey"

	"github.com/authzed/spicedb/pkg/genutil/slicez"
)

// RoutingKey is a hashring key for one dispatch.
type RoutingKey []byte

// RoutingHint names the ring owner of one dispatch.
// Only the remote dispatcher reads it. The graph passes it on without change.
type RoutingHint struct {
	// Key is the hashring key for the dispatch.
	Key RoutingKey

	// Owner is the ring member key of the owner of Key.
	// It is empty when the owner is not known.
	Owner string
}

// IsZero reports whether the hint has no key.
func (h RoutingHint) IsZero() bool {
	return len(h.Key) == 0
}

// Chunk is the resource IDs of one dispatch and the routing hint for that dispatch.
type Chunk struct {
	// ResourceIDs are the resource IDs of the dispatch.
	ResourceIDs []string

	// Hint is the routing hint for the dispatch. It is zero when the chunker gives no hint.
	Hint RoutingHint
}

// Chunker splits resource IDs into dispatch chunks.
type Chunker interface {
	// Chunk splits ids into chunks. namespace is the object type of ids, as named by its schema definition.
	// Each chunk has at most maxChunkSize IDs. A maxChunkSize of 0 selects slicez.DefaultChunkSize.
	// Each ID is in exactly one chunk.
	// The chunks can share the backing array of ids, so the caller must not change ids after the call.
	Chunk(ctx context.Context, namespace string, ids []string, maxChunkSize uint16) []Chunk
}

// DefaultChunker splits resource IDs by size only, as slicez.ForEachChunk does.
// Its chunks have no routing hint.
var DefaultChunker Chunker = sizeChunker{}

// sizeChunker is the type of DefaultChunker.
type sizeChunker struct{}

// Chunk implements Chunker.
func (sizeChunker) Chunk(_ context.Context, _ string, ids []string, maxChunkSize uint16) []Chunk {
	size := int(maxChunkSize)
	if size == 0 {
		size = slicez.DefaultChunkSize
	}

	chunks := make([]Chunk, 0, len(ids)/size+1)
	slicez.ForEachChunk(ids, maxChunkSize, func(part []string) {
		chunks = append(chunks, Chunk{ResourceIDs: part})
	})
	return chunks
}

var (
	chunkerKey     = ctxkey.NewWithDefault(DefaultChunker)
	routingHintKey = ctxkey.New[RoutingHint]()
)

// ContextWithChunker returns a context that carries chunker.
// The graph reads the chunker of the context for each dispatch.
// A nil chunker selects DefaultChunker.
func ContextWithChunker(ctx context.Context, chunker Chunker) context.Context {
	return chunkerKey.Set(ctx, chunker)
}

// ChunkerFromContext returns the chunker of ctx.
// It returns DefaultChunker if ctx has no chunker or has a nil chunker. It never returns nil.
func ChunkerFromContext(ctx context.Context) Chunker {
	return chunkerKey.Value(ctx)
}

// WithRoutingHint returns a context that carries hint for one dispatch.
// Set the hint only on the context of the dispatch call for its chunk.
// A zero hint removes the hint of ctx. Thus a dispatch without a hint never uses the hint of an outer dispatch.
func WithRoutingHint(ctx context.Context, hint RoutingHint) context.Context {
	return routingHintKey.Set(ctx, hint)
}

// WithRoutingHintIfNeeded is WithRoutingHint, but it returns ctx unchanged
// when hint is zero and ctx carries no hint.
func WithRoutingHintIfNeeded(ctx context.Context, hint RoutingHint) context.Context {
	if hint.IsZero() {
		if _, ok := RoutingHintFrom(ctx); !ok {
			return ctx
		}
	}

	return WithRoutingHint(ctx, hint)
}

// RoutingHintFrom returns the routing hint of ctx.
// It reports false if ctx has no hint or has a zero hint.
func RoutingHintFrom(ctx context.Context) (RoutingHint, bool) {
	hint, ok := routingHintKey.Value(ctx)
	if !ok || hint.IsZero() {
		return RoutingHint{}, false
	}
	return hint, true
}
