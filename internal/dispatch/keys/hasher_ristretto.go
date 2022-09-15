//go:build !wasm
// +build !wasm

package keys

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/ristretto/z"

	"github.com/cespare/xxhash/v2"
)

// dispatchCacheKeyHash computres a DispatchCheckKey for the given prefix and any hashable values.
func dispatchCacheKeyHash(prefix cachePrefix, atRevision string, args ...hashableValue) DispatchCacheKey {
	totalEstimatedLength := len(string(prefix)) + len(atRevision) + 1 // prefix, revision and `/`
	for _, arg := range args {
		totalEstimatedLength += arg.EstimatedLength() + 1 // +1 for the @
	}

	hasher := newDispatchCacheKeyHasher(prefix, totalEstimatedLength)

	for _, arg := range args {
		arg.AppendToHash(hasher)
		hasher.WriteString("@")
	}

	hasher.WriteString(atRevision)
	return hasher.BuildKey()
}

type dispatchCacheKeyHasher struct {
	stableHasher    *xxhash.Digest
	sb              strings.Builder
	estimatedLength int
}

func newDispatchCacheKeyHasher(prefix cachePrefix, estimatedLength int) *dispatchCacheKeyHasher {
	h := &dispatchCacheKeyHasher{
		stableHasher:    xxhash.New(),
		estimatedLength: estimatedLength,
	}

	h.sb.Grow(estimatedLength)

	prefixString := string(prefix)
	h.WriteString(prefixString)
	h.WriteString("/")
	return h
}

// WriteString writes a single string to the hasher.
func (h *dispatchCacheKeyHasher) WriteString(value string) {
	_, err := h.stableHasher.WriteString(value)
	if err != nil {
		panic(fmt.Errorf("got an error from writing to the stable hasher: %w", err))
	}

	_, err = h.sb.WriteString(value)
	if err != nil {
		panic(fmt.Errorf("got an error from writing to the stringbuilder for hasher: %w", err))
	}
}

// BuildKey returns the constructed DispatchCheckKey.
func (h *dispatchCacheKeyHasher) BuildKey() DispatchCacheKey {
	return DispatchCacheKey{
		stableSum:          h.stableHasher.Sum64(),
		processSpecificSum: z.MemHashString(h.sb.String()),
	}
}
