//go:build !wasm
// +build !wasm

package keys

import (
	"fmt"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

// dispatchCacheKeyHash computres a DispatchCheckKey for the given prefix and any hashable values.
func dispatchCacheKeyHash(prefix cachePrefix, atRevision string, args ...hashableValue) DispatchCacheKey {
	hasher := newDispatchCacheKeyHasher(prefix)

	for _, arg := range args {
		arg.AppendToHash(hasher)
		hasher.WriteString("@")
	}

	hasher.WriteString(atRevision)
	return hasher.BuildKey()
}

type dispatchCacheKeyHasher struct {
	stableHasher       *xxhash.Digest
	processSpecificSum uint64
}

func newDispatchCacheKeyHasher(prefix cachePrefix) *dispatchCacheKeyHasher {
	h := &dispatchCacheKeyHasher{
		stableHasher: xxhash.New(),
	}

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

	h.processSpecificSum = runMemHash(h.processSpecificSum, []byte(value))
}

// From: https://github.com/dgraph-io/ristretto/blob/master/z/rtutil.go
type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

func runMemHash(seed uint64, data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, uintptr(seed), uintptr(ss.len)))
}

// BuildKey returns the constructed DispatchCheckKey.
func (h *dispatchCacheKeyHasher) BuildKey() DispatchCacheKey {
	return DispatchCacheKey{
		stableSum:          h.stableHasher.Sum64(),
		processSpecificSum: h.processSpecificSum,
	}
}
