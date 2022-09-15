package keys

import (
	"encoding/binary"
)

// DispatchCacheKey is a struct which holds the key representing a dispatch operation being
// dispatched or cached. This key is guaranteed to be unique in byte or (uint64, uint64) form for
// a particular request.
type DispatchCacheKey struct {
	stableSum          uint64
	processSpecificSum uint64
}

// StableSumAsBytes returns the stable portion of the dispatch cache key as bytes. Note that since
// this is only returning the result of one of the two sums, the returned bytes may not be fully
// unique for the input data.
func (dck DispatchCacheKey) StableSumAsBytes() []byte {
	return binary.AppendUvarint(make([]byte, 0, 8), dck.stableSum)
}

// AsUInt64s returns the cache key in the form of two uint64's.
func (dck DispatchCacheKey) AsUInt64s() (uint64, uint64) {
	return dck.processSpecificSum, dck.stableSum
}

var emptyDispatchCacheKey = DispatchCacheKey{0, 0}
