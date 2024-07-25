package keys

import (
	"encoding/binary"
)

// DispatchCacheKey is a struct which holds the key representing a dispatch operation being
// dispatched or cached.
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

// AsUInt64s returns the cache key in the form of two uint64's. This method returns uint64s created
// from two distinct hashing algorithms, which should make the risk of key overlap incredibly
// unlikely.
func (dck DispatchCacheKey) AsUInt64s() (uint64, uint64) {
	return dck.processSpecificSum, dck.stableSum
}

func (dck DispatchCacheKey) KeyString() string {
	firstBytes := binary.AppendUvarint(make([]byte, 0, 8), dck.stableSum)
	secondBytes := binary.AppendUvarint(make([]byte, 0, 8), dck.processSpecificSum)
	return string(firstBytes) + string(secondBytes)
}

var emptyDispatchCacheKey = DispatchCacheKey{0, 0}
