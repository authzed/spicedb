package keys

import (
	"encoding/binary"
)

// DispatchCacheKey is a struct which holds the key representing a dispatch operation being
// dispatched or cached.
type DispatchCacheKey struct {
	stableSum uint64
}

// StableSumAsBytes returns the stable portion of the dispatch cache key as bytes. Note that since
// this is only returning the result of one of the two sums, the returned bytes may not be fully
// unique for the input data.
func (dck DispatchCacheKey) StableSumAsBytes() []byte {
	return binary.AppendUvarint(make([]byte, 0, 8), dck.stableSum)
}

func (dck DispatchCacheKey) KeyString() string {
	firstBytes := binary.AppendUvarint(make([]byte, 0, 8), dck.stableSum)
	return string(firstBytes)
}

var emptyDispatchCacheKey = DispatchCacheKey{0}
