package keys

import (
	"encoding/binary"
)

// DispatchCacheKey is a struct which holds the key representing a dispatch operation being
// dispatched or cached.
type DispatchCacheKey uint64

// StableSumAsBytes returns the stable portion of the dispatch cache key as bytes. Note that since
// this is only returning the result of one of the two sums, the returned bytes may not be fully
// unique for the input data.
func (dck DispatchCacheKey) StableSumAsBytes() []byte {
	return binary.AppendUvarint(make([]byte, 0, 8), uint64(dck))
}

func (dck DispatchCacheKey) KeyString() string {
	firstBytes := binary.AppendUvarint(make([]byte, 0, 8), uint64(dck))
	return string(firstBytes)
}

var emptyDispatchCacheKey = DispatchCacheKey(0)
