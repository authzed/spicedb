package resolvermeta

import (
	"github.com/bits-and-blooms/bloom/v3"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const defaultFalsePositiveRate = 0.001

// NewTraversalBloomFilter creates a new bloom filter sized to the provided number of elements and
// with a predefined false-positive ratio of 0.1%.
func NewTraversalBloomFilter(numElements uint) ([]byte, error) {
	bf := bloom.NewWithEstimates(numElements, defaultFalsePositiveRate)

	modifiedBloomFilter, err := bf.MarshalBinary()
	if err != nil {
		return nil, spiceerrors.MustBugf("unexpected error while serializing empty bloom filter: %s", err.Error())
	}

	return modifiedBloomFilter, nil
}

// MustNewTraversalBloomFilter creates a new bloom filter sized to the provided number of elements and
// with a predefined false-positive ratio of 0.1%.
func MustNewTraversalBloomFilter(numElements uint) []byte {
	bf, err := NewTraversalBloomFilter(numElements)
	if err != nil {
		panic(err)
	}

	return bf
}
