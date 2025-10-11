package dispatchv1

import (
	"errors"
	"fmt"

	"github.com/bits-and-blooms/bloom/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func (x *ResolverMeta) RecordTraversal(key string) (possiblyLoop bool, err error) {
	if key == "" {
		return false, spiceerrors.MustBugf("missing key to be recorded in traversal")
	}

	if x == nil || len(x.TraversalBloom) == 0 {
		return false, status.Error(codes.Internal, errors.New("required traversal bloom filter is missing").Error())
	}

	bf := &bloom.BloomFilter{}
	if err := bf.UnmarshalBinary(x.TraversalBloom); err != nil {
		return false, status.Error(codes.Internal, fmt.Errorf("unable to unmarshall traversal bloom filter: %w", err).Error())
	}

	if bf.TestString(key) {
		return true, nil
	}

	x.TraversalBloom, err = bf.AddString(key).MarshalBinary()
	if err != nil {
		return false, err
	}

	return false, nil
}

const defaultFalsePositiveRate = 0.001

// NewTraversalBloomFilter creates a new bloom filter sized to the provided number of elements and
// with a predefined false-positive ratio of 0.1%.
func NewTraversalBloomFilter(numElements uint) ([]byte, error) {
	bf := bloom.NewWithEstimates(numElements, defaultFalsePositiveRate)

	emptyBloomFilter, err := bf.MarshalBinary()
	if err != nil {
		return nil, spiceerrors.MustBugf("unexpected error while serializing empty bloom filter: %s", err.Error())
	}

	return emptyBloomFilter, nil
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
