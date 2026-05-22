package dispatchv1

import (
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datalayer"
)

func TestRecordTraversal(t *testing.T) {
	var rm *ResolverMeta
	_, err := rm.RecordTraversal("test")
	require.ErrorContains(t, err, "missing")

	rm = &ResolverMeta{
		SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
	}
	_, err = rm.RecordTraversal("test")
	require.ErrorContains(t, err, "missing")

	rm = &ResolverMeta{
		TraversalBloom: []byte(""),
		SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
	}
	_, err = rm.RecordTraversal("test")
	require.ErrorContains(t, err, "missing")

	rm = &ResolverMeta{
		TraversalBloom: []byte("foo"),
		SchemaHash:     []byte(datalayer.NoSchemaHashForTesting),
	}
	_, err = rm.RecordTraversal("test")
	require.ErrorContains(t, err, "unmarshall")

	bf, err := NewTraversalBloomFilter(100)
	require.NoError(t, err)
	rm.TraversalBloom = bf
	possiblyLoop, err := rm.RecordTraversal("test")
	require.False(t, possiblyLoop)
	require.NoError(t, err)

	bfs := bloom.BloomFilter{}
	err = bfs.UnmarshalBinary(rm.TraversalBloom)
	require.NoError(t, err)
	require.True(t, bfs.TestString("test"))

	possiblyLoop, err = rm.RecordTraversal("test")
	require.True(t, possiblyLoop)
	require.NoError(t, err)
}
