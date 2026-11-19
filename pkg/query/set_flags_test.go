package query

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetOperationFlagsPreserveWireFormat(t *testing.T) {
	for _, kind := range []string{"union", "intersection"} {
		for _, independent := range []bool{false, true} {
			name := kind + "/ordered"

			if independent {
				name = kind + "/independent"
			}

			t.Run(name, func(t *testing.T) {
				children := []Iterator{NewFixedIterator(), NewFixedIterator()}
				var iterator Iterator
				var expected []byte

				if kind == "union" {
					set := NewUnionIterator(children...).(*UnionIterator)
					set.canonicalKey = "k"
					set.independentCheckBranches = independent
					iterator = set
					expected = []byte{'|', 1, 'k', 0, 2, '0', 0, 0, '0', 0, 0}
				} else {
					set := NewIntersectionIterator(children...).(*IntersectionIterator)
					set.canonicalKey = "k"
					set.independentCheckBranches = independent
					iterator = set
					expected = []byte{'&', 1, 'k', 0, 2, '0', 0, 0, '0', 0, 0}
				}

				// Keep the historical flag byte and child framing explicit: this
				// catches matching encoder/decoder changes that a round trip misses.
				if independent {
					expected[3] = 1
				}

				var output bytes.Buffer
				require.NoError(t, iterator.Serialize(&output))
				require.Equal(t, expected, output.Bytes())
				decoded, err := Deserialize(bytes.NewReader(expected), nil)
				require.NoError(t, err)
				assertStructurallyEqual(t, iterator, decoded)
				outline, err := Decompile(decoded)
				require.NoError(t, err)
				require.Equal(t, independent, outline.IndependentCheckBranches)
			})
		}
	}
}

func TestSetOperationFlagCodecPropagatesErrors(t *testing.T) {
	failure := errors.New("flags writer failed")
	flags := setOperationFlags{independentCheckBranches: true}
	require.ErrorIs(t, flags.write(writerOnly{errorWriter{failure}}), failure)
	require.ErrorIs(t, flags.write(&limitedByteWriter{failure: failure}), failure)
	_, err := readSetOperationFlags(bytes.NewReader([]byte{0x80}))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
