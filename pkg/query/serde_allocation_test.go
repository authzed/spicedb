package query

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVarintBufferWritingAvoidsScratchAllocations(t *testing.T) {
	var output bytes.Buffer
	output.Grow(binary.MaxVarintLen64)
	var lastError error
	allocations := testing.AllocsPerRun(1000, func() {
		output.Reset()
		lastError = writeUvarint(&output, math.MaxUint64)
	})
	require.NoError(t, lastError)
	require.Zero(t, allocations, "per-field scratch allocations dominate public plan serialization")

	var expected [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(expected[:], math.MaxUint64)
	require.Equal(t, expected[:size], output.Bytes())
}

// writerOnly exercises the generic fallback without advertising WriteByte.
type writerOnly struct{ io.Writer }

type limitedByteWriter struct {
	bytes.Buffer
	remaining int
	failure   error
}

// WriteByte rejects the next byte after the configured successful prefix.
func (w *limitedByteWriter) WriteByte(value byte) error {
	if w.remaining == 0 {
		return w.failure
	}

	w.remaining--
	return w.Buffer.WriteByte(value)
}

func TestVarintWritersPreserveEncodingAndErrors(t *testing.T) {
	for _, value := range []uint64{0, 127, 128, 16383, 16384, math.MaxUint32, math.MaxUint64} {
		var expected [binary.MaxVarintLen64]byte
		size := binary.PutUvarint(expected[:], value)

		for _, byteWriter := range []bool{false, true} {
			var output bytes.Buffer
			var writer io.Writer = writerOnly{&output}

			if byteWriter {
				writer = &output
			}

			require.NoError(t, writeUvarint(writer, value))
			require.Equal(t, expected[:size], output.Bytes())
		}
	}

	failure := errors.New("writer failed")
	writer := &limitedByteWriter{remaining: 2, failure: failure}
	require.ErrorIs(t, writeUvarint(writer, math.MaxUint64), failure)
	require.Equal(t, []byte{0xff, 0xff}, writer.Bytes())
	require.ErrorIs(t, writeUvarint(writerOnly{errorWriter{failure}}, 128), failure)
}

type errorWriter struct{ failure error }

// Write reports the configured underlying writer failure.
func (w errorWriter) Write([]byte) (int, error) { return 0, w.failure }

func TestHeaderBufferWritingAvoidsScratchAllocations(t *testing.T) {
	var output bytes.Buffer
	output.Grow(64)
	var lastError error
	allocations := testing.AllocsPerRun(1000, func() {
		output.Reset()
		lastError = writeHeader(&output, AliasIteratorType, "named-alias")
	})
	require.NoError(t, lastError)
	require.Zero(t, allocations, "a node header needs no temporary byte slice")
}
