package contextfield

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestRetainedFingerprintsOwnTheirBackingStorage(t *testing.T) {
	a := &accumulator{frequencies: map[string]uint64{}}

	for range 2 {
		backing := strings.Repeat("x", 1<<20) + "same-value"
		fingerprint := backing[len(backing)-len("same-value"):]

		require.NoError(t, a.Add(Observation{Present: true, Fingerprint: fingerprint}))
		require.NotSame(t, unsafe.StringData(fingerprint), unsafe.StringData(a.first))

		for retained := range a.frequencies {
			require.NotSame(t, unsafe.StringData(fingerprint), unsafe.StringData(retained))
		}
	}
}
