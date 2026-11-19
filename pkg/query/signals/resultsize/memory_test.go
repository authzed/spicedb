package resultsize

import (
	"reflect"
	"testing"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"
)

func assertDigestBuffersAccounted(t *testing.T, a *accumulator) {
	t.Helper()

	// This regression deliberately inspects the dependency's backing capacities:
	// a library upgrade must not silently invalidate our conservative Size bound.
	summary := reflect.ValueOf(a.digest).Elem().FieldByName("summary").Elem()
	means := summary.FieldByName("means").Cap()
	counts := summary.FieldByName("counts").Cap()
	bufferBytes := 8 * safecast.MustConvert[int64](means+counts)

	require.GreaterOrEqual(t, a.Size(), bufferBytes+256)
}

func TestDigestBudgetCoversCompressionAndClones(t *testing.T) {
	a := NewAccumulator().(*accumulator)
	assertDigestBuffersAccounted(t, a)

	for block := range 4 {
		for i := range 2048 {
			count := safecast.MustConvert[uint64]((i*7919 + block) % 12345)

			require.NoError(t, a.Add(Observation{Count: count}))
		}

		assertDigestBuffersAccounted(t, a)
		require.NoError(t, a.digest.Compress())
		assertDigestBuffersAccounted(t, a)
		cloned := a.Clone().(*accumulator)
		assertDigestBuffersAccounted(t, cloned)
	}
}
