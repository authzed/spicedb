package runtime

import (
	"math"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAvailableMemory_ReturnsNonZero(t *testing.T) {
	mem := AvailableMemory()
	require.Positive(t, mem, "available memory should be greater than zero")
}

func TestAvailableMemory_RespectsGOMEMLIMIT(t *testing.T) {
	original := debug.SetMemoryLimit(-1)
	t.Cleanup(func() {
		debug.SetMemoryLimit(original)
	})

	const limit int64 = 512 * 1024 * 1024 // 512 MiB
	debug.SetMemoryLimit(limit)

	mem := AvailableMemory()
	require.Equal(t, uint64(limit)*75/100, mem, "should return 75%% of GOMEMLIMIT")
}

func TestAvailableMemory_FallsBackWhenGOMEMLIMITUnset(t *testing.T) {
	original := debug.SetMemoryLimit(-1)
	t.Cleanup(func() {
		debug.SetMemoryLimit(original)
	})

	debug.SetMemoryLimit(0)

	mem := AvailableMemory()
	require.Positive(t, mem, "should fall back to cgroup/system memory detection")
}

func TestAvailableMemory_FallsBackWhenGOMEMLIMITUnsetSentinel(t *testing.T) {
	original := debug.SetMemoryLimit(-1)
	t.Cleanup(func() {
		debug.SetMemoryLimit(original)
	})

	// The Go runtime's default soft memory limit (i.e. GOMEMLIMIT never set) is
	// math.MaxInt64, which is what debug.SetMemoryLimit(-1) reports. This is the
	// case seen on AWS ECS tasks where cgroup detection fails at startup. We must
	// fall back to cgroup/system detection rather than returning 75% of MaxInt64.
	debug.SetMemoryLimit(math.MaxInt64)

	mem := AvailableMemory()
	require.Positive(t, mem, "should fall back to cgroup/system memory detection")
	require.Less(t, mem, uint64(math.MaxInt64)/100*75,
		"must not treat the max-int sentinel as a real memory limit")
}

func TestAvailableMemory_Applies75PercentRatio(t *testing.T) {
	original := debug.SetMemoryLimit(-1)
	t.Cleanup(func() {
		debug.SetMemoryLimit(original)
	})

	const limit int64 = 1000 * 1024 * 1024 // 1000 MiB
	debug.SetMemoryLimit(limit)

	mem := AvailableMemory()
	expected := uint64(limit) * 75 / 100
	require.Equal(t, expected, mem, "should apply 75%% ratio to available memory")
}
