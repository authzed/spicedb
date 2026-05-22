package runtime

import (
	"runtime/debug"

	"github.com/KimMachineGun/automemlimit/memlimit"
)

// AvailableMemory returns 75% of the memory available to this process.
// It reads GOMEMLIMIT (set by cobraproclimits during startup, which is cgroup-aware
// in Kubernetes), falling back to cgroup/system detection if GOMEMLIMIT is not set.
// The 75% ratio provides headroom for memory not tracked by the caller (e.g. goroutine
// stacks, GC metadata, fragmentation).
func AvailableMemory() uint64 {
	limit := debug.SetMemoryLimit(-1)
	if limit > 0 {
		return uint64(limit) * 75 / 100
	}

	// Fallback if GOMEMLIMIT was not set (e.g. during tests).
	memProvider := memlimit.ApplyFallback(memlimit.FromCgroup, memlimit.FromSystem)
	totalMemory, err := memProvider()
	if err != nil || totalMemory == 0 {
		return 0
	}
	return totalMemory * 75 / 100
}
