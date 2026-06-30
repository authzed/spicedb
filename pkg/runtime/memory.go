package runtime

import (
	"math"
	"runtime/debug"
	"sync"

	"github.com/KimMachineGun/automemlimit/memlimit"

	"github.com/authzed/spicedb/internal/logging"
)

// MemorySource describes where the available-memory figure was derived from.
type MemorySource string

const (
	// MemorySourceGOMEMLIMIT means the figure came from GOMEMLIMIT, which is set
	// cgroup-aware at startup (and always set under Kubernetes).
	MemorySourceGOMEMLIMIT MemorySource = "GOMEMLIMIT"
	// MemorySourceDetected means GOMEMLIMIT was not usable and the figure came
	// from direct cgroup/system detection.
	MemorySourceDetected MemorySource = "cgroup/system"
	// MemorySourceUndetermined means available memory could not be determined at
	// all. Percent-based budgets (e.g. cache MaxCost) cannot be honored.
	MemorySourceUndetermined MemorySource = "undetermined"
)

var logAvailableMemoryOnce sync.Once

// AvailableMemory returns 75% of the memory available to this process.
// It reads GOMEMLIMIT (set by cobraproclimits during startup, which is cgroup-aware
// in Kubernetes), falling back to cgroup/system detection if GOMEMLIMIT is not set.
// The 75% ratio provides headroom for memory not tracked by the caller (e.g. goroutine
// stacks, GC metadata, fragmentation).
//
// The detected value and its source are logged once, the first time this is called,
// to make percent-based-budget misconfiguration diagnosable (notably on AWS ECS,
// where the container memory limit may not be written to the cgroup).
func AvailableMemory() uint64 {
	mem, source := availableMemory()
	logAvailableMemoryOnce.Do(func() {
		evt := logging.Info().
			Uint64("available-memory-bytes", mem).
			Str("source", string(source))
		if source == MemorySourceUndetermined {
			evt.Msg("could not determine available memory; percent-based cache budgets cannot be honored")
		} else {
			evt.Msg("determined available memory for percent-based budgets")
		}
	})
	return mem
}

// availableMemory returns 75% of the memory available to this process along with
// the source the figure was derived from, for diagnostics and logging.
func availableMemory() (uint64, MemorySource) {
	limit := debug.SetMemoryLimit(-1)
	// When GOMEMLIMIT is never set (the Go runtime default is math.MaxInt64), or
	// cgroup detection failed at startup, debug.SetMemoryLimit(-1) reports the
	// max-int sentinel rather than a real limit. This happens on platforms where
	// the container memory limit isn't written to the cgroup (e.g. AWS ECS tasks
	// without a container-level hard memory limit), unlike Kubernetes where the
	// kubelet always sets it. Treating the sentinel as a real limit would yield an
	// absurdly large "available memory" and let percent-based budgets (e.g. the
	// cache MaxCost) grow unbounded until the process is OOM-killed. Fall through
	// to cgroup/system detection instead.
	if limit > 0 && limit != math.MaxInt64 {
		return uint64(limit) * 75 / 100, MemorySourceGOMEMLIMIT
	}

	// Fallback if GOMEMLIMIT was not set (e.g. during tests).
	memProvider := memlimit.ApplyFallback(memlimit.FromCgroup, memlimit.FromSystem)
	totalMemory, err := memProvider()
	if err != nil || totalMemory == 0 {
		return 0, MemorySourceUndetermined
	}
	return totalMemory * 75 / 100, MemorySourceDetected
}
