package rtml

import (
	"sync/atomic"
	_ "unsafe"
)

// miror the types from go runtime which uses sysMemStat.
// https://github.com/golang/go/blob/44c5956bf7454ca178c596eb87578ea61d6c9dee/src/runtime/mstats.go#L643
type sysMemStat uint64

func (s *sysMemStat) load() uint64 {
	return atomic.LoadUint64((*uint64)(s))
}

// using go linkname so we can read the internal values that the
// garbage collector controller is using in real time and cheaply.
// this allows us to invoke it in a high frequency without the overhead
// of other alternatives like calling the runtime.ReadMemStats().
//
// using go:linkname is considered bad practice and should be avoided.
// it is used here since there is no other way to obtain those values
// in tight coupling to the real garbage collector algorithm.
//
// I am hoping that in the future, go will add a way to obtain these values
// idiomatically, but until then, this is the best we can do.
//
//go:linkname runtimeGCController runtime.gcController
var runtimeGCController gcControllerState

//go:linkname runtimeHeapGoal runtime.(*gcControllerState).heapGoal
func runtimeHeapGoal(*gcControllerState) uint64

// following struct is a mirror of the exact struct used by the go runtime.
// notice that it must match exactly (field order and types).
// if go ever changes the internal struct, this need to be updated as well,
// or we can get invalid values when accessing those fields.
type gcControllerState struct {
	gcPercent                  atomic.Int32
	memoryLimit                atomic.Int64
	heapMinimum                uint64
	runway                     atomic.Uint64
	consMark                   float64
	lastConsMark               [4]float64
	gcPercentHeapGoal          atomic.Uint64
	sweepDistMinTrigger        atomic.Uint64
	triggered                  uint64
	lastHeapGoal               uint64
	heapLive                   atomic.Uint64
	heapScan                   atomic.Uint64
	lastHeapScan               uint64
	lastStackScan              atomic.Uint64
	maxStackScan               atomic.Uint64
	globalsScan                atomic.Uint64
	heapMarked                 uint64
	heapScanWork               atomic.Int64
	stackScanWork              atomic.Int64
	globalsScanWork            atomic.Int64
	bgScanCredit               atomic.Int64
	assistTime                 atomic.Int64
	dedicatedMarkTime          atomic.Int64
	fractionalMarkTime         atomic.Int64
	idleMarkTime               atomic.Int64
	markStartTime              int64
	dedicatedMarkWorkersNeeded atomic.Int64
	idleMarkWorkers            atomic.Uint64
	assistWorkPerByte          atomic.Uint64 // This was Float64 originally (from go internals). not used so don't matter
	assistBytesPerWork         atomic.Uint64 // This was Float64 originally (from go internals). not used so don't matter
	fractionalUtilizationGoal  float64

	// fields used for memory limiting goal calculation
	heapInUse    sysMemStat
	heapReleased sysMemStat
	heapFree     sysMemStat
	totalAlloc   atomic.Uint64
	totalFree    atomic.Uint64
	mappedReady  atomic.Uint64

	test bool
	_    [64]byte
}

// Call this function to check if the memory limit of the process is reached
// and react according to the boolean return value.
//
// When the function returns "false", the process can handle new workload safely
// without risking to exceed the memory limit and cause OutOfMemory termination.
//
// When the function returns "true", the process is at risk of exceeding the memory limit
// and should reject avoid creating new expensive allocations by reject or stop pulling new work.
//
// The function is thread safe, and can be called concurrently.
// It is realtively "cheap" to call, and can be called in high frequency (on each request or workload item),
// but NOT in a busy loop.
// Use judgement and avoid calling it way too often as it still accessing atomic values and execute computations.
//
// The function uses inconsistent view of the garbage collector controller state,
// and might occasionally make mistakes, especially during active garbage collection phases.
// It is trading off consistency for performance.
// This limitation is usually acceptable for real world applications,
// as false positive workload will be rejected and then retried by the sender,
// and false negative will be handled by the memory headroom that is kept for those inaccuracies.
//
// It is important to understand that this function is heuristic in it's nature,
// and is expected to produce correct results most of the time, but not always.
func IsMemLimitReached() bool {

	// fast check - if the mapped memory is below the limit, we are good.
	// this check is expected to cover most cases (normal operationwhen memory limit is not reached)
	memoryLimit := runtimeGCController.memoryLimit.Load()
	mappedReady := runtimeGCController.mappedReady.Load()
	if uint64(memoryLimit) > mappedReady {
		return false
	}

	// any bytes in heap free are accounted for in mappedReady,
	// but is available space to make new allocations.
	heapFree := runtimeGCController.heapFree.load()
	if uint64(memoryLimit) > (mappedReady - heapFree) {
		return false
	}

	// this is the "correct" check to make (which follows what go runtime is doing).
	// it will compare the heap live with the heap goal.
	// if we are above the goal, it means a GC cycle could not lower the memory limit to acceptable level.
	heapGoal := runtimeHeapGoal(&runtimeGCController)
	heapLive := runtimeGCController.heapLive.Load()

	if heapLive < heapGoal {
		// we are below the goal, we are good, no garbage collection is needed.
		return false
	}

	// live heap is above the goal => we are not able to make new allocations safely.
	return true
}

// handy for debugging, troubleshooting, or gaining deep insights into the memory limiting state of the application.
type MemLimitRelatedStats struct {

	// The value of the memory limit in bytes (GOMEMLIMIT).
	// This value can also be overridden from code with SetMemoryLimit function.
	MemoryLimit uint64

	// The goal of the heap size in bytes.
	// On it's own it's not very useful, but when compared to HeapLive,
	// it can be used to understand if we are above the goal (when garbage collection should be ended),
	// or below the goal and how much.
	HeapGoal uint64

	// The current live heap size in bytes.
	// This value only counts "spans" of memory, meaning that once we allocate
	// few pages for a span to host future objects, those are all counted at once as live.
	// it can also include dead objects which the garbage collector did not collect yet.
	HeapLive uint64

	// Number of bytes that the go runtime is considering as "ready",
	// e.g. they are counted towards the global memory limit.
	MappedReady uint64

	// memory that is considered "ready" from the OS view,
	// counted in resident set memory (for the purposes of the memory limit)
	// not used by the heap (can be used for future allocations, or freed back to the OS)
	HeapFree uint64

	// TotalAlloc and TotalFree are monotonic counters that are incremented
	// whenever we allocate or free memory (in "span" resolution).
	// their subtraction is the amount of allocated memory.
	// it should match or be close to the HeapLive, except for
	// when the garbage collector finished marking but not yet sweeping.
	TotalAlloc uint64
	TotalFree  uint64
}

// return an inconsistent view of the memory limiting state of the application.
// the values are probed one by one, and a concurrent change to them can rarely
// cause the number to describe an inconsistent state of the application.
// It should be used for debugging, troubleshooting,
// or gaining deep insights into the memory limiting state of the application.
// To get consistent view (with trade-off of performance), use runtime.ReadMemStats() instead.
func GetMemLimitRelatedStats() MemLimitRelatedStats {
	heapGoal := runtimeHeapGoal(&runtimeGCController)
	return MemLimitRelatedStats{
		MemoryLimit: uint64(runtimeGCController.memoryLimit.Load()),
		HeapGoal:    heapGoal,
		HeapLive:    runtimeGCController.heapLive.Load(),
		MappedReady: runtimeGCController.mappedReady.Load(),
		HeapFree:    runtimeGCController.heapFree.load(),
		TotalAlloc:  runtimeGCController.totalAlloc.Load(),
		TotalFree:   runtimeGCController.totalFree.Load(),
	}
}
