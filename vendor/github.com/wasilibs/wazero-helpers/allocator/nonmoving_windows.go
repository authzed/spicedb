//go:build windows

package allocator

import (
	"fmt"
	"math"
	"sync"
	"unsafe"

	"github.com/tetratelabs/wazero/experimental"
	"golang.org/x/sys/windows"
)

var pageSize = windows.Getpagesize()

func alloc(_, max uint64) experimental.LinearMemory {
	// Round up to the page size because recommitting must be page-aligned.
	// In practice, the WebAssembly page size should be a multiple of the system
	// page size on most if not all platforms and rounding will never happen.
	rnd := uint64(pageSize) - 1
	reserved := (max + rnd) &^ rnd

	if reserved > math.MaxInt {
		// This ensures uintptr(max) overflows to a large value,
		// and windows.VirtualAlloc returns an error.
		reserved = math.MaxUint64
	}

	// Reserve max bytes of address space, to ensure we won't need to move it.
	// This does not commit memory.
	r, err := windows.VirtualAlloc(0, uintptr(reserved), windows.MEM_RESERVE, windows.PAGE_READWRITE)
	if err != nil {
		panic(fmt.Errorf("allocator_windows: failed to reserve memory: %w", err))
	}

	buf := unsafe.Slice((*byte)(unsafe.Pointer(r)), int(reserved))
	return &virtualMemory{buf: buf[:0], addr: r, max: max}
}

// The slice covers the entire mmapped memory:
//   - len(buf) is the already committed memory,
//   - cap(buf) is the reserved address space, which is max rounded up to a page.
type virtualMemory struct {
	buf  []byte
	addr uintptr
	max  uint64

	// Any reasonable Wasm implementation will take a lock before calling Grow, but this
	// is invisible to Go's race detector so it can still detect raciness when we updated
	// buf. We go ahead and take a lock when mutating since the performance effect should
	// be negligible in practice and it will help the race detector confirm the safety.
	mu sync.Mutex
}

func (m *virtualMemory) Reallocate(size uint64) []byte {
	if size > m.max {
		panic(errInvalidReallocation)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	com := uint64(len(m.buf))
	if com < size {
		// Round up to the page size.
		rnd := uint64(pageSize) - 1
		newCap := (size + rnd) &^ rnd

		// Commit additional memory up to new bytes.
		_, err := windows.VirtualAlloc(m.addr, uintptr(newCap), windows.MEM_COMMIT, windows.PAGE_READWRITE)
		if err != nil {
			panic(fmt.Errorf("allocator_windows: failed to commit memory: %w", err))
		}

		// Update committed memory.
		m.buf = m.buf[:newCap]
	}
	// Limit returned capacity because bytes beyond
	// len(m.buf) have not yet been committed.
	return m.buf[:size:len(m.buf)]
}

func (m *virtualMemory) Free() {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := windows.VirtualFree(m.addr, 0, windows.MEM_RELEASE)
	if err != nil {
		panic(fmt.Errorf("allocator_windows: failed to release memory: %w", err))
	}
	m.addr = 0
}
