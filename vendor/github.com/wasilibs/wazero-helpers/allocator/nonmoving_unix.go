//go:build unix

package allocator

import (
	"fmt"
	"math"
	"sync"

	"github.com/tetratelabs/wazero/experimental"
	"golang.org/x/sys/unix"
)

var pageSize = unix.Getpagesize()

func alloc(_, max uint64) experimental.LinearMemory {
	// Round up to the page size because recommitting must be page-aligned.
	// In practice, the WebAssembly page size should be a multiple of the system
	// page size on most if not all platforms and rounding will never happen.
	rnd := uint64(pageSize - 1)
	reserved := (max + rnd) &^ rnd

	if reserved > math.MaxInt {
		// This ensures int(max) overflows to a negative value,
		// and unix.Mmap returns EINVAL.
		reserved = math.MaxUint64
	}

	// Reserve max bytes of address space, to ensure we won't need to move it.
	// A protected, private, anonymous mapping should not commit memory.
	b, err := unix.Mmap(-1, 0, int(reserved), unix.PROT_NONE, unix.MAP_PRIVATE|unix.MAP_ANON)
	if err != nil {
		panic(fmt.Errorf("allocator_unix: failed to reserve memory: %w", err))
	}
	return &mmappedMemory{buf: b[:0], max: max}
}

// The slice covers the entire mmapped memory:
//   - len(buf) is the already committed memory,
//   - cap(buf) is the reserved address space, which is max rounded up to a page.
type mmappedMemory struct {
	buf []byte
	max uint64

	// Any reasonable Wasm implementation will take a lock before calling Grow, but this
	// is invisible to Go's race detector so it can still detect raciness when we updated
	// buf. We go ahead and take a lock when mutating since the performance effect should
	// be negligible in practice and it will help the race detector confirm the safety.
	mu sync.Mutex
}

func (m *mmappedMemory) Reallocate(size uint64) []byte {
	if size > m.max {
		panic(errInvalidReallocation)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	com := uint64(len(m.buf))
	if com < size {
		// Round up to the page size.
		rnd := uint64(unix.Getpagesize() - 1)
		newCap := (size + rnd) &^ rnd

		// Commit additional memory up to new bytes.
		err := unix.Mprotect(m.buf[com:newCap], unix.PROT_READ|unix.PROT_WRITE)
		if err != nil {
			panic(fmt.Errorf("allocator_unix: failed to commit memory: %w", err))
		}

		// Update committed memory.
		m.buf = m.buf[:newCap]
	}
	// Limit returned capacity because bytes beyond
	// len(m.buf) have not yet been committed.
	return m.buf[:size:len(m.buf)]
}

func (m *mmappedMemory) Free() {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := unix.Munmap(m.buf[:cap(m.buf)])
	if err != nil {
		panic(fmt.Errorf("allocator_unix: failed to release memory: %w", err))
	}
	m.buf = nil
}
