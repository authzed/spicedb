package allocator

import "github.com/tetratelabs/wazero/experimental"

// Separate implementation of non-Unix/Windows code to file without
// build tag to allow testing on any platform.

func sliceAlloc(_, max uint64) experimental.LinearMemory {
	buf := make([]byte, max)
	return &sliceBuffer{buf: buf[:0]}
}

type sliceBuffer struct {
	buf []byte
}

func (b *sliceBuffer) Free() {}

func (b *sliceBuffer) Reallocate(size uint64) []byte {
	if int(size) > cap(b.buf) {
		panic(errInvalidReallocation)
	}
	return b.buf[:size]
}
