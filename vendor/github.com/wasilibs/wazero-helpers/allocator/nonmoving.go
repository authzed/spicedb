package allocator

import (
	"errors"

	"github.com/tetratelabs/wazero/experimental"
)

var errInvalidReallocation = errors.New("allocator: invalid reallocation request: size exceeds reserved address space")

// NewNonMoving returns a [experimental.MemoryAllocator] that will reserve
// address space up to the maximum requested by a WebAssembly module during
// instantiation, ensuring that the base address of memory never changes even
// when the module requests memory to grow - the memory is never moved.
//
// On unix or windows, this reservation will not commit memory, allowing the
// allocation to succeed even if the requested size is more than available memory.
// On other platforms, it will follow the same behavior as wazero's default for
// shared memory, which uses Go's make([]byte, size) to pre-allocate a slice of
// the requested size, which may or may not work when more than available memory
// depending on the machine's settings related to overcommitting memory.
//
// On unix, this internally uses Mmap while on windows it uses VirtualAlloc, so
// it is appropriate to use the allocator for advanced use cases such as mapping
// an external file into the Wasm module's memory space.
func NewNonMoving() experimental.MemoryAllocator {
	return experimental.MemoryAllocatorFunc(alloc)
}
