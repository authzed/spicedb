//go:build !unix && !windows

package allocator

import "github.com/tetratelabs/wazero/experimental"

var pageSize = 0 // used only for test

func alloc(cap, max uint64) experimental.LinearMemory {
	return sliceAlloc(cap, max)
}
