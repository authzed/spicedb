// Package goroutineid exposes the ID of the calling goroutine, for debugging and test-only
// checks that need to associate state with a goroutine. It is too slow for use on hot paths in
// production builds.
package goroutineid

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
)

var goroutinePrefix = []byte("goroutine ")

// Current returns the ID of the calling goroutine.
func Current() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	b := bytes.TrimPrefix(buf[:n], goroutinePrefix)
	if i := bytes.IndexByte(b, ' '); i >= 0 {
		b = b[:i]
	}
	id, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("goroutineid: unable to parse goroutine ID: %v", err))
	}
	return id
}
