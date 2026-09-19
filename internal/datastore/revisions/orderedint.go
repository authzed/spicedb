package revisions

import "encoding/binary"

// appendOrderedInt64 appends v as 8 bytes that sort in numeric order:
//
//	-2 -> 7f ff ff ff ff ff ff fe
//	-1 -> 7f ff ff ff ff ff ff ff
//	 0 -> 80 00 00 00 00 00 00 00
//	 1 -> 80 00 00 00 00 00 00 01
//
// Flipping the top bit is what does it: left alone, -1 would encode as ff ff ff ff ff ff ff ff and
// sort above every positive value.
func appendOrderedInt64(dst []byte, v int64) []byte {
	//nolint:gosec // The conversion is the encoding: each int64 maps to its own uint64.
	return binary.BigEndian.AppendUint64(dst, uint64(v)^(1<<63))
}
