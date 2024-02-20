package genutil

import "github.com/authzed/spicedb/pkg/spiceerrors"

// MustEnsureUInt32 is a helper function that calls EnsureUInt32 and panics on error.
func MustEnsureUInt32(value int) uint32 {
	ret, err := EnsureUInt32(value)
	if err != nil {
		panic(err)
	}
	return ret
}

// EnsureUInt32 ensures that the specified value can be represented as a uint32.
func EnsureUInt32(value int) (uint32, error) {
	if value > int(^uint32(0)) {
		return 0, spiceerrors.MustBugf("specified value is too large to fit in a uint32")
	}
	return uint32(value), nil
}

// EnsureUInt8 ensures that the specified value can be represented as a uint8.
func EnsureUInt8(value int) (uint8, error) {
	if value > int(^uint8(0)) {
		return 0, spiceerrors.MustBugf("specified value is too large to fit in a uint8")
	}
	return uint8(value), nil
}
