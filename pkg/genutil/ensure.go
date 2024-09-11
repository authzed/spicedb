package genutil

import (
	"github.com/ccoveille/go-safecast"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

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
	uint32Value, err := safecast.ToUint32(value)
	if err != nil {
		return 0, spiceerrors.MustBugf("specified value could not be cast to a uint32")
	}
	return uint32Value, nil
}

// EnsureUInt8 ensures that the specified value can be represented as a uint8.
func EnsureUInt8(value int) (uint8, error) {
	uint8Value, err := safecast.ToUint8(value)
	if err != nil {
		return 0, spiceerrors.MustBugf("specified value could not be cast to a uint8")
	}
	return uint8Value, nil
}
