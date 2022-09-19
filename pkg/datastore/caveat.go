package datastore

import (
	"errors"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ErrCaveatNotFound is the error returned when a caveat is not found by either ID or name
var ErrCaveatNotFound = errors.New("caveat not found")

// CaveatID serves as the system identifier for a persisted Caveat
type CaveatID uint64

// CaveatReader offers read operations for caveats
type CaveatReader interface {
	// ReadCaveatByName returns a caveat with the provided name
	ReadCaveatByName(name string) (*core.Caveat, error)
	// ReadCaveatByID returns a caveat with the provided ID
	ReadCaveatByID(ID CaveatID) (*core.Caveat, error)
}

// CaveatStorer offers both read and write operations for Caveats
type CaveatStorer interface {
	CaveatReader
	// WriteCaveats stores the provided caveats, and returns the assigned IDs
	// Each element o the returning slice corresponds by possition to the input slice
	WriteCaveats([]*core.Caveat) ([]CaveatID, error)
	// DeleteCaveats deletes the provided caveats
	DeleteCaveats([]*core.Caveat) error
}
