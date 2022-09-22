package datastore

import (
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// CaveatReader offers read operations for caveats
type CaveatReader interface {
	// ReadCaveatByName returns a caveat with the provided name
	ReadCaveatByName(name string) (*core.Caveat, error)
}

// CaveatStorer offers both read and write operations for Caveats
type CaveatStorer interface {
	CaveatReader

	// WriteCaveats stores the provided caveats, and returns the assigned IDs
	// Each element o the returning slice corresponds by possition to the input slice
	WriteCaveats([]*core.Caveat) error

	// DeleteCaveats deletes the provided caveats
	DeleteCaveats([]*core.Caveat) error
}

// ErrCaveatNameNotFound is the error returned when a caveat is not found by its name
type ErrCaveatNameNotFound struct {
	error
	name string
}

// CaveatName returns the name of the caveat that couldn't be found
func (e ErrCaveatNameNotFound) CaveatName() string {
	return e.name
}

// NewCaveatNameNotFoundErr constructs a new caveat name not found error.
func NewCaveatNameNotFoundErr(name string) error {
	return ErrCaveatNameNotFound{
		error: fmt.Errorf("caveat with name `%s` not found", name),
		name:  name,
	}
}
