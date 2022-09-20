package datastore

import (
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

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

// ErrCaveatIDNotFound is the error returned when a caveat is not found by its ID
type ErrCaveatIDNotFound struct {
	error
	id CaveatID
}

// CaveatID returns the ID of the caveat that couldn't be found
func (e ErrCaveatIDNotFound) CaveatID() CaveatID {
	return e.id
}

// NewCaveatIDNotFoundErr constructs a new caveat ID not found error.
func NewCaveatIDNotFoundErr(id CaveatID) error {
	return ErrCaveatIDNotFound{
		error: fmt.Errorf("caveat with id `%d` not found", id),
		id:    id,
	}
}
