package datastore

import (
	"context"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// CaveatReader offers read operations for caveats
type CaveatReader interface {
	// ReadCaveatByName returns a caveat with the provided name
	ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, Revision, error)

	// ListCaveats returns all caveats stored in the system. If caveatNames are provided
	// the result will be filtered to the provided caveat names
	ListCaveats(ctx context.Context, caveatNamesForFiltering ...string) ([]*core.CaveatDefinition, error)
}

// CaveatStorer offers both read and write operations for Caveats
type CaveatStorer interface {
	CaveatReader

	// WriteCaveats stores the provided caveats, and returns the assigned IDs
	// Each element o the returning slice corresponds by possition to the input slice
	WriteCaveats([]*core.CaveatDefinition) error

	// DeleteCaveats deletes the provided caveats by name
	DeleteCaveats(names []string) error
}
