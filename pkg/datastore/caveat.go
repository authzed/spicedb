package datastore

import (
	"context"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// CaveatReader offers read operations for caveats
type CaveatReader interface {
	// ReadCaveatByName returns a caveat with the provided name.
	// It returns an instance of ErrCaveatNotFound if not found.
	ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, Revision, error)

	// ListAllCaveats returns all caveats stored in the system.
	ListAllCaveats(ctx context.Context) ([]*core.CaveatDefinition, error)

	// LookupCaveatsWithNames finds all caveats with the matching names.
	LookupCaveatsWithNames(ctx context.Context, names []string) ([]*core.CaveatDefinition, error)
}

// CaveatStorer offers both read and write operations for Caveats
type CaveatStorer interface {
	CaveatReader

	// WriteCaveats stores the provided caveats, and returns the assigned IDs
	// Each element of the returning slice corresponds by possition to the input slice
	WriteCaveats(context.Context, []*core.CaveatDefinition) error

	// DeleteCaveats deletes the provided caveats by name
	DeleteCaveats(ctx context.Context, names []string) error
}
