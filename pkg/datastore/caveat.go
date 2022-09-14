package datastore

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type CaveatReader interface {
	ReadCaveat(digest string) (CaveatIterator, error)
}

type CaveatStorer interface {
	CaveatReader
	WriteCaveats([]*core.Caveat) error
	DeleteCaveats([]*core.Caveat) error
}

type CaveatIterator interface {
	// Next returns the next caveat in the result set.
	Next() *core.Caveat

	// Err after receiving a nil response, the caller must check for an error.
	Err() error

	// Close cancels the query and closes any open connections.
	Close()
}
