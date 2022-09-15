package datastore

import (
	"errors"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var ErrCaveatNotFound = errors.New("caveat not found")

type CaveatID string

type CaveatReader interface {
	ReadCaveatByName(name string) (*core.Caveat, error)
	ReadCaveatByID(ID CaveatID) (*core.Caveat, error)
}

type CaveatStorer interface {
	CaveatReader
	WriteCaveats([]*core.Caveat) ([]CaveatID, error)
	DeleteCaveats([]*core.Caveat) error
}
