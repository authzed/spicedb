package memdb

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/util"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/hashicorp/go-memdb"
)

const tableCaveats = "caveats"

type caveat struct {
	name       string
	definition []byte
	revision   datastore.Revision
}

func (c *caveat) Unwrap() (*core.CaveatDefinition, error) {
	definition := core.CaveatDefinition{}
	err := definition.UnmarshalVT(c.definition)
	return &definition, err
}

func (r *memdbReader) ReadCaveatByName(_ context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	if !r.enableCaveats {
		return nil, datastore.NoRevision, fmt.Errorf("caveats are not enabled")
	}

	r.lockOrPanic()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, datastore.NoRevision, err
	}
	return r.readUnwrappedCaveatByName(tx, name)
}

func (r *memdbReader) readCaveatByName(tx *memdb.Txn, name string) (*caveat, datastore.Revision, error) {
	found, err := tx.First(tableCaveats, indexID, name)
	if err != nil {
		return nil, datastore.NoRevision, err
	}
	if found == nil {
		return nil, datastore.NoRevision, datastore.NewCaveatNameNotFoundErr(name)
	}
	cvt := found.(*caveat)
	return cvt, cvt.revision, nil
}

func (r *memdbReader) readUnwrappedCaveatByName(tx *memdb.Txn, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	c, rev, err := r.readCaveatByName(tx, name)
	if err != nil {
		return nil, datastore.NoRevision, err
	}
	unwrapped, err := c.Unwrap()
	if err != nil {
		return nil, datastore.NoRevision, err
	}
	return unwrapped, rev, nil
}

func (r *memdbReader) ListCaveats(_ context.Context, caveatNames ...string) ([]*core.CaveatDefinition, error) {
	if !r.enableCaveats {
		return nil, fmt.Errorf("caveats are not enabled")
	}

	r.lockOrPanic()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, err
	}

	var caveats []*core.CaveatDefinition
	it, err := tx.LowerBound(tableCaveats, indexID)
	if err != nil {
		return nil, err
	}

	setOfCaveats := util.NewSet(caveatNames...)
	for foundRaw := it.Next(); foundRaw != nil; foundRaw = it.Next() {
		rawCaveat := foundRaw.(*caveat)
		if !setOfCaveats.IsEmpty() && !setOfCaveats.Has(rawCaveat.name) {
			continue
		}
		definition, err := rawCaveat.Unwrap()
		if err != nil {
			return nil, err
		}
		caveats = append(caveats, definition)
	}

	return caveats, nil
}

<<<<<<< HEAD
func (rwt *memdbReadWriteTx) WriteCaveats(caveats []*core.CaveatDefinition) error {
=======
func (r *memdbReader) readUnwrappedCaveatByName(tx *memdb.Txn, name string) (*core.Caveat, error) {
	c, err := r.readCaveatByName(tx, name)
	if err != nil {
		return nil, err
	}
	return c.Unwrap(), nil
}

func (rwt *memdbReadWriteTx) WriteCaveats(ctx context.Context, caveats []*core.Caveat) error {
>>>>>>> 5786ed8d (Change type system to use a Resolver interface for clearer instantiation)
	rwt.lockOrPanic()
	defer rwt.Unlock()
	tx, err := rwt.txSource()
	if err != nil {
		return err
	}
	return rwt.writeCaveat(tx, caveats)
}

func (rwt *memdbReadWriteTx) writeCaveat(tx *memdb.Txn, caveats []*core.CaveatDefinition) error {
	caveatNames := util.NewSet[string]()
	for _, coreCaveat := range caveats {
		if !caveatNames.Add(coreCaveat.Name) {
			return fmt.Errorf("duplicate caveat %s", coreCaveat.Name)
		}
		marshalled, err := coreCaveat.MarshalVT()
		if err != nil {
			return err
		}
		c := caveat{
			name:       coreCaveat.Name,
			definition: marshalled,
			revision:   rwt.newRevision,
		}
		if err := tx.Insert(tableCaveats, &c); err != nil {
			return err
		}
	}
	return nil
}

<<<<<<< HEAD
func (rwt *memdbReadWriteTx) DeleteCaveats(names []string) error {
=======
func (rwt *memdbReadWriteTx) DeleteCaveats(ctx context.Context, caveats []*core.Caveat) error {
>>>>>>> 5786ed8d (Change type system to use a Resolver interface for clearer instantiation)
	rwt.lockOrPanic()
	defer rwt.Unlock()
	tx, err := rwt.txSource()
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := tx.Delete(tableCaveats, caveat{name: name}); err != nil {
			return err
		}
	}
	return nil
}
