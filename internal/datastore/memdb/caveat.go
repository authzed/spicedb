package memdb

import (
	"errors"
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/hashicorp/go-memdb"
)

const tableCaveats = "caveats"

type caveat struct {
	id         datastore.CaveatID
	name       string
	expression []byte
}

func (c *caveat) Unwrap() *core.Caveat {
	return &core.Caveat{
		Name:       c.name,
		Expression: c.expression,
	}
}

func (r *memdbReader) ReadCaveatByName(name string) (*core.Caveat, error) {
	r.lockOrPanic()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, err
	}
	return r.readUnwrappedCaveatByName(tx, name)
}

func (r *memdbReader) ReadCaveatByID(ID datastore.CaveatID) (*core.Caveat, error) {
	r.lockOrPanic()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, err
	}
	return r.readCaveatByID(tx, ID)
}

func (r *memdbReader) readCaveatByID(tx *memdb.Txn, ID datastore.CaveatID) (*core.Caveat, error) {
	found, err := tx.First(tableCaveats, indexID, ID)
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, datastore.NewCaveatIDNotFoundErr(ID)
	}
	c := found.(*caveat)
	return c.Unwrap(), nil
}

func (r *memdbReader) readCaveatByName(tx *memdb.Txn, name string) (*caveat, error) {
	found, err := tx.First(tableCaveats, indexName, name)
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, datastore.NewCaveatNameNotFoundErr(name)
	}
	return found.(*caveat), nil
}

func (r *memdbReader) readUnwrappedCaveatByName(tx *memdb.Txn, name string) (*core.Caveat, error) {
	c, err := r.readCaveatByName(tx, name)
	if err != nil {
		return nil, err
	}
	return c.Unwrap(), nil
}

func (rwt *memdbReadWriteTx) WriteCaveats(caveats []*core.Caveat) ([]datastore.CaveatID, error) {
	rwt.lockOrPanic()
	defer rwt.Unlock()
	tx, err := rwt.txSource()
	if err != nil {
		return nil, err
	}
	return rwt.writeCaveat(tx, caveats)
}

func (rwt *memdbReadWriteTx) writeCaveat(tx *memdb.Txn, caveats []*core.Caveat) ([]datastore.CaveatID, error) {
	ids := make([]datastore.CaveatID, 0, len(caveats))
	for _, coreCaveat := range caveats {
		id := datastore.CaveatID(time.Now().UnixNano())
		c := caveat{
			id:         id,
			name:       coreCaveat.Name,
			expression: coreCaveat.Expression,
		}
		// in order to implement upserts we need to determine the ID of the previously
		// stored caveat
		found, err := rwt.readCaveatByName(tx, coreCaveat.Name)
		if err != nil && !errors.As(err, &datastore.ErrCaveatNameNotFound{}) {
			return nil, err
		}
		if found != nil {
			id = found.id
			c.id = id
		}
		if err = tx.Insert(tableCaveats, &c); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (rwt *memdbReadWriteTx) DeleteCaveats(caveats []*core.Caveat) error {
	rwt.lockOrPanic()
	defer rwt.Unlock()
	tx, err := rwt.txSource()
	if err != nil {
		return err
	}
	return tx.Delete(tableCaveats, caveats)
}
