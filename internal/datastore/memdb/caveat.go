package memdb

import (
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/hashicorp/go-memdb"
)

const tableCaveats = "caveats"

type caveat struct {
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

func (r *memdbReader) readCaveatByName(tx *memdb.Txn, name string) (*caveat, error) {
	found, err := tx.First(tableCaveats, indexID, name)
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

func (rwt *memdbReadWriteTx) WriteCaveats(caveats []*core.Caveat) error {
	rwt.lockOrPanic()
	defer rwt.Unlock()
	tx, err := rwt.txSource()
	if err != nil {
		return err
	}
	return rwt.writeCaveat(tx, caveats)
}

func (rwt *memdbReadWriteTx) writeCaveat(tx *memdb.Txn, caveats []*core.Caveat) error {
	for _, coreCaveat := range caveats {
		c := caveat{
			name:       coreCaveat.Name,
			expression: coreCaveat.Expression,
		}
		if err := tx.Insert(tableCaveats, &c); err != nil {
			return err
		}
	}
	return nil
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
