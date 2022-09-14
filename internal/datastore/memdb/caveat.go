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
	caveatType core.Caveat_Type
}

func (c *caveat) CoreCaveat() *core.Caveat {
	return &core.Caveat{
		Name:       c.name,
		Expression: c.expression,
		Type:       c.caveatType,
	}
}

func (r *memdbReader) ReadCaveat(name string) (datastore.CaveatIterator, error) {
	r.lockOrPanic()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, err
	}
	return r.readCaveat(tx, name)
}

func (r *memdbReader) readCaveat(tx *memdb.Txn, name string) (datastore.CaveatIterator, error) {
	it, err := tx.Get(tableCaveats, indexID, name)
	if err != nil {
		return nil, err
	}
	return &memdbCaveatIterator{it: it}, nil
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
			caveatType: coreCaveat.Type,
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

type memdbCaveatIterator struct {
	it memdb.ResultIterator
}

func (mci *memdbCaveatIterator) Next() *core.Caveat {
	foundRaw := mci.it.Next()
	if foundRaw == nil {
		return nil
	}

	c := foundRaw.(*caveat)
	return c.CoreCaveat()
}

func (mci *memdbCaveatIterator) Err() error {
	return nil
}

func (mci *memdbCaveatIterator) Close() {}
