package proxy

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

var errReadOnly = datastore.NewReadonlyErr()

type roDatastore struct {
	datastore.Datastore
}

// NewReadonlyDatastore creates a proxy which disables write operations to a downstream delegate
// datastore.
func NewReadonlyDatastore(delegate datastore.Datastore) datastore.Datastore {
	return roDatastore{Datastore: delegate}
}

func (rd roDatastore) ReadWriteTx(
	context.Context,
	datastore.TxUserFunc,
	...options.RWTOptionsOption,
) (datastore.Revision, error) {
	return datastore.NoRevision, errReadOnly
}
