package proxy

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
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

func (rd roDatastore) ReadWriteTx(context.Context, datastore.TxUserFunc) (datastore.Revision, error) {
	return datastore.NoRevision, errReadOnly
}
