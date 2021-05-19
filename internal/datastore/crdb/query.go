package crdb

import "github.com/authzed/spicedb/internal/datastore"

func (cds *crdbDatastore) QueryTuples(namespace string, revision uint64) datastore.TupleQuery {
	return nil
}
