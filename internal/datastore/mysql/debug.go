package mysql

import (
	"github.com/authzed/spicedb/pkg/datastore"
)

func (mds *Datastore) BuildExplainQuery(sql string, args []interface{}) (string, []any, error) {
	return "EXPLAIN ANALYZE " + sql, args, nil
}

func (mds *Datastore) ParseExplain(explain string) (datastore.ParsedExplain, error) {
	panic("not implemented yet")
}

var _ datastore.SQLDatastore = &Datastore{}
