package crdb

import (
	"github.com/authzed/spicedb/pkg/datastore"
)

func (cds *crdbDatastore) BuildExplainQuery(sql string, args []interface{}) (string, []any, error) {
	return "EXPLAIN ANALYZE " + sql, args, nil
}

func (cds *crdbDatastore) ParseExplain(explain string) (datastore.ParsedExplain, error) {
	panic("not implemented yet")
}

var _ datastore.SQLDatastore = &crdbDatastore{}
