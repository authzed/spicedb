package crdb

import (
	"regexp"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
)

// See: https://www.cockroachlabs.com/docs/stable/explain

var relationTupleIndexRegex = regexp.MustCompile(`table: relation_tuple@(.+)`)

func (cds *crdbDatastore) PreExplainStatements() []string {
	return nil
}

func (cds *crdbDatastore) BuildExplainQuery(sql string, args []interface{}) (string, []any, error) {
	return "EXPLAIN  " + sql, args, nil
}

func (cds *crdbDatastore) ParseExplain(explain string) (datastore.ParsedExplain, error) {
	// TODO: change to parsing a JSON structure if possible and/or add support for other tables.
	parts := relationTupleIndexRegex.FindAllStringSubmatch(explain, -1)
	if len(parts) == 0 {
		return datastore.ParsedExplain{}, nil
	}

	indexes := mapz.NewSet[string]()
	for _, part := range parts {
		indexes.Add(part[1])
	}

	return datastore.ParsedExplain{
		IndexesUsed: indexes.AsSlice(),
	}, nil
}

var _ datastore.SQLDatastore = &crdbDatastore{}
