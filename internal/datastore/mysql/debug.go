package mysql

import (
	"encoding/json"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
)

type explain struct {
	Query  string  `json:"query"`
	Inputs []input `json:"inputs"`
}

type input struct {
	IndexName  string `json:"index_name"`
	AccessType string `json:"access_type"`
}

// See: https://dev.mysql.com/doc/refman/8.4/en/explain.html
func (mds *Datastore) PreExplainStatements() []string {
	return []string{
		"SET @@explain_json_format_version = 2;",
	}
}

func (mds *Datastore) BuildExplainQuery(sql string, args []interface{}) (string, []any, error) {
	return "EXPLAIN FORMAT=JSON " + sql, args, nil
}

func (mds *Datastore) ParseExplain(explainJSON string) (datastore.ParsedExplain, error) {
	// Unmarshal the explain JSON.
	parsed := explain{}
	if err := json.Unmarshal([]byte(explainJSON), &parsed); err != nil {
		return datastore.ParsedExplain{}, fmt.Errorf("could not parse explain: %w", err)
	}

	// Extract the index name(s) used.
	indexesUsed := mapz.NewSet[string]()
	for _, input := range parsed.Inputs {
		if input.AccessType == "index" && input.IndexName != "" {
			indexesUsed.Add(input.IndexName)
		}
	}

	return datastore.ParsedExplain{
		IndexesUsed: indexesUsed.AsSlice(),
	}, nil
}

var _ datastore.SQLDatastore = &Datastore{}
