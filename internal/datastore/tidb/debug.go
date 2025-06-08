package tidb

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

// See: https://docs.pingcap.com/tidb/stable/sql-statement-explain
func (ds *Datastore) PreExplainStatements() []string {
	// TiDB's EXPLAIN FORMAT=JSON output is compatible with MySQL 8.0's version 2.
	// No specific pre-explain statements are typically needed for TiDB unless a specific feature is being used.
	return []string{}
}

func (ds *Datastore) BuildExplainQuery(sql string, args []interface{}) (string, []any, error) {
	return "EXPLAIN FORMAT=JSON " + sql, args, nil
}

func (ds *Datastore) ParseExplain(explainJSON string) (datastore.ParsedExplain, error) {
	// Unmarshal the explain JSON.
	// TiDB's EXPLAIN FORMAT=JSON output structure is similar to MySQL's.
	// This parsing logic should generally work, but might need adjustments
	// if there are TiDB-specific fields or differences in structure.
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
