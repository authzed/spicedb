package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
)

// See: https://www.postgresql.org/docs/current/sql-explain.html
type explain struct {
	Plan plan `json:"Plan"`
}

type plan struct {
	NodeType  string `json:"Node Type"`
	IndexName string `json:"Index Name"`
}

func (pgd *pgDatastore) PreExplainStatements() []string {
	return nil
}

func (pgd *pgDatastore) BuildExplainQuery(sql string, args []interface{}) (string, []any, error) {
	return "EXPLAIN (FORMAT JSON) " + sql, args, nil
}

func (pgd *pgDatastore) ParseExplain(explainJSON string) (datastore.ParsedExplain, error) {
	// Unmarshal the explain JSON.
	parsed := []explain{}
	if err := json.Unmarshal([]byte(explainJSON), &parsed); err != nil {
		return datastore.ParsedExplain{}, fmt.Errorf("could not parse explain: %w", err)
	}

	// Extract the index name(s) used.
	indexesUsed := mapz.NewSet[string]()
	for _, p := range parsed {
		if p.Plan.IndexName != "" {
			indexesUsed.Add(p.Plan.IndexName)
		}
	}

	return datastore.ParsedExplain{
		IndexesUsed: indexesUsed.AsSlice(),
	}, nil
}

var _ datastore.SQLDatastore = &pgDatastore{}
