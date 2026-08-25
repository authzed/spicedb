package crdb

import (
	"github.com/jackc/pgx/v5/pgtype"
)

// RegisterTypes registers the Go types whose PostgreSQL mapping pgx cannot infer
// on its own under the text-based query exec modes.
//
// Under QueryExecModeExec pgx does not ask the server for parameter OIDs; it
// derives them from the Go type of each argument and rejects anything it cannot
// type unambiguously. A caveat context is passed as a bare map[string]any (see
// crdbReadWriteTXN.WriteRelationships), which pgx would otherwise consider
// ambiguous between json, jsonb and hstore.
func RegisterTypes(m *pgtype.Map) {
	m.RegisterDefaultPgType(map[string]any{}, "jsonb")
}
