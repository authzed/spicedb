package fdw

import (
	"context"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/authzed/spicedb/internal/fdw/explain"
	"github.com/authzed/spicedb/internal/fdw/tables"
)

var explainSchema = wire.Columns{
	{
		Table: 0,
		Name:  "QUERY PLAN",
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
}

func (p *PgBackend) handleExplainStmt(_ context.Context, stmt *pg_query.ExplainStmt, _ string) (wire.PreparedStatements, error) {
	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		selectStatement, err := tables.ParseSelectStatement(stmt.GetQuery().GetSelectStmt(), true)
		if selectStatement != nil && selectStatement.IsUnsupported() {
			// Return an EXPLAIN plan that shows an insanely high cost to ensure PG does not select the unsupported
			// query plan.
			if err := writer.Row([]any{explain.Unsupported}); err != nil {
				return err
			}
			return writer.Complete("EXPLAIN")
		}

		if err != nil {
			return err
		}

		return selectStatement.Explain(ctx, p.client, writer)
	}
	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(explainSchema))), nil
}
