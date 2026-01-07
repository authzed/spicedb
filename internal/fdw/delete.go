package fdw

import (
	"context"

	wire "github.com/jeroenrinzema/psql-wire"
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/authzed/spicedb/internal/fdw/tables"
)

func (p *PgBackend) handleDeleteStmt(ctx context.Context, stmt *pg_query.DeleteStmt, query string) (wire.PreparedStatements, error) {
	parsed, err := tables.ParseDeleteStatement(ctx, stmt)
	if err != nil {
		return nil, err
	}

	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		return parsed.ExecuteDelete(ctx, p.client, writer, parameters)
	}

	params := wire.ParseParameters(query)
	return wire.Prepared(wire.NewStatement(handle, wire.WithParameters(params), wire.WithColumns(parsed.ReturningColumns()))), nil
}
