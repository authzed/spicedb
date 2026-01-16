package fdw

import (
	"context"
	"errors"
	"maps"

	wire "github.com/jeroenrinzema/psql-wire"
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/authzed/spicedb/internal/fdw/common"
	"github.com/authzed/spicedb/internal/fdw/tables"
)

type cursor struct {
	name       string
	selectStmt *tables.SelectStatement
	parameters []wire.Parameter
	rowsCursor tables.RowsCursor
}

// See: https://www.postgresql.org/docs/current/sql-declare.html
func (p *PgBackend) handleDeclareCursorStmt(_ context.Context, stmt *pg_query.DeclareCursorStmt, query string) (wire.PreparedStatements, error) {
	selectStmt, err := tables.ParseSelectStatement(stmt.Query.GetSelectStmt(), false)
	if err != nil {
		return nil, err
	}

	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		session := mustSession(ctx)
		session.addCursor(&cursor{stmt.Portalname, selectStmt, parameters, nil})
		return writer.Complete("declare cursor completed")
	}

	params := wire.ParseParameters(query)
	return wire.Prepared(wire.NewStatement(handle, wire.WithParameters(params))), nil
}

// See: https://www.postgresql.org/docs/current/sql-fetch.html
func (p *PgBackend) handleFetchStmt(ctx context.Context, stmt *pg_query.FetchStmt, _ string) (wire.PreparedStatements, error) {
	session := mustSession(ctx)
	cursorName := stmt.Portalname
	if cursorName == "" {
		session.mu.Lock()
		if len(session.cursors) != 1 {
			session.mu.Unlock()
			return nil, common.NewSemanticsError(errors.New("cursor name is required when multiple cursors are present"))
		}
		cursorName = mustFirst(maps.Keys(session.cursors))
		session.mu.Unlock()
	}

	if stmt.Direction != pg_query.FetchDirection_FETCH_FORWARD {
		return nil, common.NewUnsupportedError(errors.New("only forward fetch direction is supported"))
	}

	cursor, err := session.lookupCursor(cursorName)
	if err != nil {
		return nil, err
	}

	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		updatedRowsCursor, err := cursor.selectStmt.Fetch(ctx, p.client, parameters, writer, stmt.HowMany, cursor.rowsCursor, cursor.selectStmt)
		if err != nil {
			return err
		}
		cursor.rowsCursor = updatedRowsCursor
		return nil
	}
	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(cursor.selectStmt.Schema()))), nil
}

// See: https://www.postgresql.org/docs/current/sql-close.html
func (p *PgBackend) handleClosePortalStmt(_ context.Context, stmt *pg_query.ClosePortalStmt, _ string) (wire.PreparedStatements, error) {
	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		session := mustSession(ctx)
		cursorName := stmt.Portalname
		if cursorName == "" {
			session.mu.Lock()
			if len(session.cursors) != 1 {
				session.mu.Unlock()
				return common.NewSemanticsError(errors.New("cursor name is required when multiple cursors are present"))
			}
			cursorName = mustFirst(maps.Keys(session.cursors))
			session.mu.Unlock()
		}

		session.deleteCursor(cursorName)
		return writer.Complete("CLOSED")
	}
	return wire.Prepared(wire.NewStatement(handle)), nil
}
