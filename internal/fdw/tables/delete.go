package tables

import (
	"context"
	"errors"
	"fmt"

	wire "github.com/jeroenrinzema/psql-wire"
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/authzed/authzed-go/v1"

	"github.com/authzed/spicedb/internal/fdw/common"
)

// DeleteStatement represents a parsed DELETE query for SpiceDB tables.
// It handles deleting relationships from SpiceDB.
type DeleteStatement struct {
	isUnsupported    bool
	tableDef         tableDefinition
	tableName        string
	returningColumns []wire.Column
	fields           map[string]valueOrRef
}

func (ds *DeleteStatement) ReturningColumns() []wire.Column {
	return ds.returningColumns
}

func (ds *DeleteStatement) ExecuteDelete(ctx context.Context, client *authzed.Client, writer wire.DataWriter, parameters []wire.Parameter) error {
	tableDef := tables[ds.tableName]
	if tableDef.deleteRunner == nil {
		return common.NewUnsupportedError(fmt.Errorf("table %s does not support DELETE", ds.tableName))
	}

	countDeleted, returningValues, err := tableDef.deleteRunner(ctx, client, ds, parameters)
	if err != nil {
		return err
	}

	if len(returningValues) > 0 {
		if err := writer.Row(returningValues); err != nil {
			return common.NewQueryError(fmt.Errorf("failed to write returning row: %w", err))
		}
	}

	return writer.Complete(fmt.Sprintf("DELETE %d", countDeleted))
}

// ParseDeleteStatement parses a Postgres DELETE statement into a DeleteStatement.
// Returns an error if the query is not supported or malformed.
func ParseDeleteStatement(ctx context.Context, query *pg_query.DeleteStmt) (*DeleteStatement, error) {
	if query == nil {
		return nil, common.NewUnsupportedError(errors.New("query must be an DELETE statement"))
	}

	tableName := query.GetRelation().GetRelname()
	tableDef, ok := tables[tableName]
	if !ok {
		return nil, common.NewUnsupportedError(fmt.Errorf("table %s does not exist", tableName))
	}
	if tableDef.deleteRunner == nil {
		return nil, common.NewUnsupportedError(fmt.Errorf("table %s does not support DELETE", tableName))
	}

	if len(query.UsingClause) > 0 {
		return nil, common.NewUnsupportedError(errors.New("DELETE statements with USING are not supported"))
	}

	if query.WithClause != nil {
		return nil, common.NewUnsupportedError(errors.New("DELETE statements with WITH are not supported"))
	}

	returningColumns, err := returningColumnsFromQuery(tableDef, query)
	if err != nil {
		return nil, err
	}

	pm := NewPatternMatcher()
	fields, err := pm.MatchWhereClause(query.GetWhereClause())
	if err != nil {
		return nil, err
	}

	// Ensure all fields match the table definition.
	for fieldName := range fields {
		if !tableDef.hasColumn(fieldName) {
			return &DeleteStatement{
				isUnsupported: true,
				tableName:     tableName,
				tableDef:      tableDef,
			}, common.NewUnsupportedError(fmt.Errorf("field %s does not exist in table %s", fieldName, tableName))
		}
	}

	return &DeleteStatement{
		isUnsupported:    false,
		tableName:        tableName,
		tableDef:         tableDef,
		returningColumns: returningColumns,
		fields:           fields,
	}, nil
}
