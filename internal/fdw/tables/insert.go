package tables

import (
	"context"
	"errors"
	"fmt"
	"slices"

	wire "github.com/jeroenrinzema/psql-wire"
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/authzed/authzed-go/v1"

	"github.com/authzed/spicedb/internal/fdw/common"
)

// InsertStatement represents a parsed INSERT query for SpiceDB tables.
// It handles inserting relationships into SpiceDB.
type InsertStatement struct {
	tableName        string
	colNames         []string
	metadata         map[string]any
	returningColumns []wire.Column
}

func (is *InsertStatement) ReturningColumns() []wire.Column {
	return is.returningColumns
}

func (is *InsertStatement) ExecuteInsert(ctx context.Context, client *authzed.Client, writer wire.DataWriter, parameters []wire.Parameter) error {
	tableDef := tables[is.tableName]
	if tableDef.insertRunner == nil {
		return common.NewUnsupportedError(fmt.Errorf("table %s does not support INSERT", is.tableName))
	}

	countInserted, returningValues, err := tableDef.insertRunner(ctx, client, is, parameters)
	if err != nil {
		return err
	}

	if len(returningValues) > 0 {
		if err := writer.Row(returningValues); err != nil {
			return common.NewQueryError(fmt.Errorf("failed to write returning row: %w", err))
		}
	}

	return writer.Complete(fmt.Sprintf("INSERT 0 %d", countInserted))
}

// ParseInsertStatement parses a Postgres INSERT statement into an InsertStatement.
// Returns an error if the query is not supported or malformed.
func ParseInsertStatement(ctx context.Context, query *pg_query.InsertStmt) (*InsertStatement, error) {
	if query == nil {
		return nil, common.NewUnsupportedError(errors.New("query must be an INSERT statement"))
	}

	tableName := query.GetRelation().GetRelname()
	tableDef, ok := tables[tableName]
	if !ok {
		return nil, common.NewUnsupportedError(fmt.Errorf("table %s does not exist", tableName))
	}
	if tableDef.insertRunner == nil {
		return nil, common.NewUnsupportedError(fmt.Errorf("table %s does not support INSERT", tableName))
	}

	columns := query.GetCols()
	if len(columns) < len(tableDef.requiredInsertColumns) {
		return nil, common.NewQueryError(fmt.Errorf("expected at least %d columns for insert into relationships", len(tableDef.requiredInsertColumns)))
	}

	// Collect the column names, in order.
	colNames := make([]string, 0, len(columns))
	for colIndex, col := range columns {
		if col.GetResTarget() == nil {
			return nil, common.NewQueryError(fmt.Errorf("column %d is missing a name", colIndex))
		}
		colName := col.GetResTarget().Name
		colNames = append(colNames, colName)

		if !tableDef.hasColumn(colName) {
			return nil, common.NewQueryError(fmt.Errorf("column %s does not exist on table %s", colName, tableName))
		}
	}

	// Ensure all required columns were specified.
	for _, requiredCol := range tableDef.requiredInsertColumns {
		if !slices.Contains(colNames, requiredCol) {
			return nil, common.NewQueryError(fmt.Errorf("missing required column %s for insert into relationships", requiredCol))
		}
	}

	var metadata map[string]any
	if tableDef.insertValidator != nil {
		md, err := tableDef.insertValidator(ctx, query, colNames)
		if err != nil {
			return nil, err
		}
		metadata = md
	}

	if query.Override != pg_query.OverridingKind_OVERRIDING_NOT_SET {
		return nil, common.NewUnsupportedError(errors.New("INSERT ... OVERRIDING is not supported"))
	}

	returningColumns, err := returningColumnsFromQuery(tableDef, query)
	if err != nil {
		return nil, err
	}

	return &InsertStatement{
		tableName:        tableName,
		colNames:         colNames,
		metadata:         metadata,
		returningColumns: returningColumns,
	}, nil
}
