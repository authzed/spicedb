package tables

import (
	"context"
	"errors"
	"fmt"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"

	"github.com/authzed/spicedb/internal/fdw/common"
	"github.com/authzed/spicedb/internal/fdw/explain"
	log "github.com/authzed/spicedb/internal/logging"
)

// schemaField represents a field name in the schema table
type schemaField string

const (
	schemaSchemaTextField schemaField = "schema_text"
)

var schemaSchema = wire.Columns{
	{
		Table: 0,
		Name:  string(schemaSchemaTextField),
		Oid:   uint32(oid.T_text),
		Width: 256,
	},
}

func schemaInsertRunner(ctx context.Context, client *authzed.Client, insertStmt *InsertStatement, parameters []wire.Parameter) (int64, []any, error) {
	if len(insertStmt.returningColumns) > 0 {
		return 0, nil, common.NewQueryError(errors.New("unexpected RETURNING clause"))
	}

	valuesByColumnName := make(map[string]string, len(insertStmt.colNames))
	for i, colName := range insertStmt.colNames {
		valuesByColumnName[colName] = string(parameters[i].Value())
	}

	_, err := client.WriteSchema(ctx, &v1.WriteSchemaRequest{
		Schema: valuesByColumnName[string(schemaSchemaTextField)],
	})
	if err != nil {
		return 0, nil, common.NewQueryError(fmt.Errorf("failed to write schema: %w", err))
	}

	return 1, nil, nil
}

func buildSchemaSelectHandler(fields fieldMap[string]) (selectHandler, error) {
	return schemaSelectHandler, nil
}

func schemaSelectHandler(ctx context.Context, client *authzed.Client, parameters []wire.Parameter, howMany int64, writer wire.DataWriter, rowsCursor RowsCursor, selectStmt *SelectStatement) (RowsCursor, error) {
	schema, err := client.ReadSchema(ctx, &v1.ReadSchemaRequest{})
	if err != nil {
		return nil, common.NewQueryError(fmt.Errorf("failed to read schema: %w", err))
	}

	if err := writer.Row([]any{schema.SchemaText}); err != nil {
		return nil, common.NewQueryError(fmt.Errorf("failed to write schema row: %w", err))
	}
	if err := writer.Complete("SELECT 1"); err != nil {
		return nil, common.NewQueryError(fmt.Errorf("failed to complete query: %w", err))
	}

	return nil, nil
}

func buildSchemaExplainHandler(fields fieldMap[string]) (explainHandler, error) {
	return func(ctx context.Context, client *authzed.Client, writer wire.DataWriter) error {
		cost := explain.Default("read", "schema").String()
		log.Debug().Str("cost", cost).Msg("schema explain cost")
		if err := writer.Row([]any{cost}); err != nil {
			return fmt.Errorf("error writing schema explain row: %w", err)
		}
		return writer.Complete("EXPLAIN")
	}, nil
}
