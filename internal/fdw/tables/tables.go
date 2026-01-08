package tables

import (
	"context"

	wire "github.com/jeroenrinzema/psql-wire"
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/authzed/authzed-go/v1"
)

type (
	insertRunner    func(ctx context.Context, client *authzed.Client, insertStmt *InsertStatement, parameters []wire.Parameter) (int64, []any, error)
	insertValidator func(ctx context.Context, stmt *pg_query.InsertStmt, colNames []string) (map[string]any, error)
)

type (
	deleteRunner func(ctx context.Context, client *authzed.Client, deleteStmt *DeleteStatement, parameters []wire.Parameter) (int64, []any, error)
)

type (
	selectHandler        func(ctx context.Context, client *authzed.Client, parameters []wire.Parameter, howMany int64, writer wire.DataWriter, rowsCursor RowsCursor, selectStmt *SelectStatement) (RowsCursor, error)
	selectHandlerBuilder func(fields fieldMap[string]) (selectHandler, error)
)

type (
	explainHandler        func(ctx context.Context, client *authzed.Client, writer wire.DataWriter) error
	explainHandlerBuilder func(fields fieldMap[string]) (explainHandler, error)
)

type tableDefinition struct {
	name                  string
	schema                wire.Columns
	insertValidator       insertValidator
	insertRunner          insertRunner
	deleteRunner          deleteRunner
	buildSelectHandler    selectHandlerBuilder
	buildExplainHandler   explainHandlerBuilder
	requiredInsertColumns []string
}

func (td tableDefinition) hasColumn(colName string) bool {
	for _, col := range td.schema {
		if col.Name == colName {
			return true
		}
	}
	return false
}

func (td tableDefinition) columnNames() []string {
	names := make([]string, len(td.schema))
	for i, col := range td.schema {
		names[i] = col.Name
	}
	return names
}

func (td tableDefinition) getSchemaColumn(name string) (wire.Column, bool) {
	for _, col := range td.schema {
		if col.Name == name {
			return col, true
		}
	}
	return wire.Column{}, false
}

type fieldMap[F ~string] map[F]valueOrRef

func (m fieldMap[F]) hasFields(fieldNames ...F) bool {
	for _, fieldName := range fieldNames {
		if _, ok := m[fieldName]; !ok {
			return false
		}
	}
	return true
}

// convertFieldMap converts a typed fieldMap to a string-keyed fieldMap for compatibility with generic functions
func convertFieldMap[F ~string](fields fieldMap[F]) fieldMap[string] {
	result := fieldMap[string]{}
	for k, v := range fields {
		result[string(k)] = v
	}
	return result
}

var tables = map[string]tableDefinition{
	"relationships": {
		name:                  "relationships",
		schema:                relationshipsSchema,
		insertRunner:          relationshipsInsertRunner,
		insertValidator:       relationshipInsertValidator,
		deleteRunner:          relationshipsDeleteRunner,
		buildSelectHandler:    buildRelationshipsSelectHandler,
		buildExplainHandler:   buildRelationshipsExplainHandler,
		requiredInsertColumns: relationshipsRequiredInsertColumns,
	},
	"permissions": {
		name:                "permissions",
		schema:              permissionsSchema,
		buildSelectHandler:  buildPermissionsSelectHandler,
		buildExplainHandler: buildPermissionsExplainHandler,
	},
	"schema": {
		name:                "schema",
		schema:              schemaSchema,
		insertRunner:        schemaInsertRunner,
		buildSelectHandler:  buildSchemaSelectHandler,
		buildExplainHandler: buildSchemaExplainHandler,
	},
}
