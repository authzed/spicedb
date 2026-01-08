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

// RowsCursor is a cursor for iterating over query results.
type RowsCursor any

// SelectStatement represents a parsed SELECT query for SpiceDB tables.
// It handles querying permissions, relationships, and schema data.
type SelectStatement struct {
	isUnsupported bool

	tableName      string
	tableDef       tableDefinition
	columnNames    []string
	fields         map[string]valueOrRef
	selectHandler  selectHandler
	explainHandler explainHandler
}

// ParseSelectStatement parses a Postgres SELECT statement into a SelectStatement.
// The forExplain parameter indicates if this is being parsed for an EXPLAIN query.
// Returns an error if the query is not supported by the FDW.
func ParseSelectStatement(query *pg_query.SelectStmt, forExplain bool) (*SelectStatement, error) {
	if query == nil {
		return nil, common.NewUnsupportedError(errors.New("only SELECT is supported"))
	}

	// FROM
	fromClauses := query.GetFromClause()
	if len(fromClauses) != 1 {
		return nil, common.NewUnsupportedError(errors.New("a select statement must have exactly one FROM clause"))
	}

	fromVar := fromClauses[0].GetRangeVar()
	if fromVar == nil {
		return nil, common.NewUnsupportedError(errors.New("a select statement must have a FROM clause pointing to a single table"))
	}

	tableName := fromVar.GetRelname()
	tableDef, ok := tables[tableName]
	if !ok {
		return nil, common.NewUnsupportedError(fmt.Errorf("table %s does not exist", tableName))
	}

	// Check for * or column names.
	columnNames := make([]string, 0)
	if len(query.GetTargetList()) == 1 {
		target := query.GetTargetList()[0].GetResTarget()
		if target != nil && target.GetVal() != nil && target.GetVal().GetColumnRef() != nil {
			fields := target.GetVal().GetColumnRef().GetFields()
			if len(fields) == 1 && fields[0].GetAStar() != nil {
				// SELECT *
				columnNames = tableDef.columnNames()
			}
		}
	}

	if len(columnNames) == 0 {
		for _, target := range query.GetTargetList() {
			if target.GetResTarget() == nil {
				return nil, common.NewUnsupportedError(errors.New("a select statement must have a target list"))
			}

			if target.GetResTarget().GetVal() == nil {
				return nil, common.NewUnsupportedError(errors.New("a select statement must have a target list with a value"))
			}

			if target.GetResTarget().GetVal().GetColumnRef() == nil {
				return nil, common.NewUnsupportedError(errors.New("a select statement must have a target list with a column reference"))
			}

			if target.GetResTarget().GetVal().GetColumnRef().GetFields() == nil {
				return nil, common.NewUnsupportedError(errors.New("a select statement must have a target list with a column reference"))
			}

			if len(target.GetResTarget().GetVal().GetColumnRef().GetFields()) != 1 {
				return nil, common.NewUnsupportedError(errors.New("a select statement must have a target list with a single column reference"))
			}

			if target.GetResTarget().GetVal().GetColumnRef().GetFields()[0].GetString_() == nil {
				return nil, common.NewUnsupportedError(errors.New("a select statement must have a target list with a column reference"))
			}

			if target.GetResTarget().GetVal().GetColumnRef().GetFields()[0].GetString_().Sval == "" {
				return nil, common.NewUnsupportedError(errors.New("a select statement must have a target list with a column reference"))
			}

			columnName := target.GetResTarget().GetVal().GetColumnRef().GetFields()[0].GetString_().Sval
			if !tableDef.hasColumn(columnName) {
				return &SelectStatement{
					isUnsupported: true,
					tableName:     tableName,
					tableDef:      tableDef,
				}, common.NewUnsupportedError(fmt.Errorf("column %s does not exist in table %s", columnName, tableName))
			}

			columnNames = append(columnNames, columnName)
		}
	}

	// Ensure unused clauses are not present.
	if err := checkUnusedSelectClauses(query); err != nil {
		return &SelectStatement{
			isUnsupported: true,
			tableName:     tableName,
			tableDef:      tableDef,
		}, common.NewUnsupportedError(fmt.Errorf("unsupported select statement: %w", err))
	}

	pm := NewPatternMatcher()
	fields, err := pm.MatchWhereClause(query.GetWhereClause())
	if err != nil {
		return &SelectStatement{
			isUnsupported: true,
			tableName:     tableName,
			tableDef:      tableDef,
		}, common.NewUnsupportedError(fmt.Errorf("unsupported select statement: %w", err))
	}

	// Ensure all fields match the table definition.
	for fieldName := range fields {
		if !tableDef.hasColumn(fieldName) {
			return &SelectStatement{
				isUnsupported: true,
				tableName:     tableName,
				tableDef:      tableDef,
			}, common.NewUnsupportedError(fmt.Errorf("field %s does not exist in table %s", fieldName, tableName))
		}
	}

	if forExplain {
		buildExplainHandler := tableDef.buildExplainHandler
		if buildExplainHandler == nil {
			return nil, common.NewUnsupportedError(fmt.Errorf("table %s does not support EXPLAIN", tableName))
		}

		explainHandler, err := buildExplainHandler(fields)
		if err != nil {
			return &SelectStatement{
				isUnsupported: true,
				tableName:     tableName,
				tableDef:      tableDef,
			}, common.NewUnsupportedError(fmt.Errorf("unsupported select statement: %w", err))
		}

		return &SelectStatement{
			tableName:      tableName,
			tableDef:       tableDef,
			fields:         fields,
			explainHandler: explainHandler,
		}, nil
	} else {
		selectHandler, err := tableDef.buildSelectHandler(fields)
		if err != nil {
			return &SelectStatement{
				isUnsupported: true,
				tableName:     tableName,
				tableDef:      tableDef,
			}, common.NewUnsupportedError(fmt.Errorf("unsupported select statement: %w", err))
		}

		return &SelectStatement{
			tableName:     tableName,
			tableDef:      tableDef,
			fields:        fields,
			selectHandler: selectHandler,
			columnNames:   columnNames,
		}, nil
	}
}

func (ss *SelectStatement) IsUnsupported() bool {
	return ss.isUnsupported
}

func (ss *SelectStatement) Schema() wire.Columns {
	columns := make(wire.Columns, 0, len(ss.columnNames))
	for _, columnName := range ss.columnNames {
		schemaColumn, _ := ss.tableDef.getSchemaColumn(columnName)
		columns = append(columns, schemaColumn)
	}

	return columns
}

func (ss *SelectStatement) TableName() string {
	return ss.tableName
}

func (ss *SelectStatement) Fetch(ctx context.Context, client *authzed.Client, parameters []wire.Parameter, writer wire.DataWriter, howMany int64, rowsCursor RowsCursor, selectStmt *SelectStatement) (RowsCursor, error) {
	if ss.isUnsupported {
		return nil, common.NewUnsupportedError(errors.New("this query is unsupported"))
	}

	return ss.selectHandler(ctx, client, parameters, howMany, writer, rowsCursor, selectStmt)
}

func (ss *SelectStatement) Explain(ctx context.Context, client *authzed.Client, writer wire.DataWriter) error {
	if ss.isUnsupported {
		return common.NewUnsupportedError(errors.New("this query is unsupported"))
	}

	return ss.explainHandler(ctx, client, writer)
}

func checkUnusedSelectClauses(selectStatement *pg_query.SelectStmt) error {
	if selectStatement.GetDistinctClause() != nil {
		return errors.New("DISTINCT not supported")
	}

	if selectStatement.GetGroupClause() != nil {
		return errors.New("GROUP BY not supported")
	}

	if selectStatement.GetGroupDistinct() {
		return errors.New("GROUP BY not supported")
	}

	if selectStatement.GetHavingClause() != nil {
		return errors.New("HAVING not supported")
	}

	if selectStatement.GetIntoClause() != nil {
		return errors.New("INTO not supported")
	}

	if selectStatement.GetSortClause() != nil {
		return errors.New("ORDER BY not supported")
	}

	if selectStatement.GetLockingClause() != nil {
		return errors.New("LOCK not supported")
	}

	if selectStatement.GetWindowClause() != nil {
		return errors.New("WINDOW not supported")
	}

	if selectStatement.GetLimitCount() != nil {
		return errors.New("LIMIT not supported")
	}

	return nil
}

type valueOrRef struct {
	fieldName             string
	value                 string
	parameterIndex        int32
	isSubQueryPlaceholder bool
}
