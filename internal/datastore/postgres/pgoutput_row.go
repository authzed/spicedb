package postgres

import (
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// pgoutput row decoding for the commit LSN ledger, the one remaining consumer
// of logical replication. The ledger only ever decodes the transaction table's
// xid column, but decoding is kept whole-row and codec-driven so that it reads
// values exactly the way pgx reads them over SQL.

// logicalColumnValue is one column of a decoded row in `pgoutput` text format.
type logicalColumnValue struct {
	// isNull indicates a SQL NULL.
	isNull bool
	// missing indicates an unchanged TOASTed value for which no old tuple
	// value was available; the actual value is unknown.
	missing bool
	// dataType is the PostgreSQL type OID of the column, from the RELATION message.
	dataType uint32
	text     string
}

// logicalRow is a decoded row keyed by column name, merged from the new tuple
// and (for unchanged TOASTed values) the old tuple.
type logicalRow struct {
	tableName string
	values    map[string]logicalColumnValue
	typeMap   *pgtype.Map
}

func decodeLogicalRow(typeMap *pgtype.Map, relation *pglogrepl.RelationMessage, tupleData *pglogrepl.TupleData, oldTupleData *pglogrepl.TupleData) (*logicalRow, error) {
	if tupleData == nil {
		return nil, fmt.Errorf("missing tuple data for table %s", relation.RelationName)
	}

	if len(tupleData.Columns) != len(relation.Columns) {
		return nil, fmt.Errorf("tuple data for table %s has %d columns, expected %d", relation.RelationName, len(tupleData.Columns), len(relation.Columns))
	}

	row := &logicalRow{
		tableName: relation.RelationName,
		values:    make(map[string]logicalColumnValue, len(relation.Columns)),
		typeMap:   typeMap,
	}

	for index, column := range relation.Columns {
		value, err := decodeLogicalColumn(column, tupleData.Columns[index])
		if err != nil {
			return nil, fmt.Errorf("column %s of table %s: %w", column.Name, relation.RelationName, err)
		}

		if value.missing && oldTupleData != nil && index < len(oldTupleData.Columns) {
			// An unchanged TOASTed value is not sent in the new tuple; with a
			// full replica identity the old tuple carries it instead.
			oldValue, err := decodeLogicalColumn(column, oldTupleData.Columns[index])
			if err != nil {
				return nil, fmt.Errorf("column %s of table %s: %w", column.Name, relation.RelationName, err)
			}
			if !oldValue.missing {
				value = oldValue
			}
		}

		row.values[column.Name] = value
	}

	return row, nil
}

func decodeLogicalColumn(column *pglogrepl.RelationMessageColumn, tupleColumn *pglogrepl.TupleDataColumn) (logicalColumnValue, error) {
	switch tupleColumn.DataType {
	case pglogrepl.TupleDataTypeText:
		return logicalColumnValue{dataType: column.DataType, text: string(tupleColumn.Data)}, nil
	case pglogrepl.TupleDataTypeNull:
		return logicalColumnValue{dataType: column.DataType, isNull: true}, nil
	case pglogrepl.TupleDataTypeToast:
		return logicalColumnValue{dataType: column.DataType, missing: true}, nil
	case pglogrepl.TupleDataTypeBinary:
		return logicalColumnValue{}, errors.New("unexpected binary-format tuple data")
	default:
		return logicalColumnValue{}, fmt.Errorf("unknown tuple data type %q", tupleColumn.DataType)
	}
}

func (r *logicalRow) column(name string) (logicalColumnValue, error) {
	value, ok := r.values[name]
	if !ok {
		return logicalColumnValue{}, fmt.Errorf("column %s not found in replicated row for table %s", name, r.tableName)
	}
	if value.missing {
		return logicalColumnValue{}, fmt.Errorf("column %s of table %s has an unchanged TOASTed value and no old tuple was available", name, r.tableName)
	}
	return value, nil
}

// scanColumn decodes the named column's pgoutput text value into dst using the
// pgtype codec registered for the column's type OID.
func (r *logicalRow) scanColumn(name string, dst any) error {
	value, err := r.column(name)
	if err != nil {
		return err
	}
	if value.isNull {
		return fmt.Errorf("column %s of table %s is unexpectedly NULL", name, r.tableName)
	}
	if err := r.typeMap.Scan(value.dataType, pgtype.TextFormatCode, []byte(value.text), dst); err != nil {
		return fmt.Errorf("unable to decode column %s of table %s: %w", name, r.tableName, err)
	}
	return nil
}

func (r *logicalRow) xid8Column(name string) (xid8, error) {
	var xid xid8
	if err := r.scanColumn(name, &xid); err != nil {
		return xid8{}, err
	}
	return xid, nil
}
