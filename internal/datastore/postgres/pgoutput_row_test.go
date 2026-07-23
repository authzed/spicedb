//go:build datastore && postgres

package postgres

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const (
	// testLiveSentinel is the text form of liveDeletedTxnID as it appears in
	// replicated tuple data.
	testLiveSentinel = "9223372036854775807"
)

// column value markers for buildTuple
type (
	nullValue  struct{}
	toastValue struct{}
)

// testColumn is a column definition for testRelation: a name plus the
// PostgreSQL type OID that a real RELATION message would carry.
type testColumn struct {
	name string
	oid  uint32
}

// TestPGOutputColumnDecoding asserts that pgoutput text column values are
// decoded through the pgtype codecs registered for the column type OIDs carried
// in the RELATION message: the same decode path pgx uses when a row is scanned
// over SQL. The commit LSN ledger only reads the transaction table's xid, but it
// reads it out of the WAL rather than out of a result set, so the equivalence is
// what makes the two agree.
func TestPGOutputColumnDecoding(t *testing.T) {
	transactionRelation := testTransactionRelation()

	testCases := []struct {
		name        string
		relation    *pglogrepl.RelationMessage
		tuple       *pglogrepl.TupleData
		assert      func(t *testing.T, row *logicalRow)
		errContains string
	}{
		{
			name:     "xid8 decodes to the transaction ID",
			relation: transactionRelation,
			tuple:    transactionRowTuple("4294967297"),
			assert: func(t *testing.T, row *logicalRow) {
				xid, err := row.xid8Column("xid")
				require.NoError(t, err)
				require.Equal(t, uint64(4294967297), xid.Uint64, "an epoch-extended xid8 must survive decoding")
			},
		},
		{
			name:     "a snapshot decodes through its registered codec",
			relation: transactionRelation,
			tuple:    transactionRowTuple("900"),
			assert: func(t *testing.T, row *logicalRow) {
				var snapshot pgSnapshot
				require.NoError(t, row.scanColumn("snapshot", &snapshot))
				require.Equal(t, uint64(900), snapshot.xmin)
			},
		},
		{
			name:     "a timestamp decodes to the instant it names",
			relation: transactionRelation,
			tuple:    transactionRowTuple("900"),
			assert: func(t *testing.T, row *logicalRow) {
				var timestamp time.Time
				require.NoError(t, row.scanColumn("timestamp", &timestamp))
				require.Equal(t, "2026-07-22 10:11:12.123456 +0000 UTC", timestamp.UTC().String())
			},
		},
		{
			name:     "a NULL column is reported rather than decoded",
			relation: transactionRelation,
			tuple:    buildTuple("1", "900", "2026-07-22 10:11:12.123456", nullValue{}, "900:900:"),
			assert: func(t *testing.T, row *logicalRow) {
				var metadata map[string]any
				require.ErrorContains(t, row.scanColumn("metadata", &metadata), "unexpectedly NULL")
			},
		},
		{
			name:     "an unresolved TOASTed value is an error, not a silent zero",
			relation: transactionRelation,
			tuple:    buildTuple("1", toastValue{}, "2026-07-22 10:11:12.123456", `{}`, "900:900:"),
			assert: func(t *testing.T, row *logicalRow) {
				_, err := row.xid8Column("xid")
				require.ErrorContains(t, err, "unchanged TOASTed value")
			},
		},
		{
			name:     "a column the row does not have is an error",
			relation: transactionRelation,
			tuple:    transactionRowTuple("900"),
			assert: func(t *testing.T, row *logicalRow) {
				_, err := row.xid8Column("nonexistent")
				require.ErrorContains(t, err, "not found")
			},
		},
		{
			name:        "a tuple whose width disagrees with the relation is rejected",
			relation:    transactionRelation,
			tuple:       buildTuple("1", "900"),
			errContains: "expected 5",
		},
		{
			name:        "missing tuple data is rejected",
			relation:    transactionRelation,
			errContains: "missing tuple data",
		},
	}

	typeMap := pgtype.NewMap()
	RegisterTypes(typeMap)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			row, err := decodeLogicalRow(typeMap, tc.relation, tc.tuple, nil)
			if tc.errContains != "" {
				require.ErrorContains(t, err, tc.errContains)
				return
			}
			require.NoError(t, err)
			tc.assert(t, row)
		})
	}

	t.Run("binary-format tuple data is rejected", func(t *testing.T) {
		_, err := decodeLogicalColumn(
			&pglogrepl.RelationMessageColumn{Name: "xid", DataType: xid8TypeOID},
			&pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeBinary},
		)
		require.ErrorContains(t, err, "binary-format")
	})

	t.Run("an unchanged TOASTed value falls back to the old tuple", func(t *testing.T) {
		row, err := decodeLogicalRow(
			typeMap, transactionRelation,
			buildTuple("1", toastValue{}, "2026-07-22 10:11:12.123456", `{}`, "900:900:"),
			transactionRowTuple("900"),
		)
		require.NoError(t, err)

		xid, err := row.xid8Column("xid")
		require.NoError(t, err)
		require.Equal(t, uint64(900), xid.Uint64)
	})
}

func testTransactionRelation() *pglogrepl.RelationMessage {
	return testRelation(
		1, "relation_tuple_transaction",
		testColumn{"id", pgtype.Int8OID},
		testColumn{"xid", xid8TypeOID},
		testColumn{"timestamp", pgtype.TimestampOID},
		testColumn{"metadata", pgtype.JSONBOID},
		testColumn{"snapshot", pgSnapshotTypeOID},
	)
}

func testTupleRelation() *pglogrepl.RelationMessage {
	return testRelation(
		2, "relation_tuple",
		testColumn{"namespace", pgtype.VarcharOID},
		testColumn{"object_id", pgtype.VarcharOID},
		testColumn{"relation", pgtype.VarcharOID},
		testColumn{"userset_namespace", pgtype.VarcharOID},
		testColumn{"userset_object_id", pgtype.VarcharOID},
		testColumn{"userset_relation", pgtype.VarcharOID},
		testColumn{"caveat_name", pgtype.VarcharOID},
		testColumn{"caveat_context", pgtype.JSONBOID},
		testColumn{"expiration", pgtype.TimestamptzOID},
		testColumn{"created_xid", xid8TypeOID},
		testColumn{"deleted_xid", xid8TypeOID},
	)
}

func testRelation(id uint32, name string, columns ...testColumn) *pglogrepl.RelationMessage {
	messageColumns := make([]*pglogrepl.RelationMessageColumn, 0, len(columns))
	for _, column := range columns {
		messageColumns = append(messageColumns, &pglogrepl.RelationMessageColumn{Name: column.name, DataType: column.oid})
	}

	return &pglogrepl.RelationMessage{
		RelationID:   id,
		Namespace:    "public",
		RelationName: name,
		ColumnNum:    spiceerrors.MustSafecast[uint16](len(messageColumns)),
		Columns:      messageColumns,
	}
}

func buildTuple(values ...any) *pglogrepl.TupleData {
	columns := make([]*pglogrepl.TupleDataColumn, 0, len(values))
	for _, value := range values {
		switch v := value.(type) {
		case nullValue:
			columns = append(columns, &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull})
		case toastValue:
			columns = append(columns, &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeToast})
		case string:
			columns = append(columns, &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(v)})
		default:
			panic("unsupported test tuple value")
		}
	}

	return &pglogrepl.TupleData{
		ColumnNum: spiceerrors.MustSafecast[uint16](len(columns)),
		Columns:   columns,
	}
}

func transactionRowTuple(xid string) *pglogrepl.TupleData {
	return buildTuple("1", xid, "2026-07-22 10:11:12.123456", `{"purpose":"testing"}`, xid+":"+xid+":")
}

func relationshipTuple(subjectObjectID string, caveatContext any, deletedXid string) *pglogrepl.TupleData {
	return buildTuple(
		"document", "doc1", "viewer", "user", subjectObjectID, "...",
		"somecaveat", caveatContext, nullValue{}, "900", deletedXid,
	)
}
