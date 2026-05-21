package common

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ccoveille/go-safecast/v2"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// InsertNewTransaction inserts a fresh row into the transaction table and
// returns its auto-generated id. When metadata is nil the row is inserted
// with column defaults only (useful in migrations that predate the metadata
// column or do not need to record metadata).
func InsertNewTransaction(ctx context.Context, tx *sql.Tx, txnTable string, metadata driver.Valuer) (uint64, error) {
	var (
		result sql.Result
		err    error
	)
	if metadata == nil {
		//nolint:gosec // table name is from internal schema configuration
		result, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s () VALUES ()", txnTable))
	} else {
		//nolint:gosec // table name is from internal schema configuration
		result, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (metadata) VALUES (?)", txnTable), metadata)
	}
	if err != nil {
		return 0, fmt.Errorf("insert into transaction table: %w", err)
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get last inserted transaction id: %w", err)
	}

	txnID, err := safecast.Convert[uint64](lastInsertID)
	if err != nil {
		return 0, spiceerrors.MustBugf("last inserted transaction id was negative: %v", err)
	}
	return txnID, nil
}
