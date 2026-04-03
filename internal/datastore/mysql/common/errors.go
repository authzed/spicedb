package common

import (
	"errors"

	"github.com/go-sql-driver/mysql"

	dscommon "github.com/authzed/spicedb/internal/datastore/common"
)

const (
	// mysqlMissingTableErrorNumber is the MySQL error number for "table doesn't exist".
	// This corresponds to MySQL error 1146 (ER_NO_SUCH_TABLE) with SQLSTATE 42S02.
	mysqlMissingTableErrorNumber = 1146
)

// IsMissingTableError returns true if the error is a MySQL error indicating a missing table.
// This typically happens when migrations have not been run.
func IsMissingTableError(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlMissingTableErrorNumber
}

// WrapMissingTableError checks if the error is a missing table error and wraps it with
// a helpful message instructing the user to run migrations. If it's not a missing table error,
// it returns nil. If it's already a SchemaNotInitializedError, it returns the original error
// to preserve the wrapped error through the call chain.
func WrapMissingTableError(err error) error {
	// Don't double-wrap if already a SchemaNotInitializedError - return original to preserve it
	var schemaErr dscommon.SchemaNotInitializedError
	if errors.As(err, &schemaErr) {
		return err
	}
	if IsMissingTableError(err) {
		return dscommon.NewSchemaNotInitializedError(err)
	}
	return nil
}
