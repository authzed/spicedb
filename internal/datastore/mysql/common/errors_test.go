package common

import (
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	dscommon "github.com/authzed/spicedb/internal/datastore/common"
)

func TestIsMissingTableError(t *testing.T) {
	t.Run("returns true for missing table error", func(t *testing.T) {
		mysqlErr := &mysql.MySQLError{
			Number:  mysqlMissingTableErrorNumber,
			Message: "Table 'spicedb.caveat' doesn't exist",
		}
		require.True(t, IsMissingTableError(mysqlErr))
	})

	t.Run("returns false for other mysql errors", func(t *testing.T) {
		mysqlErr := &mysql.MySQLError{
			Number:  1062, // Duplicate entry error
			Message: "Duplicate entry '1' for key 'PRIMARY'",
		}
		require.False(t, IsMissingTableError(mysqlErr))
	})

	t.Run("returns false for non-mysql errors", func(t *testing.T) {
		err := fmt.Errorf("some other error")
		require.False(t, IsMissingTableError(err))
	})

	t.Run("returns false for nil error", func(t *testing.T) {
		require.False(t, IsMissingTableError(nil))
	})

	t.Run("returns true for wrapped missing table error", func(t *testing.T) {
		mysqlErr := &mysql.MySQLError{
			Number:  mysqlMissingTableErrorNumber,
			Message: "Table 'spicedb.caveat' doesn't exist",
		}
		wrappedErr := fmt.Errorf("query failed: %w", mysqlErr)
		require.True(t, IsMissingTableError(wrappedErr))
	})
}

func TestWrapMissingTableError(t *testing.T) {
	t.Run("wraps missing table error", func(t *testing.T) {
		mysqlErr := &mysql.MySQLError{
			Number:  mysqlMissingTableErrorNumber,
			Message: "Table 'spicedb.caveat' doesn't exist",
		}
		wrapped := WrapMissingTableError(mysqlErr)
		require.Error(t, wrapped)

		var schemaErr dscommon.SchemaNotInitializedError
		require.ErrorAs(t, wrapped, &schemaErr)
		require.Contains(t, wrapped.Error(), "spicedb datastore migrate")
	})

	t.Run("returns nil for non-missing-table errors", func(t *testing.T) {
		mysqlErr := &mysql.MySQLError{
			Number:  1062, // Duplicate entry error
			Message: "Duplicate entry '1' for key 'PRIMARY'",
		}
		require.NoError(t, WrapMissingTableError(mysqlErr))
	})

	t.Run("returns nil for non-mysql errors", func(t *testing.T) {
		err := fmt.Errorf("some other error")
		require.NoError(t, WrapMissingTableError(err))
	})

	t.Run("returns nil for nil error", func(t *testing.T) {
		require.NoError(t, WrapMissingTableError(nil))
	})

	t.Run("preserves original error in chain", func(t *testing.T) {
		mysqlErr := &mysql.MySQLError{
			Number:  mysqlMissingTableErrorNumber,
			Message: "Table 'spicedb.caveat' doesn't exist",
		}
		wrapped := WrapMissingTableError(mysqlErr)
		require.Error(t, wrapped)

		// The original mysql error should be accessible via unwrapping
		var foundMySQLErr *mysql.MySQLError
		require.ErrorAs(t, wrapped, &foundMySQLErr)
		require.Equal(t, uint16(mysqlMissingTableErrorNumber), foundMySQLErr.Number)
	})

	t.Run("does not double-wrap already wrapped errors", func(t *testing.T) {
		mysqlErr := &mysql.MySQLError{
			Number:  mysqlMissingTableErrorNumber,
			Message: "Table 'spicedb.caveat' doesn't exist",
		}
		// First wrap
		wrapped := WrapMissingTableError(mysqlErr)
		require.Error(t, wrapped)

		// Second wrap should return the already-wrapped error (preserving it through call chain)
		doubleWrapped := WrapMissingTableError(wrapped)
		require.Error(t, doubleWrapped)
		require.Equal(t, wrapped, doubleWrapped)

		// Should still be detectable as SchemaNotInitializedError
		var schemaErr dscommon.SchemaNotInitializedError
		require.ErrorAs(t, doubleWrapped, &schemaErr)
	})
}
