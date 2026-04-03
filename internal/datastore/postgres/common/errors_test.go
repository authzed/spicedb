package common

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	dscommon "github.com/authzed/spicedb/internal/datastore/common"
)

func TestIsMissingTableError(t *testing.T) {
	t.Run("returns true for missing table error", func(t *testing.T) {
		pgErr := &pgconn.PgError{
			Code:    PgMissingTable,
			Message: "relation \"caveat\" does not exist",
		}
		require.True(t, IsMissingTableError(pgErr))
	})

	t.Run("returns false for other postgres errors", func(t *testing.T) {
		pgErr := &pgconn.PgError{
			Code:    pgSerializationFailure,
			Message: "could not serialize access",
		}
		require.False(t, IsMissingTableError(pgErr))
	})

	t.Run("returns false for non-postgres errors", func(t *testing.T) {
		err := fmt.Errorf("some other error")
		require.False(t, IsMissingTableError(err))
	})

	t.Run("returns false for nil error", func(t *testing.T) {
		require.False(t, IsMissingTableError(nil))
	})

	t.Run("returns true for wrapped missing table error", func(t *testing.T) {
		pgErr := &pgconn.PgError{
			Code:    PgMissingTable,
			Message: "relation \"caveat\" does not exist",
		}
		wrappedErr := fmt.Errorf("query failed: %w", pgErr)
		require.True(t, IsMissingTableError(wrappedErr))
	})
}

func TestWrapMissingTableError(t *testing.T) {
	t.Run("wraps missing table error", func(t *testing.T) {
		pgErr := &pgconn.PgError{
			Code:    PgMissingTable,
			Message: "relation \"caveat\" does not exist",
		}
		wrapped := WrapMissingTableError(pgErr)
		require.Error(t, wrapped)

		var schemaErr dscommon.SchemaNotInitializedError
		require.ErrorAs(t, wrapped, &schemaErr)
		require.Contains(t, wrapped.Error(), "spicedb datastore migrate")
	})

	t.Run("returns nil for non-missing-table errors", func(t *testing.T) {
		pgErr := &pgconn.PgError{
			Code:    pgSerializationFailure,
			Message: "could not serialize access",
		}
		require.NoError(t, WrapMissingTableError(pgErr))
	})

	t.Run("returns nil for non-postgres errors", func(t *testing.T) {
		err := fmt.Errorf("some other error")
		require.NoError(t, WrapMissingTableError(err))
	})

	t.Run("returns nil for nil error", func(t *testing.T) {
		require.NoError(t, WrapMissingTableError(nil))
	})

	t.Run("preserves original error in chain", func(t *testing.T) {
		pgErr := &pgconn.PgError{
			Code:    PgMissingTable,
			Message: "relation \"caveat\" does not exist",
		}
		wrapped := WrapMissingTableError(pgErr)
		require.Error(t, wrapped)

		// The original postgres error should be accessible via unwrapping
		var foundPgErr *pgconn.PgError
		require.ErrorAs(t, wrapped, &foundPgErr)
		require.Equal(t, PgMissingTable, foundPgErr.Code)
	})

	t.Run("does not double-wrap already wrapped errors", func(t *testing.T) {
		pgErr := &pgconn.PgError{
			Code:    PgMissingTable,
			Message: "relation \"caveat\" does not exist",
		}
		// First wrap
		wrapped := WrapMissingTableError(pgErr)
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
