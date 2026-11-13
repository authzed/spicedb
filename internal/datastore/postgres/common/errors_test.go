package common

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
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
