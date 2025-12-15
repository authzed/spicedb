package common

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestSchemaNotInitializedError(t *testing.T) {
	underlyingErr := fmt.Errorf("relation \"caveat\" does not exist (SQLSTATE 42P01)")
	err := NewSchemaNotInitializedError(underlyingErr)

	t.Run("error message contains migration instructions", func(t *testing.T) {
		require.Contains(t, err.Error(), "spicedb datastore migrate")
		require.Contains(t, err.Error(), "database schema has not been initialized")
	})

	t.Run("unwrap returns underlying error", func(t *testing.T) {
		var schemaErr SchemaNotInitializedError
		require.ErrorAs(t, err, &schemaErr)
		require.ErrorIs(t, schemaErr.Unwrap(), underlyingErr)
	})

	t.Run("grpc status is FailedPrecondition", func(t *testing.T) {
		var schemaErr SchemaNotInitializedError
		require.ErrorAs(t, err, &schemaErr)
		status := schemaErr.GRPCStatus()
		require.Equal(t, codes.FailedPrecondition, status.Code())
	})

	t.Run("can be detected with errors.As", func(t *testing.T) {
		var schemaErr SchemaNotInitializedError
		require.ErrorAs(t, err, &schemaErr)
	})

	t.Run("wrapped error preserves chain", func(t *testing.T) {
		wrappedErr := fmt.Errorf("outer: %w", err)
		var schemaErr SchemaNotInitializedError
		require.ErrorAs(t, wrappedErr, &schemaErr)
	})
}
