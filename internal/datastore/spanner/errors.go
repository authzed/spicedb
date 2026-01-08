package spanner

import (
	"errors"
	"strings"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/datastore/common"
)

// IsMissingTableError returns true if the error is a Spanner error indicating a missing table.
// This typically happens when migrations have not been run.
func IsMissingTableError(err error) bool {
	if spanner.ErrCode(err) == codes.NotFound {
		// Check if it's specifically about a missing table
		errMsg := err.Error()
		if strings.Contains(errMsg, "Table not found") {
			return true
		}
	}
	return false
}

// WrapMissingTableError checks if the error is a missing table error and wraps it with
// a helpful message instructing the user to run migrations. If it's not a missing table error,
// it returns nil. If it's already a SchemaNotInitializedError, it returns the original error
// to preserve the wrapped error through the call chain.
func WrapMissingTableError(err error) error {
	// Don't double-wrap if already a SchemaNotInitializedError - return original to preserve it
	var schemaErr common.SchemaNotInitializedError
	if errors.As(err, &schemaErr) {
		return err
	}
	if IsMissingTableError(err) {
		return common.NewSchemaNotInitializedError(err)
	}
	return nil
}
