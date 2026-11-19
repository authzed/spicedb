package spanner

import (
	"strings"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

// IsMissingTableError returns true if the error is a Spanner error indicating a missing table.
// This typically happens when migrations have not been run.
func IsMissingTableError(err error) bool {
	if spanner.ErrCode(err) == codes.NotFound {
		errMsg := err.Error()
		if strings.Contains(errMsg, "Table not found") || strings.Contains(errMsg, "table not found") {
			return true
		}
	}
	return false
}
