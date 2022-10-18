package common

import (
	"context"

	log "github.com/authzed/spicedb/internal/logging"
)

// LogOnError executes the function and logs the error.
// Useful to avoid silently ignoring errors in defer statements
func LogOnError(ctx context.Context, f func() error) {
	if err := f(); err != nil {
		log.Ctx(ctx).Err(err).Msg("datastore error")
	}
}
