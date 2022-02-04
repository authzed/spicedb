package common

import (
	"context"

	"github.com/rs/zerolog/log"
)

var (
	ErrUnableToQueryTuples  = "unable to query tuples: %w"
	ErrUnableToWriteTuples  = "unable to write tuples: %w"
	ErrUnableToDeleteTuples = "unable to delete tuples: %w"

	ErrUnableToWriteConfig    = "unable to write namespace config: %w"
	ErrUnableToReadConfig     = "unable to read namespace config: %w"
	ErrUnableToDeleteConfig   = "unable to delete namespace config: %w"
	ErrUnableToListNamespaces = "unable to list namespaces: %w"
)

// LogOnError executes the function and logs the error.
// Useful to avoid silently ignoring errors in defer statements
func LogOnError(ctx context.Context, f func() error) {
	if err := f(); err != nil {
		log.Ctx(ctx).Error().Err(err)
	}
}
