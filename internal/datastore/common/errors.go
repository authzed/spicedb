package common

import (
	"context"

	"github.com/rs/zerolog/log"
)

// LogOnError executes the function and logs the error.
// Useful to avoid silently ignoring errors in defer statements
func LogOnError(ctx context.Context, f func() error) {
	if err := f(); err != nil {
		log.Ctx(ctx).Error().Err(err)
	}
}
