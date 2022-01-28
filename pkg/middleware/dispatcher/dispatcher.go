package dispatcher

import (
	"context"

	"github.com/authzed/spicedb/internal/dispatch"
	dispatchermw "github.com/authzed/spicedb/internal/middleware/dispatcher"
)

// FromContext reads the selected dispatcher out of a context.Context
// and returns nil if it does not exist.
func FromContext(ctx context.Context) dispatch.Dispatcher {
	return dispatchermw.FromContext(ctx)
}

// MustFromContext reads the selected dispatcher out of a context.Context, computes a zedtoken
// from it, and panics if it has not been set on the context.
func MustFromContext(ctx context.Context) dispatch.Dispatcher {
	dispatcher := dispatchermw.FromContext(ctx)
	if dispatcher == nil {
		panic("dispatcher middleware did not inject dispatcher")
	}

	return dispatcher
}
