package graph

import (
	"context"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
)

// branchContext returns a context disconnected from the parent context, but populated with the datastore.
// Also returns a function for canceling the newly context, without canceling the parent context.
// This is used when cancelation of a child context should not propagate upwards.
func branchContext(ctx context.Context) (context.Context, func(cancelErr error)) {
	ds := datastoremw.FromContext(ctx)
	newContextForReachable := datastoremw.ContextWithDatastore(context.Background(), ds)
	return context.WithCancelCause(newContextForReachable)
}
