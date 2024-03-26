package graph

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	log "github.com/authzed/spicedb/internal/logging"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/middleware/requestid"
)

// branchContext returns a context disconnected from the parent context, but populated with the datastore.
// Also returns a function for canceling the newly created context, without canceling the parent context.
func branchContext(ctx context.Context) (context.Context, func(cancelErr error)) {
	// Add tracing to the context.
	span := trace.SpanFromContext(ctx)
	detachedContext := trace.ContextWithSpan(context.Background(), span)

	// Add datastore to the context.
	ds := datastoremw.FromContext(ctx)
	detachedContext = datastoremw.ContextWithDatastore(detachedContext, ds)

	// Add logging to the context.
	loggerFromContext := log.Ctx(ctx)
	if loggerFromContext != nil {
		detachedContext = loggerFromContext.WithContext(detachedContext)
	}

	detachedContext = requestid.PropagateIfExists(ctx, detachedContext)

	return context.WithCancelCause(detachedContext)
}
