package graph

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/middleware/requestid"
)

// branchContext returns a context disconnected from the parent context, but populated with the datastore.
// Also returns a function for canceling the newly created context, without canceling the parent context.
func branchContext(ctx context.Context) (context.Context, func(cancelErr error)) {
	// Add tracing to the context.
	span := trace.SpanFromContext(ctx)
	detachedContext := trace.ContextWithSpan(context.Background(), span)

	// Add data layer to the context.
	dl := datalayer.FromContext(ctx)
	detachedContext = datalayer.ContextWithDataLayer(detachedContext, dl)

	// Add logging to the context.
	loggerFromContext := log.Ctx(ctx)
	if loggerFromContext != nil {
		detachedContext = loggerFromContext.WithContext(detachedContext)
	}

	detachedContext = requestid.PropagateIfExists(ctx, detachedContext)

	return context.WithCancelCause(detachedContext)
}
