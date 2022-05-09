package datastore

import (
	"context"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/trace"
)

// SeparateContextWithTracing is a utility method which allows for severing the context between
// grpc and the datastore to prevent context cancellation from killing database connections that
// should otherwise go back to the connection pool.
func SeparateContextWithTracing(ctx context.Context) context.Context {
	span := trace.SpanFromContext(ctx)
	ctxWithObservability := trace.ContextWithSpan(context.Background(), span)

	loggerFromContext := log.Ctx(ctx)
	if loggerFromContext != nil {
		ctxWithObservability = loggerFromContext.WithContext(ctxWithObservability)
	}

	return ctxWithObservability
}
