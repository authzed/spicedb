package postgres

import (
	"context"

	"github.com/jackc/pgx/v4"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type tracingLogger struct{}

func (tl tracingLogger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(msg, trace.WithAttributes(attribute.Stringer("level", level), attribute.String("datastore", "postgres")))
}
