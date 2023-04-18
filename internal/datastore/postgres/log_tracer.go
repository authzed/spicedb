package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/tracelog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type tracingLogger struct{}

func (tl tracingLogger) Log(ctx context.Context, level tracelog.LogLevel, msg string, _ map[string]interface{}) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(msg, trace.WithAttributes(attribute.Stringer("level", level), attribute.String("datastore", "postgres")))
}
