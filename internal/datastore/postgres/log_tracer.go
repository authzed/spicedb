package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/tracelog"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.32.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/telemetry/otelconv"
)

type tracingLogger struct{}

func (tl tracingLogger) Log(ctx context.Context, level tracelog.LogLevel, msg string, _ map[string]any) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent(msg, trace.WithAttributes(attribute.Stringer(otelconv.AttrDatastorePostgresLogLevel, level), semconv.DBSystemNamePostgreSQL))
}
