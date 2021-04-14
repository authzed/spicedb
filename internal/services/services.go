package services

import "go.opentelemetry.io/otel"

var tracer = otel.Tracer("spicedb/internal/services")
