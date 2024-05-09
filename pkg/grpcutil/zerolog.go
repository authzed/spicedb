package grpcutil

import (
	"context"
	"time"

	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

var (
	durationFieldOption = grpclog.WithDurationField(func(duration time.Duration) grpclog.Fields {
		return grpclog.Fields{"grpc.time_ms", duration.Milliseconds()}
	})

	traceIDFieldOption = grpclog.WithFieldsFromContext(func(ctx context.Context) grpclog.Fields {
		if span := trace.SpanContextFromContext(ctx); span.IsSampled() {
			return grpclog.Fields{"traceID", span.TraceID().String()}
		}
		return nil
	})
)

// ZerologUnaryInterceptor maps gRPC logging to Zerolog according to the
// provided code-mapping function.
func ZerologUnaryInterceptor(l zerolog.Logger, mappingfn grpclog.CodeToLevel) grpc.UnaryServerInterceptor {
	return grpclog.UnaryServerInterceptor(
		zerologger(l),
		grpclog.WithLevels(mappingfn),
		durationFieldOption,
		traceIDFieldOption,
	)
}

// ZerologStreamInterceptor maps gRPC logging to Zerolog according to the
// provided code-mapping function.
func ZerologStreamInterceptor(l zerolog.Logger, mappingfn grpclog.CodeToLevel) grpc.StreamServerInterceptor {
	return grpclog.StreamServerInterceptor(
		zerologger(l),
		grpclog.WithLevels(mappingfn),
		durationFieldOption,
		traceIDFieldOption,
	)
}

func zerologger(l zerolog.Logger) grpclog.Logger {
	return grpclog.LoggerFunc(func(ctx context.Context, lvl grpclog.Level, msg string, fields ...any) {
		l := l.With().Fields(fields).Logger()

		switch lvl {
		case grpclog.LevelDebug:
			l.Debug().Msg(msg)
		case grpclog.LevelInfo:
			l.Info().Msg(msg)
		case grpclog.LevelWarn:
			l.Warn().Msg(msg)
		case grpclog.LevelError:
			l.Error().Msg(msg)
		default:
			l.Error().Int("level", int(lvl)).Msg("unknown error level - falling back to info level")
			l.Info().Msg(msg)
		}
	})
}
