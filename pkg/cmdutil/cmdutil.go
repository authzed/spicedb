// Package cmdutil implements various boilerplate for writing consistent CLI
// interfaces with Cobra.
package cmdutil

import (
	"strings"

	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/trace/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"
)

// LoggingPreRun reads the flag "log-level" and sets the corresponding zerolog
// log level.
//
// This function exits with log.Fatal on failure.
func LoggingPreRun(cmd *cobra.Command, args []string) {
	level := strings.ToLower(cobrautil.MustGetString(cmd, "log-level"))
	switch level {
	case "trace":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "fatal":
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	case "panic":
		zerolog.SetGlobalLevel(zerolog.PanicLevel)
	default:
		log.Fatal().Str("level", level).Msg("unknown log level")
	}

	log.Info().Str("new level", level).Msg("set log level")
}

// TracingPreRun reads the flags "tracing", "tracing-collector-endpoint", and
// "tracing-service-name" and configures the corresponding tracing provider.
//
// This function exits with log.Fatal on failure.
func TracingPreRun(cmd *cobra.Command, args []string) {
	provider := strings.ToLower(cobrautil.MustGetString(cmd, "tracing"))
	switch provider {
	case "none":
		// Nothing.
	case "jaeger":
		initJaegerTracer(
			cobrautil.MustGetString(cmd, "tracing-collector-endpoint"),
			cobrautil.MustGetString(cmd, "tracing-service-name"),
		)
	default:
		log.Fatal().Str("provider", provider).Msg("unknown tracing provider")
	}

	log.Info().Str("new provider", provider).Msg("set tracing provider")
}

func initJaegerTracer(endpoint, serviceName string) {
	exp, err := jaeger.NewRawExporter(jaeger.WithCollectorEndpoint(endpoint))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize jaeger exporter")
	}

	bsp := sdktrace.NewBatchSpanProcessor(exp)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithResource(resource.NewWithAttributes(semconv.ServiceNameKey.String(serviceName))),
	)
	otel.SetTracerProvider(tp)
}
