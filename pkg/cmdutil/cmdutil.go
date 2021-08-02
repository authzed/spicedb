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
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// RegisterLoggingPersistentFlags registers the PersistentFlags required to
// use LoggingPreRun().
func RegisterLoggingPersistentFlags(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().String("log-level", "info", "verbosity of logging (trace, debug, info, warn, error, fatal, panic)")
}

// TracingPreRun reads the provided command's flags and configures the
// corresponding log level. The required flags can be added to a command by
// using RegisterLoggingPersistentFlags().
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

// RegisterTracingPersistentFlags registers the PersistentFlags required to
// use TracingPreRun().
func RegisterTracingPersistentFlags(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().String("tracing", "none", "destination for tracing (none, jaeger)")
	rootCmd.PersistentFlags().String("tracing-collector-endpoint", "http://jaeger:14268/api/traces", "endpoint to which to send tracing data")
	rootCmd.PersistentFlags().String("tracing-service-name", rootCmd.Use, "service name to use when sending trace data")
}

// TracingPreRun reads the provided command's flags and configures the
// corresponding tracing provider. The required flags can be added to a command
// by using RegisterTracingPersistentFlags().
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
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize jaeger exporter")
	}

	// Configure the global tracer as a batched, always sampling Jaeger exporter.
	otel.SetTracerProvider(trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithSpanProcessor(trace.NewBatchSpanProcessor(exp)),
		trace.WithResource(resource.NewSchemaless(semconv.ServiceNameKey.String(serviceName))),
	))

	// Configure the global tracer to use the W3C method for propagating contexts
	// across services.
	//
	// For low-level details see:
	// https://www.w3.org/TR/trace-context/
	otel.SetTextMapPropagator(propagation.TraceContext{})
}
