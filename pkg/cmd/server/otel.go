package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/contrib/propagators/ot"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	log "github.com/authzed/spicedb/internal/logging"
)

const OTelShutdownTimeout = 5 * time.Second

// otelShutdowner is the interface satisfied by *sdktrace.TracerProvider.
// Using a local interface instead of the concrete type makes
// ShutdownOTelProvider testable with a mock without importing sdktrace.
type otelShutdowner interface {
	Shutdown(ctx context.Context) error
	ForceFlush(ctx context.Context) error
}

// OTelConfig holds the configuration for the OpenTelemetry provider.
// It is bound to Cobra flags by RegisterOTelFlags and lives on server.Config
// so the provider can be constructed during Complete().
// Embedded callers may set it programmatically.
type OTelConfig struct {
	Provider        string
	Endpoint        string
	ServiceName     string
	TracePropagator string
	Insecure        bool
	SampleRatio     float64
}

// RegisterOTelFlags registers all OpenTelemetry flags on cmd, binding them directly into cfg.
func RegisterOTelFlags(cmd *cobra.Command, cfg *OTelConfig) {
	f := cmd.Flags()
	f.StringVar(&cfg.Provider, "otel-provider", "none", `OpenTelemetry provider for tracing ("none", "otlphttp", "otlpgrpc")`)
	f.StringVar(&cfg.Endpoint, "otel-endpoint", "", `OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables`)
	f.StringVar(&cfg.ServiceName, "otel-service-name", "spicedb", `service name for trace data`)
	f.StringVar(&cfg.TracePropagator, "otel-trace-propagator", "w3c", `OpenTelemetry trace propagation format ("b3", "w3c", "ottrace"). Add multiple propagators separated by comma.`)
	f.BoolVar(&cfg.Insecure, "otel-insecure", false, `connect to the OpenTelemetry collector in plaintext`)
	f.Float64Var(&cfg.SampleRatio, "otel-sample-ratio", 0.01, `ratio of traces that are sampled`)
}

// InitOTelProvider builds a TracerProvider from cfg, sets it as the global OTel
// provider, and returns a shutdown function.
// When cfg.Provider is "none" or empty, the returned function is a no-op.
func InitOTelProvider(ctx context.Context, cfg OTelConfig) (func() error, error) {
	providerName := strings.TrimSpace(strings.ToLower(cfg.Provider))

	if providerName == "none" || providerName == "" {
		return func() error { return nil }, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(cfg.ServiceName)),
		resource.WithProcess(),
		resource.WithOS(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, fmt.Errorf("building OTel resource: %w", err)
	}

	var exporter sdktrace.SpanExporter

	switch providerName {
	case "otlpgrpc":
		opts := []otlptracegrpc.Option{}
		if cfg.Endpoint != "" {
			opts = append(opts, otlptracegrpc.WithEndpoint(cfg.Endpoint))
		}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		exp, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("creating otlpgrpc exporter: %w", err)
		}
		exporter = exp

	case "otlphttp":
		opts := []otlptracehttp.Option{}
		if cfg.Endpoint != "" {
			opts = append(opts, otlptracehttp.WithEndpoint(cfg.Endpoint))
		}
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		exp, err := otlptracehttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("creating otlphttp exporter: %w", err)
		}
		exporter = exp

	default:
		return nil, fmt.Errorf(
			"unknown otel-provider %q: must be one of: none, otlpgrpc, otlphttp",
			providerName,
		)
	}

	sampleRatio := cfg.SampleRatio
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(buildPropagator(cfg.TracePropagator))

	return func() error { return ShutdownOTelProvider(context.Background(), tp) }, nil
}

// ShutdownOTelProvider flushes all pending spans then shuts the provider down.
// ForceFlush is always called before Shutdown. Safe to call with nil (no-op).
func ShutdownOTelProvider(ctx context.Context, provider otelShutdowner) error {
	if provider == nil {
		return nil
	}
	shutCtx, cancel := context.WithTimeout(ctx, OTelShutdownTimeout)
	defer cancel()
	log.Info().Msg("shutting down OTel provider")
	if err := provider.ForceFlush(shutCtx); err != nil {
		// Log but continue — Shutdown must still be attempted.
		log.Warn().Err(err).Msg("otel: ForceFlush error during shutdown")
	}
	return provider.Shutdown(shutCtx)
}

// buildPropagator returns the TextMapPropagator for the given name.
func buildPropagator(names string) propagation.TextMapPropagator {
	var tmPropagators []propagation.TextMapPropagator
	for _, p := range strings.Split(names, ",") {
		switch strings.ToLower(strings.TrimSpace(p)) {
		case "b3":
			tmPropagators = append(tmPropagators, b3.New())
		case "ottrace":
			tmPropagators = append(tmPropagators, ot.OT{})
		case "w3c":
			fallthrough
		default:
			tmPropagators = append(tmPropagators, propagation.Baggage{})
			tmPropagators = append(tmPropagators, propagation.TraceContext{})
		}
	}
	return propagation.NewCompositeTextMapPropagator(tmPropagators...)
}
