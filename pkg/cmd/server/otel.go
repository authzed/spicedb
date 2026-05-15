// pkg/cmd/server/otel.go
//
// This file owns the full OpenTelemetry lifecycle for SpiceDB:
//
//  1. RegisterOTelFlags  — registers CLI flags on a cobra.Command.
//  2. OTelConfig / PopulateOTelConfig — struct + helper to move values from
//     Cobra flags into a plain Go struct so OTel config is accessible
//     programmatically (e.g. in tests and embedded use-cases).
//  3. InitOTelProvider   — builds and registers the global TracerProvider.
//  4. ShutdownOTelProvider — flush + shutdown with a single shared timeout.
//
// Why native lifecycle instead of cobraotel?
//   - cobraotel registered a PreRunE but had no corresponding PostRunE hook,
//     leaving the TracerProvider with no owner to call Shutdown/ForceFlush.
//     Traces are dropped on SIGTERM. Native ownership closes this lifecycle gap.

package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/authzed/ctxkey"
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

// otelProviderKey is the context key for storing an otelShutdowner.
// Using ctxkey.New follows the codebase-wide pattern (see pkg/datalayer/context.go).
var otelProviderKey = ctxkey.New[otelShutdowner]()

// OTelShutdownTimeout is the maximum time for flushing spans on shutdown.
const OTelShutdownTimeout = 30 * time.Second

// otelShutdowner is the interface satisfied by *sdktrace.TracerProvider.
// Using a local interface instead of the concrete type makes
// ShutdownOTelProvider testable with a mock without importing sdktrace.
type otelShutdowner interface {
	Shutdown(ctx context.Context) error
	ForceFlush(ctx context.Context) error
}

// OTelConfig holds the configuration for the OpenTelemetry provider.
// It is populated from Cobra flags by PopulateOTelConfig and then passed
// into server.Config so the provider can be constructed during Complete().
type OTelConfig struct {
	Provider        string
	Endpoint        string
	ServiceName     string
	TracePropagator string
	Insecure        bool
	Headers         map[string]string
	SampleRatio     float64
}

// PopulateOTelConfig reads all otel-* flag values from cmd into an OTelConfig.
// Call this from the serve RunE after Viper has synced flags, before calling
// server.Complete.
func PopulateOTelConfig(cmd *cobra.Command) (OTelConfig, error) {
	f := cmd.Flags()

	provider, err := f.GetString("otel-provider")
	if err != nil {
		return OTelConfig{}, fmt.Errorf("reading otel-provider: %w", err)
	}
	endpoint, err := f.GetString("otel-endpoint")
	if err != nil {
		return OTelConfig{}, fmt.Errorf("reading otel-endpoint: %w", err)
	}
	serviceName, err := f.GetString("otel-service-name")
	if err != nil {
		return OTelConfig{}, fmt.Errorf("reading otel-service-name: %w", err)
	}
	propagator, err := f.GetString("otel-trace-propagator")
	if err != nil {
		return OTelConfig{}, fmt.Errorf("reading otel-trace-propagator: %w", err)
	}
	insecure, err := f.GetBool("otel-insecure")
	if err != nil {
		return OTelConfig{}, fmt.Errorf("reading otel-insecure: %w", err)
	}
	headers, err := f.GetStringToString("otel-headers")
	if err != nil {
		return OTelConfig{}, fmt.Errorf("reading otel-headers: %w", err)
	}
	sampleRatio, err := f.GetFloat64("otel-sample-ratio")
	if err != nil {
		return OTelConfig{}, fmt.Errorf("reading otel-sample-ratio: %w", err)
	}

	return OTelConfig{
		Provider:        strings.TrimSpace(strings.ToLower(provider)),
		Endpoint:        endpoint,
		ServiceName:     serviceName,
		TracePropagator: propagator,
		Insecure:        insecure,
		Headers:         headers,
		SampleRatio:     sampleRatio,
	}, nil
}

// RegisterOTelFlags registers all OpenTelemetry flags on cmd.
// The flags registered here are identical in name and default value to those
// previously registered by cobraotel.RegisterFlags.
func RegisterOTelFlags(cmd *cobra.Command) {
	f := cmd.Flags()
	f.String("otel-provider", "none",
		`OpenTelemetry provider for tracing ("none", "otlpgrpc", "otlphttp")`)
	f.String("otel-endpoint", "",
		`OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables`)
	f.String("otel-service-name", "spicedb",
		`service name for trace data`)
	f.String("otel-trace-propagator", "w3c",
		`OpenTelemetry trace propagation format ("b3", "w3c", "ottrace"). Add multiple propagators separated by comma.`)
	f.Bool("otel-insecure", false,
		`connect to the OpenTelemetry collector in plaintext`)
	f.StringToString("otel-headers", nil,
		`key=value pairs sent as headers to the OTel provider`)
	f.Float64("otel-sample-ratio", 0.01,
		`ratio of traces that are sampled`)
}

// InitOTelProvider builds a TracerProvider from cfg, sets it as the global OTel
// provider, and returns it for lifecycle management.
//
// Returns (nil, nil) when cfg.Provider is "none" or empty. Callers must handle nil.
func InitOTelProvider(ctx context.Context, cfg OTelConfig) (otelShutdowner, error) {
	providerName := cfg.Provider

	if providerName == "none" || providerName == "" {
		return nil, nil
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
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlptracegrpc.WithHeaders(cfg.Headers))
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
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlptracehttp.WithHeaders(cfg.Headers))
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

	return tp, nil
}

// ShutdownOTelProvider flushes all pending spans then shuts the provider down.
// ForceFlush is always called before Shutdown. Safe to call with nil (no-op).
func ShutdownOTelProvider(ctx context.Context, provider otelShutdowner) error {
	if provider == nil {
		return nil
	}
	// A single context covers both ForceFlush and Shutdown: they share the same
	// overall shutdown budget. Separate contexts would silently double the max
	// wait time.
	shutCtx, cancel := context.WithTimeout(ctx, OTelShutdownTimeout)
	defer cancel()
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
