package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/contrib/propagators/ot"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/telemetry/otelconv"
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.oteloptions.go . OTelConfig

const OTelShutdownTimeout = 5 * time.Second

type otelShutdowner interface {
	Shutdown(ctx context.Context) error
	ForceFlush(ctx context.Context) error
}

type OTelConfig struct {
	Provider        string  `debugmap:"visible" default:"none"`
	Endpoint        string  `debugmap:"visible"`
	ServiceName     string  `debugmap:"visible" default:"spicedb"`
	TracePropagator string  `debugmap:"visible" default:"w3c"`
	UsePlaintext    bool    `debugmap:"visible"`
	SampleRatio     float64 `debugmap:"visible" default:"0.01"`
	// IgnoreRemoteSampling makes SpiceDB apply SampleRatio to every request,
	// including those the caller marked as sampled (or not sampled) in its trace
	// headers. The zero value keeps the default behaviour of following the caller.
	IgnoreRemoteSampling bool                  `debugmap:"visible"`
	PrometheusRegistry   prometheus.Registerer `debugmap:"hidden"`
}

func (o *OTelConfig) SetDefaults() {
	o.PrometheusRegistry = prometheus.NewRegistry()
}

// RegisterOTelFlags registers all OpenTelemetry flags on cmd, binding them directly into cfg.
func RegisterOTelFlags(cmd *cobra.Command, cfg *OTelConfig) {
	f := cmd.Flags()
	f.StringVar(&cfg.Provider, "otel-provider", "none", `OpenTelemetry provider for tracing ("none", "otlphttp", "otlpgrpc")`)
	f.StringVar(&cfg.Endpoint, "otel-endpoint", "", `OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables`)
	f.StringVar(&cfg.ServiceName, "otel-service-name", "spicedb", `service name for trace data`)
	f.StringVar(&cfg.TracePropagator, "otel-trace-propagator", "w3c", `OpenTelemetry trace propagation format ("b3", "w3c", "ottrace"). Add multiple propagators separated by comma.`)
	f.BoolVar(&cfg.UsePlaintext, "otel-insecure", false, `connect to the OpenTelemetry collector in plaintext`)
	f.Float64Var(&cfg.SampleRatio, "otel-sample-ratio", 0.01, `ratio of traces that are sampled`)
	f.BoolVar(&cfg.IgnoreRemoteSampling, "otel-ignore-remote-sampling", false, `ignore the sampling decision sent by callers in their trace headers and apply otel-sample-ratio to every request. The caller's trace ID is still kept`)
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
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String(otelconv.ServiceName, cfg.ServiceName)),
		resource.WithHost(),
		// WithFromEnv brings in any OTEL_* environment variables and merges them into the
		// realized configuration.
		resource.WithFromEnv(),
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
		if cfg.UsePlaintext {
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
		if cfg.UsePlaintext {
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

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(buildSampler(cfg.SampleRatio, cfg.IgnoreRemoteSampling)),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(buildPropagator(cfg.TracePropagator))

	return func() error { return ShutdownOTelProvider(context.Background(), tp) }, nil
}

// buildSampler returns the sampler for the given ratio.
//
// Spans whose parent was created in this process always follow the parent, so
// a trace is never cut in the middle. By default, spans whose parent came from a
// caller's trace headers follow the caller's decision. When ignoreRemote is set,
// the ratio applies to them too, so callers can't make SpiceDB trace more (or
// less) than the configured ratio.
//
// The ratio sampler decides from the trace ID alone, so every SpiceDB node (and
// any downstream service that follows its parent) reaches the same decision for
// a given trace, as long as they all use the same ratio.
func buildSampler(ratio float64, ignoreRemote bool) sdktrace.Sampler {
	ratioSampler := sdktrace.TraceIDRatioBased(ratio)
	if !ignoreRemote {
		return sdktrace.ParentBased(ratioSampler)
	}
	return sdktrace.ParentBased(ratioSampler,
		sdktrace.WithRemoteParentSampled(ratioSampler),
		sdktrace.WithRemoteParentNotSampled(ratioSampler),
	)
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
