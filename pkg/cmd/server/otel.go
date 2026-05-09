// pkg/cmd/server/otel.go
//
// This file replicates the OpenTelemetry provider initialization previously
// provided by github.com/jzelinskie/cobrautil/v2/cobraotel.
//
// Motivation:
//   - Issue #712: otel-provider defaults to "none" so OTEL_* env vars alone
//     cannot activate tracing. Native ownership lets us address this properly.
//   - Issue #3095: cobraotel owns the TracerProvider with no way for the
//     signal handler to call Shutdown/ForceFlush. Traces are dropped on
//     SIGTERM. Native ownership closes this lifecycle gap.

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
)

// otelProviderContextKey is the unexported context key for the TracerProvider.
type otelProviderContextKey struct{}

// OTelShutdownTimeout is the maximum time for flushing spans on shutdown.
const OTelShutdownTimeout = 30 * time.Second

// otelShutdowner is the interface satisfied by *sdktrace.TracerProvider.
// Using a local interface instead of the concrete type makes
// ShutdownOTelProvider testable with a mock without importing sdktrace.
type otelShutdowner interface {
	Shutdown(ctx context.Context) error
	ForceFlush(ctx context.Context) error
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

	// Legacy flags
	f.String("otel-jaeger-endpoint", "", "OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables")
	_ = f.MarkHidden("otel-jaeger-endpoint")
	f.String("otel-jaeger-service-name", "spicedb", "service name for trace data")
	_ = f.MarkHidden("otel-jaeger-service-name")
}

// InitOTelProvider reads otel-* flags from cmd, builds a TracerProvider, sets
// it as the global OTel provider, and returns it for lifecycle management.
//
// Returns (nil, nil) when otel-provider is "none". Callers must handle nil.
func InitOTelProvider(cmd *cobra.Command) (otelShutdowner, error) {
	flags := cmd.Flags()

	// cobra commands may have no context when called outside Execute()
	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	providerName, err := flags.GetString("otel-provider")
	if err != nil {
		return nil, fmt.Errorf("reading otel-provider: %w", err)
	}
	providerName = strings.TrimSpace(strings.ToLower(providerName))

	if providerName == "none" || providerName == "" {
		return nil, nil
	}

	endpoint, err := flags.GetString("otel-endpoint")
	if err != nil {
		return nil, fmt.Errorf("reading otel-endpoint: %w", err)
	}

	serviceName, err := flags.GetString("otel-service-name")
	if err != nil {
		return nil, fmt.Errorf("reading otel-service-name: %w", err)
	}

	propagatorName, err := flags.GetString("otel-trace-propagator")
	if err != nil {
		return nil, fmt.Errorf("reading otel-trace-propagator: %w", err)
	}

	insecureConn, err := flags.GetBool("otel-insecure")
	if err != nil {
		return nil, fmt.Errorf("reading otel-insecure: %w", err)
	}

	headers, err := flags.GetStringToString("otel-headers")
	if err != nil {
		return nil, fmt.Errorf("reading otel-headers: %w", err)
	}

	sampleRatio, err := flags.GetFloat64("otel-sample-ratio")
	if err != nil {
		return nil, fmt.Errorf("reading otel-sample-ratio: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
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
		if endpoint != "" {
			opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
		}
		if insecureConn {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		if len(headers) > 0 {
			opts = append(opts, otlptracegrpc.WithHeaders(headers))
		}
		exp, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("creating otlpgrpc exporter: %w", err)
		}
		exporter = exp

	case "otlphttp":
		opts := []otlptracehttp.Option{}
		if endpoint != "" {
			opts = append(opts, otlptracehttp.WithEndpoint(endpoint))
		}
		if insecureConn {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		if len(headers) > 0 {
			opts = append(opts, otlptracehttp.WithHeaders(headers))
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
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio))),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(buildPropagator(propagatorName))

	return tp, nil
}

// ShutdownOTelProvider flushes all pending spans then shuts the provider down.
// ForceFlush is always called before Shutdown. Safe to call with nil (no-op).
func ShutdownOTelProvider(ctx context.Context, provider otelShutdowner) error {
	if provider == nil {
		return nil
	}

	flushCtx, flushCancel := context.WithTimeout(ctx, OTelShutdownTimeout)
	defer flushCancel()
	if err := provider.ForceFlush(flushCtx); err != nil {
		// Log but continue — Shutdown must still be attempted.
		fmt.Printf("otel: ForceFlush error (continuing to Shutdown): %v\n", err)
	}

	shutCtx, shutCancel := context.WithTimeout(ctx, OTelShutdownTimeout)
	defer shutCancel()
	return provider.Shutdown(shutCtx)
}

// OTelPreRunE is a cobra.PreRunE function that initializes the OTel provider
// and stores it on the command context so the shutdown handler can retrieve it.
func OTelPreRunE(cmd *cobra.Command, _ []string) error {
	provider, err := InitOTelProvider(cmd)
	if err != nil {
		return fmt.Errorf("initializing OTel provider: %w", err)
	}
	parent := cmd.Context()
	if parent == nil {
		parent = context.Background()
	}
	ctx := context.WithValue(parent, otelProviderContextKey{}, provider)
	cmd.SetContext(ctx)
	return nil
}

// OTelProviderFromContext retrieves the otelShutdowner stored by OTelPreRunE.
// Returns nil if no provider was initialized (e.g. otel-provider was "none").
func OTelProviderFromContext(ctx context.Context) otelShutdowner {
	v, _ := ctx.Value(otelProviderContextKey{}).(otelShutdowner)
	return v
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
