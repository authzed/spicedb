// Package cobraotel implements a builder for registering flags and producing
// a Cobra RunFunc that configures OpenTelemetry.
package cobraotel

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/go-logr/logr"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/stringz"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/contrib/propagators/ot"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
)

// Option is function used to configure OpenTelemetry within a Cobra RunFunc.
type Option func(*Builder)

// New creates a Cobra RunFunc Builder for OpenTelemetry.
func New(serviceName string, opts ...Option) *Builder {
	bi, ok := debug.ReadBuildInfo()
	if !ok && serviceName == "" {
		panic("no service name provided and failed to read from debug info")
	}

	b := &Builder{
		flagPrefix:  "otel",
		serviceName: stringz.DefaultEmpty(serviceName, bi.Main.Path),
		preRunLevel: 0,
		logger:      logr.Discard(),
	}
	for _, configure := range opts {
		configure(b)
	}
	return b
}

// Builder is used to configure OpenTelemetry via Cobra.
type Builder struct {
	flagPrefix  string
	serviceName string
	logger      logr.Logger
	preRunLevel int
}

func (b *Builder) prefix(s string) string {
	return cobrautil.PrefixJoiner(b.flagPrefix)(s)
}

// RegisterFlags adds flags for configuring OpenTelemetry.
//
// The following flags are added:
// - "$PREFIX-provider"
// - "$PREFIX-trace-propagator"
// - "$PREFIX-insecure"
// - "$PREFIX-endpoint"
// - "$PREFIX-service-name"
func (b *Builder) RegisterFlags(flags *pflag.FlagSet) {
	flags.String(b.prefix("provider"), "none", `OpenTelemetry provider for tracing ("none", "otlphttp", "otlpgrpc")`)
	flags.String(b.prefix("endpoint"), "", "OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables")
	flags.String(b.prefix("service-name"), b.serviceName, "service name for trace data")
	flags.String(b.prefix("trace-propagator"), "w3c", `OpenTelemetry trace propagation format ("b3", "w3c", "ottrace"). Add multiple propagators separated by comma.`)
	flags.Bool(b.prefix("insecure"), false, `connect to the OpenTelemetry collector in plaintext`)
	flags.Float64(b.prefix("sample-ratio"), 0.01, "ratio of traces that are sampled")

	// Legacy flags! Will eventually be dropped!
	flags.String("otel-jaeger-endpoint", "", "OpenTelemetry collector endpoint - the endpoint can also be set by using enviroment variables")
	if err := flags.MarkHidden("otel-jaeger-endpoint"); err != nil {
		panic("failed to mark flag hidden: " + err.Error())
	}
	flags.String("otel-jaeger-service-name", b.serviceName, "service name for trace data")
	if err := flags.MarkHidden("otel-jaeger-service-name"); err != nil {
		panic("failed to mark flag hidden: " + err.Error())
	}
}

// RegisterFlagCompletion adds completion functions supported flags.
//
// The following flags are completed:
// - "$PREFIX-provider"
// - "$PREFIX-trace-propagator"
func (b *Builder) RegisterFlagCompletion(cmd *cobra.Command) error {
	if err := cmd.RegisterFlagCompletionFunc(b.prefix("provider"), func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"none", "otlphttp", "otlpgrpc"}, cobra.ShellCompDirectiveDefault
	}); err != nil {
		return err
	}

	if err := cmd.RegisterFlagCompletionFunc(b.prefix("trace-propagator"), func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"b3", "w3c", "ottrace"}, cobra.ShellCompDirectiveDefault
	}); err != nil {
		return err
	}

	return nil
}

// RunE returns a Cobra run func that configures the
// corresponding otel provider from a command.
//
// The required flags can be added to a command by using
// RegisterOpenTelemetryFlags().
func (b *Builder) RunE() cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		if cobrautil.IsBuiltinCommand(cmd) {
			return nil // No-op for builtins
		}

		provider := strings.ToLower(cobrautil.MustGetString(cmd, b.prefix("provider")))
		serviceName := cobrautil.MustGetString(cmd, b.prefix("service-name"))
		endpoint := cobrautil.MustGetString(cmd, b.prefix("endpoint"))
		insecure := cobrautil.MustGetBool(cmd, b.prefix("insecure"))
		propagators := strings.Split(cobrautil.MustGetString(cmd, b.prefix("trace-propagator")), ",")
		sampleRatio := cobrautil.MustGetFloat64(cmd, b.prefix("sample-ratio"))
		var noLogger logr.Logger
		if b.logger != noLogger {
			otel.SetLogger(b.logger)
		}

		var exporter trace.SpanExporter
		var err error

		// If endpoint is not set, the clients are configured via the OpenTelemetry environment variables or
		// default values.
		// See: https://github.com/open-telemetry/opentelemetry-go/tree/main/exporters/otlp/otlptrace#environment-variables
		// or https://github.com/open-telemetry/opentelemetry-go/tree/main/exporters/jaeger#environment-variables
		switch provider {
		case "none":
			// Nothing.
		case "otlphttp":
			var opts []otlptracehttp.Option
			if endpoint != "" {
				opts = append(opts, otlptracehttp.WithEndpoint(endpoint))
			}
			if insecure {
				opts = append(opts, otlptracehttp.WithInsecure())
			}
			exporter, err = otlptrace.New(context.Background(), otlptracehttp.NewClient(opts...))
			if err != nil {
				return err
			}

			if err := initOtelTracer(exporter, serviceName, propagators, sampleRatio); err != nil {
				return err
			}
		case "otlpgrpc":
			var opts []otlptracegrpc.Option
			if endpoint != "" {
				opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
			}
			if insecure {
				opts = append(opts, otlptracegrpc.WithInsecure())
			}

			exporter, err = otlptrace.New(context.Background(), otlptracegrpc.NewClient(opts...))
			if err != nil {
				return err
			}

			if err := initOtelTracer(exporter, serviceName, propagators, sampleRatio); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown tracing provider: %s", provider)
		}

		b.logger.V(b.preRunLevel).Info(
			"configured opentelemetry tracing",
			"provider", provider,
			"endpoint", endpoint,
			"service", serviceName,
			"insecure", insecure,
			"sampleRatio", sampleRatio,
		)
		return nil
	}
}

func initOtelTracer(exporter trace.SpanExporter, serviceName string, propagators []string, sampleRatio float64) error {
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return err
	}

	otel.SetTracerProvider(trace.NewTracerProvider(
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(sampleRatio))),
		trace.WithBatcher(exporter),
		trace.WithResource(res),
	))
	setTracePropagators(propagators)

	return nil
}

// setTextMapPropagator sets the OpenTelemetry trace propagation format.
// Currently it supports b3, ot-trace and w3c.
func setTracePropagators(propagators []string) {
	var tmPropagators []propagation.TextMapPropagator

	for _, p := range propagators {
		switch p {
		case "b3":
			tmPropagators = append(tmPropagators, b3.New())
		case "ottrace":
			tmPropagators = append(tmPropagators, ot.OT{})
		case "w3c":
			fallthrough
		default:
			// W3C baggage support
			tmPropagators = append(tmPropagators, propagation.Baggage{})
			// W3C for compatibility with other tracing systems
			tmPropagators = append(tmPropagators, propagation.TraceContext{})
		}
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(tmPropagators...))
}

// WithLogger configures logging of the configured OpenTelemetry environment.
func WithLogger(logger logr.Logger) Option {
	return func(b *Builder) { b.logger = logger }
}

// WithFlagPrefix defines prefix used with the generated flags.
//
// Defaults to "log".
func WithFlagPrefix(flagPrefix string) Option {
	return func(b *Builder) { b.flagPrefix = flagPrefix }
}

// WithPreRunLevel defines the logging level used for pre-run log messages.
//
// Defaults to "debug".
func WithPreRunLevel(preRunLevel int) Option {
	return func(b *Builder) { b.preRunLevel = preRunLevel }
}
