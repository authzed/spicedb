package cobrautil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/jzelinskie/stringz"
	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
)

// IsBuiltinCommand checks against a hard-coded list of the names of commands
// that cobra provides out-of-the-box.
func IsBuiltinCommand(cmd *cobra.Command) bool {
	return stringz.SliceContains([]string{
		"help [command]",
		"completion [command]",
	},
		cmd.Use,
	)
}

// SyncViperPreRunE returns a Cobra run func that synchronizes Viper environment
// flags prefixed with the provided argument.
//
// Thanks to Carolyn Van Slyck: https://github.com/carolynvs/stingoftheviper
func SyncViperPreRunE(prefix string) func(cmd *cobra.Command, args []string) error {
	prefix = strings.ReplaceAll(strings.ToUpper(prefix), "-", "_")
	return func(cmd *cobra.Command, args []string) error {
		if IsBuiltinCommand(cmd) {
			return nil // No-op for builtins
		}

		v := viper.New()
		viper.SetEnvPrefix(prefix)

		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			suffix := strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
			_ = v.BindEnv(f.Name, prefix+"_"+suffix)

			if !f.Changed && v.IsSet(f.Name) {
				val := v.Get(f.Name)
				_ = cmd.Flags().Set(f.Name, fmt.Sprintf("%v", val))
			}
		})

		return nil
	}
}

// CobraRunFunc is the signature of cobra.Command RunFuncs.
type CobraRunFunc func(cmd *cobra.Command, args []string) error

// RunFuncStack chains together a collection of CobraCommandFuncs into one.
func CommandStack(cmdfns ...CobraRunFunc) CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		for _, cmdfn := range cmdfns {
			if err := cmdfn(cmd, args); err != nil {
				return err
			}
		}
		return nil
	}
}

// RegisterZeroLogFlags adds flags for use in with ZeroLogPreRunE:
// - "$PREFIX-level"
// - "$PREFIX-format"
func RegisterZeroLogFlags(flags *pflag.FlagSet, flagPrefix string) {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "log")
	flags.String(flagPrefix+"-level", "info", `verbosity of logging ("trace", "debug", "info", "warn", "error")`)
	flags.String(flagPrefix+"-format", "auto", `format of logs ("auto", "console", "json")`)
}

// ZeroLogRunE returns a Cobra run func that configures the corresponding
// log level from a command.
//
// The required flags can be added to a command by using
// RegisterLoggingPersistentFlags().
func ZeroLogRunE(flagPrefix string, prerunLevel zerolog.Level) CobraRunFunc {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "log")
	return func(cmd *cobra.Command, args []string) error {
		if IsBuiltinCommand(cmd) {
			return nil // No-op for builtins
		}

		format := MustGetString(cmd, flagPrefix+"-format")
		if format == "console" || format == "auto" && isatty.IsTerminal(os.Stdout.Fd()) {
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		}

		level := strings.ToLower(MustGetString(cmd, flagPrefix+"-level"))
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
			return fmt.Errorf("unknown log level: %s", level)
		}

		log.WithLevel(prerunLevel).Str("new level", level).Msg("set log level")

		return nil
	}
}

// RegisterOpenTelemetryFlags adds the following flags for use with
// OpenTelemetryPreRunE:
// - "$PREFIX-provider"
// - "$PREFIX-endpoint"
// - "$PREFIX-service-name"
func RegisterOpenTelemetryFlags(flags *pflag.FlagSet, flagPrefix, serviceName string) {
	bi, _ := debug.ReadBuildInfo()
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "otel")
	serviceName = stringz.DefaultEmpty(serviceName, bi.Main.Path)

	flags.String(flagPrefix+"-provider", "none", `opentelemetry provider for tracing ("none", "jaeger, otlphttp")`)
	flags.String(flagPrefix+"-endpoint", "http://jaeger:14268/api/traces", "collector endpoint")
	flags.String(flagPrefix+"-service-name", serviceName, "service name for trace data")
}

// OpenTelemetryRunE returns a Cobra run func that configures the
// corresponding otel provider from a command.
//
// The required flags can be added to a command by using
// RegisterOpenTelemetryFlags().
func OpenTelemetryRunE(flagPrefix string, prerunLevel zerolog.Level) CobraRunFunc {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "otel")
	return func(cmd *cobra.Command, args []string) error {
		if IsBuiltinCommand(cmd) {
			return nil // No-op for builtins
		}

		provider := strings.ToLower(MustGetString(cmd, flagPrefix+"-provider"))
		switch provider {
		case "none":
			// Nothing.
		case "jaeger":
			return initJaegerTracer(
				MustGetString(cmd, flagPrefix+"-jaeger-endpoint"),
				MustGetString(cmd, flagPrefix+"-service-name"),
			)
		case "otlphttp":
			return initOtlpHttpTracer(MustGetString(cmd, flagPrefix+"-endpoint"), MustGetString(cmd, flagPrefix+"-service-name"))
		default:
			return fmt.Errorf("unknown tracing provider: %s", provider)
		}

		log.WithLevel(prerunLevel).Str("new provider", provider).Msg("set tracing provider")
		return nil
	}
}

func initJaegerTracer(endpoint, serviceName string) error {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
	if err != nil {
		return err
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
	return nil
}

func initOtlpHttpTracer(endpoint, serviceName string) error {
	ctx := context.Background()

	// TODO: this got messy needs a spring clean

	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithHeaders(map[string]string{"lightstep-access-token": "developer"}),
		otlptracegrpc.WithCompressor(gzip.Name),
		otlptracegrpc.WithInsecure(),
	)
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return err
	}

	attributes := []attribute.KeyValue{
		semconv.ServiceNameKey.String(serviceName),
	}
	r, _ := resource.New(
		context.Background(),
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithAttributes(attributes...),
	)

	tp := trace.NewTracerProvider(
		trace.WithResource(r),
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithSpanProcessor(trace.NewBatchSpanProcessor(exporter)),
	)
	otel.SetTracerProvider(tp)

	// Configure the global tracer to use the W3C method for propagating contexts
	// across services.
	//
	// For low-level details see:
	// https://www.w3.org/TR/trace-context/
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.Baggage{},
		propagation.TraceContext{},
	))
	return nil
}

// RegisterGrpcServerFlags adds the following flags for use with
// GrpcServerFromFlags:
// - "$PREFIX-addr"
// - "$PREFIX-tls-cert-path"
// - "$PREFIX-tls-key-path"
// - "$PREFIX-max-conn-age"
func RegisterGrpcServerFlags(flags *pflag.FlagSet, flagPrefix, serviceName, defaultAddr string, defaultEnabled bool) {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "grpc")
	serviceName = stringz.DefaultEmpty(serviceName, "grpc")
	defaultAddr = stringz.DefaultEmpty(defaultAddr, ":50051")

	flags.String(flagPrefix+"-addr", defaultAddr, "address to listen on to serve "+serviceName)
	flags.String(flagPrefix+"-network", "tcp", "network type to serve "+serviceName+` ("tcp", "tcp4", "tcp6", "unix", "unixpacket")`)
	flags.String(flagPrefix+"-tls-cert-path", "", "local path to the TLS certificate used to serve "+serviceName)
	flags.String(flagPrefix+"-tls-key-path", "", "local path to the TLS key used to serve "+serviceName)
	flags.Duration(flagPrefix+"-max-conn-age", 30*time.Second, "how long a connection serving "+serviceName+" should be able to live")
	flags.Bool(flagPrefix+"-enabled", defaultEnabled, "enable "+serviceName+" gRPC server")
}

// GrpcServerFromFlags creates an *grpc.Server as configured by the flags from
// RegisterGrpcServerFlags().
func GrpcServerFromFlags(cmd *cobra.Command, flagPrefix string, opts ...grpc.ServerOption) (*grpc.Server, error) {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "grpc")

	opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionAge: MustGetDuration(cmd, flagPrefix+"-max-conn-age"),
	}))

	certPath := MustGetStringExpanded(cmd, flagPrefix+"-tls-cert-path")
	keyPath := MustGetStringExpanded(cmd, flagPrefix+"-tls-key-path")

	switch {
	case certPath == "" && keyPath == "":
		log.Warn().Str("prefix", flagPrefix).Msg("grpc server serving plaintext")
		return grpc.NewServer(opts...), nil

	case certPath != "" && keyPath != "":
		creds, err := credentials.NewServerTLSFromFile(certPath, keyPath)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(creds))
		return grpc.NewServer(opts...), nil

	default:
		return nil, fmt.Errorf(
			"failed to start gRPC server: must provide both --%s-tls-cert-path and --%s-tls-key-path",
			flagPrefix,
			flagPrefix,
		)
	}
}

// GrpcListenFromFlags listens on an gRPC server using the configuration stored
// in the cobra command that was registered with RegisterGrpcServerFlags.
func GrpcListenFromFlags(cmd *cobra.Command, flagPrefix string, srv *grpc.Server, level zerolog.Level) error {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "grpc")

	if !MustGetBool(cmd, flagPrefix+"-enabled") {
		return nil
	}

	network := MustGetString(cmd, flagPrefix+"-network")
	addr := MustGetStringExpanded(cmd, flagPrefix+"-addr")

	l, err := net.Listen(network, addr)
	if err != nil {
		return fmt.Errorf("failed to listen on addr for gRPC server: %w", err)
	}

	log.WithLevel(level).
		Str("addr", addr).
		Str("network", network).
		Str("prefix", flagPrefix).
		Msg("grpc server started listening")

	if err := srv.Serve(l); err != nil {
		return fmt.Errorf("failed to serve gRPC: %w", err)
	}

	return nil
}

// RegisterHTTPServerFlags adds the following flags for use with
// HttpServerFromFlags:
// - "$PREFIX-addr"
// - "$PREFIX-tls-cert-path"
// - "$PREFIX-tls-key-path"
// - "$PREFIX-enabled"
func RegisterHTTPServerFlags(flags *pflag.FlagSet, flagPrefix, serviceName, defaultAddr string, defaultEnabled bool) {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "http")
	serviceName = stringz.DefaultEmpty(serviceName, "http")
	defaultAddr = stringz.DefaultEmpty(defaultAddr, ":8443")

	flags.String(flagPrefix+"-addr", defaultAddr, "address to listen on to serve "+serviceName)
	flags.String(flagPrefix+"-tls-cert-path", "", "local path to the TLS certificate used to serve "+serviceName)
	flags.String(flagPrefix+"-tls-key-path", "", "local path to the TLS key used to serve "+serviceName)
	flags.Bool(flagPrefix+"-enabled", defaultEnabled, "enable "+serviceName+" http server")
}

// HTTPServerFromFlags creates an *http.Server as configured by the flags from
// RegisterHttpServerFlags().
func HTTPServerFromFlags(cmd *cobra.Command, flagPrefix string) *http.Server {
	flagPrefix = stringz.DefaultEmpty(flagPrefix, "http")
	return &http.Server{
		Addr: MustGetStringExpanded(cmd, flagPrefix+"-addr"),
	}
}

// HTTPListenFromFlags listens on an HTTP server using the configuration stored
// in the cobra command that was registered with RegisterHttpServerFlags.
func HTTPListenFromFlags(cmd *cobra.Command, flagPrefix string, srv *http.Server, level zerolog.Level) error {
	if !MustGetBool(cmd, flagPrefix+"-enabled") {
		return nil
	}

	certPath := MustGetStringExpanded(cmd, flagPrefix+"-tls-cert-path")
	keyPath := MustGetStringExpanded(cmd, flagPrefix+"-tls-key-path")

	switch {
	case certPath == "" && keyPath == "":
		log.Warn().Str("addr", srv.Addr).Str("prefix", flagPrefix).Msg("http server serving plaintext")
		if err := srv.ListenAndServe(); err != nil && errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("failed while serving http: %w", err)
		}
		return nil

	case certPath != "" && keyPath != "":
		log.WithLevel(level).Str("addr", srv.Addr).Str("prefix", flagPrefix).Msg("https server started serving")
		if err := srv.ListenAndServeTLS(certPath, keyPath); err != nil && errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("failed while serving https: %w", err)
		}
		return nil

	default:
		return fmt.Errorf(
			"failed to start http server: must provide both --%s-tls-cert-path and --%s-tls-key-path",
			flagPrefix,
			flagPrefix,
		)
	}
}
