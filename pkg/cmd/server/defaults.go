package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/fatih/color"
	"github.com/felixge/fgprof"
	"github.com/go-logr/zerologr"
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobraotel"
	"github.com/jzelinskie/cobrautil/v2/cobraproclimits"
	"github.com/jzelinskie/cobrautil/v2/cobrazerolog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/authzed/authzed-go/pkg/requestmeta"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/logging"
	dispatchmw "github.com/authzed/spicedb/internal/middleware/dispatcher"
	"github.com/authzed/spicedb/internal/middleware/memoryprotection"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	consistencymw "github.com/authzed/spicedb/pkg/middleware/consistency"
	logmw "github.com/authzed/spicedb/pkg/middleware/logging"
	"github.com/authzed/spicedb/pkg/middleware/requestid"
	"github.com/authzed/spicedb/pkg/middleware/serverversion"
	"github.com/authzed/spicedb/pkg/releases"
	"github.com/authzed/spicedb/pkg/runtime"
)

var DisableTelemetryHandler *prometheus.Registry

// ServeExample creates an example usage string with the provided program name.
func ServeExample(programName string) string {
	return fmt.Sprintf(`	%[1]s:
		%[3]s serve --grpc-preshared-key "somerandomkeyhere"

	%[2]s:
		%[3]s serve --grpc-preshared-key "realkeyhere" \
		--grpc-tls-cert-path path/to/tls/cert --grpc-tls-key-path path/to/tls/key \
		--http-enabled http-tls-cert-path path/to/tls/cert --http-tls-key-path path/to/tls/key \
		--datastore-engine postgres \
		--datastore-conn-uri "postgres-connection-string-here"
`,
		color.YellowString("No TLS and in-memory datastore"),
		color.GreenString("TLS and HTTP enabled, and a real datastore"),
		programName,
	)
}

// DefaultPreRunE sets up viper, zerolog, and OpenTelemetry flag handling for a
// command.
func DefaultPreRunE(programName string) cobrautil.CobraRunFunc {
	return cobrautil.CommandStack(
		cobrautil.SyncViperDotEnvPreRunE(programName, "spicedb.env", zerologr.New(&logging.Logger)),
		cobrazerolog.New(
			cobrazerolog.WithTarget(func(logger zerolog.Logger) {
				logging.SetGlobalLogger(logger)
			}),
		).RunE(),
		// NOTE: These need to be declared after the logger to access
		// the logging context.
		// NOTE: We've observed OOMKill when setting to 1.0 under heavy-load,
		// and zero under the same load and 0.9
		cobraproclimits.SetMemLimitRunE(memlimit.WithRatio(0.9)),
		cobraproclimits.SetProcLimitRunE(),
		cobraotel.New("spicedb",
			cobraotel.WithLogger(zerologr.New(&logging.Logger)),
		).RunE(),
		releases.CheckAndLogRunE(),
		runtime.RunE(),
	)
}

// MetricsHandler sets up an HTTP server that handles serving Prometheus
// metrics and pprof endpoints.
func MetricsHandler(telemetryRegistry *prometheus.Registry, c *Config) http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
		// Opt into OpenMetrics e.g. to support exemplars.
		EnableOpenMetrics: true,
	}))
	if telemetryRegistry != nil {
		mux.Handle("/telemetry", promhttp.HandlerFor(telemetryRegistry, promhttp.HandlerOpts{}))
	}

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.Handle("/debug/fgprof", fgprof.Handler())
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/debug/pprof/cmdline", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "This profile type has been disabled to avoid leaking private command-line arguments")
	})
	mux.HandleFunc("/debug/config", func(w http.ResponseWriter, r *http.Request) {
		if c == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		json, err := json.MarshalIndent(c.DebugMap(), "", "  ")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "%s", string(json))
	})

	return mux
}

var defaultCodeToLevel = grpclog.WithLevels(func(code codes.Code) grpclog.Level {
	if code == codes.DeadlineExceeded {
		// The server has a deadline set, so we consider it a normal condition.
		// This ensures that we don't log them as errors.
		return grpclog.LevelInfo
	}
	return grpclog.DefaultServerCodeToLevel(code)
})

var dispatchDefaultCodeToLevel = grpclog.WithLevels(func(code codes.Code) grpclog.Level {
	switch code {
	case codes.OK, codes.Canceled:
		return grpclog.LevelDebug
	case codes.NotFound, codes.AlreadyExists, codes.InvalidArgument, codes.Unauthenticated:
		return grpclog.LevelWarn
	default:
		return grpclog.DefaultServerCodeToLevel(code)
	}
})

var durationFieldOption = grpclog.WithDurationField(func(duration time.Duration) grpclog.Fields {
	return grpclog.Fields{"grpc.time_ms", duration.Milliseconds()}
})

var timestampFormatOption = grpclog.WithTimestampFormat(time.RFC3339Nano)

var traceIDFieldOption = grpclog.WithFieldsFromContext(func(ctx context.Context) grpclog.Fields {
	if span := trace.SpanContextFromContext(ctx); span.IsSampled() {
		return grpclog.Fields{"traceID", span.TraceID().String()}
	}
	return nil
})

var alwaysDebugOption = grpclog.WithLevels(func(code codes.Code) grpclog.Level {
	return grpclog.LevelDebug
})

const (
	DefaultMiddlewareRequestID        = "requestid"
	DefaultMiddlewareLog              = "log"
	DefaultMiddlewareGRPCLog          = "grpclog"
	DefaultMiddlewareGRPCAuth         = "grpcauth"
	DefaultMiddlewareGRPCProm         = "grpcprom"
	DefaultMiddlewareServerVersion    = "serverversion"
	DefaultMiddlewareMemoryProtection = "memoryprotection"

	DefaultInternalMiddlewareDispatch          = "dispatch"
	DefaultInternalMiddlewareDatastore         = "datastore"
	DefaultInternalMiddlewareDatastoreCounting = "datastore-counting"
	DefaultInternalMiddlewareConsistency       = "consistency"
	DefaultInternalMiddlewareServerSpecific    = "servicespecific"
)

//go:generate go run github.com/ecordell/optgen -output zz_generated.middlewareoption.go . MiddlewareOption
type MiddlewareOption struct {
	Logger                    zerolog.Logger                       `debugmap:"hidden"`
	AuthFunc                  grpcauth.AuthFunc                    `debugmap:"hidden"`
	EnableVersionResponse     bool                                 `debugmap:"visible"`
	DispatcherForMiddleware   dispatch.Dispatcher                  `debugmap:"hidden"`
	EnableRequestLog          bool                                 `debugmap:"visible"`
	EnableResponseLog         bool                                 `debugmap:"visible"`
	DisableGRPCHistogram      bool                                 `debugmap:"visible"`
	MiddlewareServiceLabel    string                               `debugmap:"visible"`
	MismatchingZedTokenOption consistencymw.MismatchingTokenOption `debugmap:"visible"`

	MemoryUsageProvider memoryprotection.MemoryUsageProvider `debugmap:"hidden"`

	unaryDatastoreMiddleware  *ReferenceableMiddleware[grpc.UnaryServerInterceptor]  `debugmap:"hidden"`
	streamDatastoreMiddleware *ReferenceableMiddleware[grpc.StreamServerInterceptor] `debugmap:"hidden"`
}

type Middleware interface {
	UnaryServerInterceptor() grpc.UnaryServerInterceptor
	StreamServerInterceptor() grpc.StreamServerInterceptor
}

func (m MiddlewareOption) WithDatastoreMiddleware(middleware Middleware) MiddlewareOption {
	unary := NewUnaryMiddleware().
		WithName(DefaultInternalMiddlewareDatastore).
		WithInternal(true).
		WithInterceptor(middleware.UnaryServerInterceptor()).
		Done()

	stream := NewStreamMiddleware().
		WithName(DefaultInternalMiddlewareDatastore).
		WithInternal(true).
		WithInterceptor(middleware.StreamServerInterceptor()).
		Done()

	m.unaryDatastoreMiddleware = &unary
	m.streamDatastoreMiddleware = &stream

	return m
}

func (m MiddlewareOption) WithDatastore(ds datastore.Datastore) MiddlewareOption {
	dl := datalayer.NewDataLayer(ds)
	unary := NewUnaryMiddleware().
		WithName(DefaultInternalMiddlewareDatastore).
		WithInternal(true).
		WithInterceptor(datalayer.UnaryServerInterceptor(dl)).
		Done()

	stream := NewStreamMiddleware().
		WithName(DefaultInternalMiddlewareDatastore).
		WithInternal(true).
		WithInterceptor(datalayer.StreamServerInterceptor(dl)).
		Done()

	m.unaryDatastoreMiddleware = &unary
	m.streamDatastoreMiddleware = &stream

	return m
}

// gRPCMetricsUnaryInterceptor creates the default prometheus metrics interceptor for unary gRPCs
var gRPCMetricsUnaryInterceptor grpc.UnaryServerInterceptor

// gRPCMetricsStreamingInterceptor creates the default prometheus metrics interceptor for streaming gRPCs
var gRPCMetricsStreamingInterceptor grpc.StreamServerInterceptor

var serverMetricsOnce sync.Once

// GRPCMetrics returns the interceptors used for the default gRPC metrics from grpc-ecosystem/go-grpc-middleware
func GRPCMetrics(disableLatencyHistogram bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	serverMetricsOnce.Do(func() {
		gRPCMetricsUnaryInterceptor, gRPCMetricsStreamingInterceptor = createServerMetrics(disableLatencyHistogram)
	})

	return gRPCMetricsUnaryInterceptor, gRPCMetricsStreamingInterceptor
}

const healthCheckRoute = "/grpc.health.v1.Health/Check"

func matchesRoute(route string) func(_ context.Context, c interceptors.CallMeta) bool {
	return func(_ context.Context, c interceptors.CallMeta) bool {
		return c.FullMethod() == route
	}
}

func doesNotMatchRoute(route string) func(_ context.Context, c interceptors.CallMeta) bool {
	return func(_ context.Context, c interceptors.CallMeta) bool {
		return c.FullMethod() != route
	}
}

// DefaultUnaryMiddleware generates the default middleware chain used for the public SpiceDB Unary gRPC methods
func DefaultUnaryMiddleware(opts MiddlewareOption) (*MiddlewareChain[grpc.UnaryServerInterceptor], error) {
	grpcMetricsUnaryInterceptor, _ := GRPCMetrics(opts.DisableGRPCHistogram)
	memoryProtectionUnaryInterceptor := memoryprotection.New(opts.MemoryUsageProvider, "unary-middleware")
	chain, err := NewMiddlewareChain([]ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
		NewUnaryMiddleware().
			WithName(DefaultMiddlewareRequestID).
			WithInterceptor(requestid.UnaryServerInterceptor(requestid.GenerateIfMissing(true))).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareLog).
			WithInterceptor(logmw.UnaryServerInterceptor(logmw.ExtractMetadataField(string(requestmeta.RequestIDKey), "requestID"))).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareGRPCLog + "-debug").
			WithInterceptor(selector.UnaryServerInterceptor(
				grpclog.UnaryServerInterceptor(InterceptorLogger(opts.Logger), determineEventsToLog(opts), alwaysDebugOption, durationFieldOption, timestampFormatOption, traceIDFieldOption),
				selector.MatchFunc(matchesRoute(healthCheckRoute)))).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareGRPCLog).
			WithInterceptor(selector.UnaryServerInterceptor(
				grpclog.UnaryServerInterceptor(InterceptorLogger(opts.Logger), determineEventsToLog(opts), defaultCodeToLevel, durationFieldOption, timestampFormatOption, traceIDFieldOption),
				selector.MatchFunc(doesNotMatchRoute(healthCheckRoute)))).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareGRPCProm).
			WithInterceptor(grpcMetricsUnaryInterceptor).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareMemoryProtection).
			WithInterceptor(selector.UnaryServerInterceptor(
										memoryProtectionUnaryInterceptor.UnaryServerInterceptor(),
										selector.MatchFunc(doesNotMatchRoute(healthCheckRoute)))).
			EnsureAlreadyExecuted(DefaultMiddlewareGRPCProm). // so that prom middleware reports OOM status
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareGRPCAuth).
			WithInterceptor(grpcauth.UnaryServerInterceptor(opts.AuthFunc)).
			EnsureAlreadyExecuted(DefaultMiddlewareGRPCProm). // so that prom middleware reports auth failures
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareServerVersion).
			WithInterceptor(serverversion.UnaryServerInterceptor(opts.EnableVersionResponse)).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultInternalMiddlewareDispatch).
			WithInternal(true).
			WithInterceptor(dispatchmw.UnaryServerInterceptor(opts.DispatcherForMiddleware)).
			Done(),

		*opts.unaryDatastoreMiddleware,

		NewUnaryMiddleware().
			WithName(DefaultInternalMiddlewareDatastoreCounting).
			WithInternal(true).
			WithInterceptor(datalayer.UnaryCountingInterceptor(nil)).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultInternalMiddlewareConsistency).
			WithInterceptor(consistencymw.UnaryServerInterceptor(opts.MiddlewareServiceLabel, opts.MismatchingZedTokenOption)).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultInternalMiddlewareServerSpecific).
			WithInternal(true).
			WithInterceptor(servicespecific.UnaryServerInterceptor).
			Done(),
	}...)
	return &chain, err
}

// DefaultStreamingMiddleware generates the default middleware chain used for the public SpiceDB Streaming gRPC methods
func DefaultStreamingMiddleware(opts MiddlewareOption) (*MiddlewareChain[grpc.StreamServerInterceptor], error) {
	_, grpcMetricsStreamingInterceptor := GRPCMetrics(opts.DisableGRPCHistogram)
	memoryProtectionStreamInterceptor := memoryprotection.New(opts.MemoryUsageProvider, "stream-middleware")
	chain, err := NewMiddlewareChain([]ReferenceableMiddleware[grpc.StreamServerInterceptor]{
		NewStreamMiddleware().
			WithName(DefaultMiddlewareRequestID).
			WithInterceptor(requestid.StreamServerInterceptor(requestid.GenerateIfMissing(true))).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareLog).
			WithInterceptor(logmw.StreamServerInterceptor(logmw.ExtractMetadataField(string(requestmeta.RequestIDKey), "requestID"))).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareGRPCLog + "-debug").
			WithInterceptor(selector.StreamServerInterceptor(
				grpclog.StreamServerInterceptor(InterceptorLogger(opts.Logger), determineEventsToLog(opts), alwaysDebugOption, durationFieldOption, timestampFormatOption, traceIDFieldOption),
				selector.MatchFunc(matchesRoute(healthCheckRoute)))).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareGRPCLog).
			WithInterceptor(selector.StreamServerInterceptor(
				grpclog.StreamServerInterceptor(InterceptorLogger(opts.Logger), determineEventsToLog(opts), defaultCodeToLevel, durationFieldOption, timestampFormatOption, traceIDFieldOption),
				selector.MatchFunc(doesNotMatchRoute(healthCheckRoute)))).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareGRPCProm).
			WithInterceptor(grpcMetricsStreamingInterceptor).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareMemoryProtection).
			WithInterceptor(memoryProtectionStreamInterceptor.StreamServerInterceptor()).
			EnsureInterceptorAlreadyExecuted(DefaultMiddlewareGRPCProm). // so that prom middleware reports OOM backpressure
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareGRPCAuth).
			WithInterceptor(grpcauth.StreamServerInterceptor(opts.AuthFunc)).
			EnsureInterceptorAlreadyExecuted(DefaultMiddlewareGRPCProm). // so that prom middleware reports auth failures
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareServerVersion).
			WithInterceptor(serverversion.StreamServerInterceptor(opts.EnableVersionResponse)).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultInternalMiddlewareDispatch).
			WithInternal(true).
			WithInterceptor(dispatchmw.StreamServerInterceptor(opts.DispatcherForMiddleware)).
			Done(),

		*opts.streamDatastoreMiddleware,

		NewStreamMiddleware().
			WithName(DefaultInternalMiddlewareDatastoreCounting).
			WithInternal(true).
			WithInterceptor(datalayer.StreamCountingInterceptor(nil)).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultInternalMiddlewareConsistency).
			WithInterceptor(consistencymw.StreamServerInterceptor(opts.MiddlewareServiceLabel, opts.MismatchingZedTokenOption)).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultInternalMiddlewareServerSpecific).
			WithInternal(true).
			WithInterceptor(servicespecific.StreamServerInterceptor).
			Done(),
	}...)
	return &chain, err
}

func determineEventsToLog(opts MiddlewareOption) grpclog.Option {
	eventsToLog := []grpclog.LoggableEvent{grpclog.FinishCall}
	if opts.EnableRequestLog {
		eventsToLog = append(eventsToLog, grpclog.PayloadReceived)
	}

	if opts.EnableResponseLog {
		eventsToLog = append(eventsToLog, grpclog.PayloadSent)
	}

	return grpclog.WithLogOnEvents(eventsToLog...)
}

// DefaultDispatchMiddleware generates the default middleware chain used for the internal dispatch SpiceDB gRPC API
func DefaultDispatchMiddleware(logger zerolog.Logger, authFunc grpcauth.AuthFunc, ds datastore.Datastore, disableGRPCLatencyHistogram bool, memoryUsageProvider memoryprotection.MemoryUsageProvider) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	grpcMetricsUnaryInterceptor, grpcMetricsStreamingInterceptor := GRPCMetrics(disableGRPCLatencyHistogram)
	dispatchMemoryProtection := memoryprotection.New(memoryUsageProvider, "dispatch-middleware")
	dl := datalayer.NewDataLayer(ds)

	return []grpc.UnaryServerInterceptor{
			requestid.UnaryServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.UnaryServerInterceptor(logmw.ExtractMetadataField(string(requestmeta.RequestIDKey), "requestID")),
			grpclog.UnaryServerInterceptor(InterceptorLogger(logger), dispatchDefaultCodeToLevel, durationFieldOption, timestampFormatOption, traceIDFieldOption),
			grpcMetricsUnaryInterceptor,
			dispatchMemoryProtection.UnaryServerInterceptor(),
			grpcauth.UnaryServerInterceptor(authFunc),
			datalayer.UnaryServerInterceptor(dl),
			servicespecific.UnaryServerInterceptor,
		}, []grpc.StreamServerInterceptor{
			// NOTE: the logging middlewares are not present here in streaming, to remove their significant overhead
			// when returning streaming messages.
			requestid.StreamServerInterceptor(requestid.GenerateIfMissing(true)),
			grpcMetricsStreamingInterceptor,
			dispatchMemoryProtection.StreamServerInterceptor(),
			grpcauth.StreamServerInterceptor(authFunc),
			datalayer.StreamServerInterceptor(dl),
			servicespecific.StreamServerInterceptor,
		}
}

func InterceptorLogger(l zerolog.Logger) grpclog.Logger {
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

// initializes prometheus grpc interceptors with exemplar support enabled
func createServerMetrics(disableHistogram bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	var opts []grpcprom.ServerMetricsOption
	if !disableHistogram {
		opts = append(opts, grpcprom.WithServerHandlingTimeHistogram(
			grpcprom.WithHistogramOpts(&prometheus.HistogramOpts{
				NativeHistogramBucketFactor:    1.1, // At most 10% increase from bucket to bucket.
				NativeHistogramMaxBucketNumber: 100,
				Buckets: []float64{
					.001, .003, .006, .010, .018, .024, .032, .042, .056, .075, .100, .178, .316, .562, 1, 5,
				},
			}),
		))
	}
	srvMetrics := grpcprom.NewServerMetrics(opts...)
	// deliberately ignore if these metrics were already registered, so that
	// custom builds of SpiceDB can register these metrics with custom labels
	_ = prometheus.Register(srvMetrics)

	exemplarFromContext := func(ctx context.Context) prometheus.Labels {
		if span := trace.SpanContextFromContext(ctx); span.IsSampled() {
			return prometheus.Labels{"traceID": span.TraceID().String()}
		}
		return nil
	}

	exemplarContext := grpcprom.WithExemplarFromContext(exemplarFromContext)
	return srvMetrics.UnaryServerInterceptor(exemplarContext), srvMetrics.StreamServerInterceptor(exemplarContext)
}
