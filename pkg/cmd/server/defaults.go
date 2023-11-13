package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/fatih/color"
	"github.com/go-logr/zerologr"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobraotel"
	"github.com/jzelinskie/cobrautil/v2/cobrazerolog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/logging"
	consistencymw "github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	dispatchmw "github.com/authzed/spicedb/internal/middleware/dispatcher"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/pkg/datastore"
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
		%[3]s serve --grpc-preshared-key "realkeyhere" --grpc-tls-cert-path path/to/tls/cert --grpc-tls-key-path path/to/tls/key \
			--http-tls-cert-path path/to/tls/cert --http-tls-key-path path/to/tls/key \
			--datastore-engine postgres --datastore-conn-uri "postgres-connection-string-here"
`,
		color.YellowString("No TLS and in-memory"),
		color.GreenString("TLS and a real datastore"),
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

	mux.Handle("/metrics", promhttp.Handler())
	if telemetryRegistry != nil {
		mux.Handle("/telemetry", promhttp.HandlerFor(telemetryRegistry, promhttp.HandlerOpts{}))
	}

	mux.HandleFunc("/debug/pprof/", pprof.Index)
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

var defaultGRPCLogOptions = []grpclog.Option{
	// the server has a deadline set, so we consider it a normal condition
	// this makes sure we don't log them as errors
	grpclog.WithLevels(func(code codes.Code) grpclog.Level {
		if code == codes.DeadlineExceeded {
			return grpclog.LevelInfo
		}
		return grpclog.DefaultServerCodeToLevel(code)
	}),
	grpclog.WithDurationField(func(duration time.Duration) grpclog.Fields {
		return grpclog.Fields{"grpc.time_ms", duration.Milliseconds()}
	}),
}

const (
	DefaultMiddlewareRequestID     = "requestid"
	DefaultMiddlewareLog           = "log"
	DefaultMiddlewareGRPCLog       = "grpclog"
	DefaultMiddlewareOTelGRPC      = "otelgrpc"
	DefaultMiddlewareGRPCAuth      = "grpcauth"
	DefaultMiddlewareGRPCProm      = "grpcprom"
	DefaultMiddlewareServerVersion = "serverversion"

	DefaultInternalMiddlewareDispatch       = "dispatch"
	DefaultInternalMiddlewareDatastore      = "datastore"
	DefaultInternalMiddlewareConsistency    = "consistency"
	DefaultInternalMiddlewareServerSpecific = "servicespecific"
)

type MiddlewareOption struct {
	logger                zerolog.Logger
	authFunc              grpcauth.AuthFunc
	enableVersionResponse bool
	dispatcher            dispatch.Dispatcher
	ds                    datastore.Datastore
	enableRequestLog      bool
	enableResponseLog     bool
}

// DefaultUnaryMiddleware generates the default middleware chain used for the public SpiceDB Unary gRPC methods
func DefaultUnaryMiddleware(opts MiddlewareOption) (*MiddlewareChain[grpc.UnaryServerInterceptor], error) {
	chain, err := NewMiddlewareChain([]ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
		NewUnaryMiddleware().
			WithName(DefaultMiddlewareRequestID).
			WithInterceptor(requestid.UnaryServerInterceptor(requestid.GenerateIfMissing(true))).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareLog).
			WithInterceptor(logmw.UnaryServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID"))).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareGRPCLog).
			WithInterceptor(grpclog.UnaryServerInterceptor(InterceptorLogger(opts.logger), determineEventsToLog(opts)...)).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareOTelGRPC).
			WithInterceptor(otelgrpc.UnaryServerInterceptor()). // nolint: staticcheck
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareGRPCProm).
			WithInterceptor(grpcprom.UnaryServerInterceptor).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareGRPCAuth).
			WithInterceptor(grpcauth.UnaryServerInterceptor(opts.authFunc)).
			EnsureAlreadyExecuted(DefaultMiddlewareGRPCProm). // so that prom middleware reports auth failures
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultMiddlewareServerVersion).
			WithInterceptor(serverversion.UnaryServerInterceptor(opts.enableVersionResponse)).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultInternalMiddlewareDispatch).
			WithInternal(true).
			WithInterceptor(dispatchmw.UnaryServerInterceptor(opts.dispatcher)).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultInternalMiddlewareDatastore).
			WithInternal(true).
			WithInterceptor(datastoremw.UnaryServerInterceptor(opts.ds)).
			Done(),

		NewUnaryMiddleware().
			WithName(DefaultInternalMiddlewareConsistency).
			WithInternal(true).
			WithInterceptor(consistencymw.UnaryServerInterceptor()).
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
	chain, err := NewMiddlewareChain([]ReferenceableMiddleware[grpc.StreamServerInterceptor]{
		NewStreamMiddleware().
			WithName(DefaultMiddlewareRequestID).
			WithInterceptor(requestid.StreamServerInterceptor(requestid.GenerateIfMissing(true))).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareLog).
			WithInterceptor(logmw.StreamServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID"))).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareGRPCLog).
			WithInterceptor(grpclog.StreamServerInterceptor(InterceptorLogger(opts.logger), determineEventsToLog(opts)...)).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareOTelGRPC).
			WithInterceptor(otelgrpc.StreamServerInterceptor()). // nolint: staticcheck
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareGRPCProm).
			WithInterceptor(grpcprom.StreamServerInterceptor).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareGRPCAuth).
			WithInterceptor(grpcauth.StreamServerInterceptor(opts.authFunc)).
			EnsureInterceptorAlreadyExecuted(DefaultMiddlewareGRPCProm). // so that prom middleware reports auth failures
			Done(),

		NewStreamMiddleware().
			WithName(DefaultMiddlewareServerVersion).
			WithInterceptor(serverversion.StreamServerInterceptor(opts.enableVersionResponse)).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultInternalMiddlewareDispatch).
			WithInternal(true).
			WithInterceptor(dispatchmw.StreamServerInterceptor(opts.dispatcher)).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultInternalMiddlewareDatastore).
			WithInternal(true).
			WithInterceptor(datastoremw.StreamServerInterceptor(opts.ds)).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultInternalMiddlewareConsistency).
			WithInternal(true).
			WithInterceptor(consistencymw.StreamServerInterceptor()).
			Done(),

		NewStreamMiddleware().
			WithName(DefaultInternalMiddlewareServerSpecific).
			WithInternal(true).
			WithInterceptor(servicespecific.StreamServerInterceptor).
			Done(),
	}...)
	return &chain, err
}

func determineEventsToLog(opts MiddlewareOption) []grpclog.Option {
	eventsToLog := []grpclog.LoggableEvent{grpclog.FinishCall}
	if opts.enableRequestLog {
		eventsToLog = append(eventsToLog, grpclog.PayloadReceived)
	}

	if opts.enableResponseLog {
		eventsToLog = append(eventsToLog, grpclog.PayloadSent)
	}

	logOnEvents := grpclog.WithLogOnEvents(eventsToLog...)
	grpcLogOptions := append(defaultGRPCLogOptions, logOnEvents)

	return grpcLogOptions
}

// DefaultDispatchMiddleware generates the default middleware chain used for the internal dispatch SpiceDB gRPC API
func DefaultDispatchMiddleware(logger zerolog.Logger, authFunc grpcauth.AuthFunc, ds datastore.Datastore) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	return []grpc.UnaryServerInterceptor{
			requestid.UnaryServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.UnaryServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
			grpclog.UnaryServerInterceptor(InterceptorLogger(logger), defaultGRPCLogOptions...),
			otelgrpc.UnaryServerInterceptor(), // nolint: staticcheck
			grpcprom.UnaryServerInterceptor,
			grpcauth.UnaryServerInterceptor(authFunc),
			datastoremw.UnaryServerInterceptor(ds),
			servicespecific.UnaryServerInterceptor,
		}, []grpc.StreamServerInterceptor{
			requestid.StreamServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.StreamServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
			grpclog.StreamServerInterceptor(InterceptorLogger(logger), defaultGRPCLogOptions...),
			otelgrpc.StreamServerInterceptor(), // nolint: staticcheck
			grpcprom.StreamServerInterceptor,
			grpcauth.StreamServerInterceptor(authFunc),
			datastoremw.StreamServerInterceptor(ds),
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
