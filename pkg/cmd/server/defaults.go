package server

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/fatih/color"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jzelinskie/cobrautil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/logging"
	consistencymw "github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	dispatchmw "github.com/authzed/spicedb/internal/middleware/dispatcher"
	"github.com/authzed/spicedb/internal/middleware/serverversion"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/pkg/datastore"
	logmw "github.com/authzed/spicedb/pkg/middleware/logging"
	"github.com/authzed/spicedb/pkg/middleware/requestid"
	"github.com/authzed/spicedb/pkg/releases"
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
		cobrautil.SyncViperPreRunE(programName),
		cobrautil.ZeroLogRunE("log", zerolog.InfoLevel),
		cobrautil.OpenTelemetryRunE("otel", zerolog.InfoLevel),
		releases.CheckAndLogRunE(),
	)
}

// MetricsHandler sets up an HTTP server that handles serving Prometheus
// metrics and pprof endpoints.
func MetricsHandler(telemetryRegistry *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	if telemetryRegistry != nil {
		mux.Handle("/telemetry", promhttp.HandlerFor(telemetryRegistry, promhttp.HandlerOpts{}))
	}
	return mux
}

func DefaultMiddleware(logger zerolog.Logger, authFunc grpcauth.AuthFunc, enableVersionResponse bool, dispatcher dispatch.Dispatcher, ds datastore.Datastore) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	return []grpc.UnaryServerInterceptor{
			requestid.UnaryServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.UnaryServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
			grpclog.UnaryServerInterceptor(logging.InterceptorLogger(logger)),
			otelgrpc.UnaryServerInterceptor(),
			grpcauth.UnaryServerInterceptor(authFunc),
			grpcprom.UnaryServerInterceptor,
			dispatchmw.UnaryServerInterceptor(dispatcher),
			datastoremw.UnaryServerInterceptor(ds),
			consistencymw.UnaryServerInterceptor(),
			servicespecific.UnaryServerInterceptor,
			serverversion.UnaryServerInterceptor(enableVersionResponse),
		}, []grpc.StreamServerInterceptor{
			requestid.StreamServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.StreamServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
			grpclog.StreamServerInterceptor(logging.InterceptorLogger(logger)),
			otelgrpc.StreamServerInterceptor(),
			grpcauth.StreamServerInterceptor(authFunc),
			grpcprom.StreamServerInterceptor,
			dispatchmw.StreamServerInterceptor(dispatcher),
			datastoremw.StreamServerInterceptor(ds),
			consistencymw.StreamServerInterceptor(),
			servicespecific.StreamServerInterceptor,
			serverversion.StreamServerInterceptor(enableVersionResponse),
		}
}

func DefaultDispatchMiddleware(logger zerolog.Logger, authFunc grpcauth.AuthFunc, ds datastore.Datastore) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	return []grpc.UnaryServerInterceptor{
			requestid.UnaryServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.UnaryServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
			grpclog.UnaryServerInterceptor(logging.InterceptorLogger(logger)),
			otelgrpc.UnaryServerInterceptor(),
			grpcauth.UnaryServerInterceptor(authFunc),
			grpcprom.UnaryServerInterceptor,
			datastoremw.UnaryServerInterceptor(ds),
			servicespecific.UnaryServerInterceptor,
		}, []grpc.StreamServerInterceptor{
			requestid.StreamServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.StreamServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
			grpclog.StreamServerInterceptor(logging.InterceptorLogger(logger)),
			otelgrpc.StreamServerInterceptor(),
			grpcauth.StreamServerInterceptor(authFunc),
			grpcprom.StreamServerInterceptor,
			datastoremw.StreamServerInterceptor(ds),
			servicespecific.StreamServerInterceptor,
		}
}
