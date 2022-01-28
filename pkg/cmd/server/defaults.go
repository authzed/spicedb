package server

import (
	"fmt"
	"net/http"
	"net/http/pprof"

	"github.com/fatih/color"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpczerolog "github.com/grpc-ecosystem/go-grpc-middleware/providers/zerolog/v2"
	grpclog "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jzelinskie/cobrautil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/auth"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	dispatchmw "github.com/authzed/spicedb/internal/middleware/dispatcher"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	logmw "github.com/authzed/spicedb/pkg/middleware/logging"
	"github.com/authzed/spicedb/pkg/middleware/requestid"
)

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
		cobrautil.ZeroLogPreRunE("log", zerolog.InfoLevel),
		cobrautil.OpenTelemetryPreRunE("otel", zerolog.InfoLevel),
	)
}

// MetricsHandler sets up an HTTP server that handles serving Prometheus
// metrics and pprof endpoints.
func MetricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func DefaultMiddleware(logger zerolog.Logger, presharedKey string, dispatcher dispatch.Dispatcher, ds datastore.Datastore) ([]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor) {
	return []grpc.UnaryServerInterceptor{
			requestid.UnaryServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.UnaryServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
			grpclog.UnaryServerInterceptor(grpczerolog.InterceptorLogger(logger)),
			otelgrpc.UnaryServerInterceptor(),
			grpcauth.UnaryServerInterceptor(auth.RequirePresharedKey(presharedKey)),
			grpcprom.UnaryServerInterceptor,
			dispatchmw.UnaryServerInterceptor(dispatcher),
			datastoremw.UnaryServerInterceptor(ds),
			servicespecific.UnaryServerInterceptor,
		}, []grpc.StreamServerInterceptor{
			requestid.StreamServerInterceptor(requestid.GenerateIfMissing(true)),
			logmw.StreamServerInterceptor(logmw.ExtractMetadataField("x-request-id", "requestID")),
			grpclog.StreamServerInterceptor(grpczerolog.InterceptorLogger(logger)),
			otelgrpc.StreamServerInterceptor(),
			grpcauth.StreamServerInterceptor(auth.RequirePresharedKey(presharedKey)),
			grpcprom.StreamServerInterceptor,
			dispatchmw.StreamServerInterceptor(dispatcher),
			datastoremw.StreamServerInterceptor(ds),
			servicespecific.StreamServerInterceptor,
		}
}
