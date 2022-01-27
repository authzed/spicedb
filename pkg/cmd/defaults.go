package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/propagators/ot"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/jzelinskie/cobrautil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
		cobrautil.ZeroLogRunE("log", zerolog.InfoLevel),
		cobrautil.OpenTelemetryRunE("otel", zerolog.InfoLevel),
		customizeGitHubOpenTracing(),
	)
}

// customizeGitHubOpenTracing overrides the OpenTelemetry propogators set in cobrautil
//
// In GitHub's environment there are some legacy OpenTracing clients, so to have complete
// traces propogators need to include OpenTracing. This is not set as default in cobrautil.
// See https://github.com/github/observability/issues/1495 for history and
// https://thehub.github.com/engineering/development-and-ops/observability/distributed-tracing/instrumentation/#golang
func customizeGitHubOpenTracing() cobrautil.CobraRunFunc {
	return func(cmd *cobra.Command, args []string) error {
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.Baggage{},      // W3C baggage support
			propagation.TraceContext{}, // W3C for compatibility with other tracing system
			ot.OT{},
		))
		return nil
	}
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

// SignalContextWithGracePeriod creates a new context that will be cancelled
// when an interrupt/SIGTERM signal is received and the provided grace period
// subsequently finishes.
func SignalContextWithGracePeriod(ctx context.Context, gracePeriod time.Duration) context.Context {
	newCtx, cancelfn := context.WithCancel(ctx)
	go func() {
		signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		<-signalctx.Done()
		log.Info().Msg("received interrupt")

		if gracePeriod > 0 {
			interruptGrace, _ := signal.NotifyContext(context.Background(), os.Interrupt)
			graceTimer := time.NewTimer(gracePeriod)

			log.Info().Stringer("timeout", gracePeriod).Msg("starting shutdown grace period")

			select {
			case <-graceTimer.C:
			case <-interruptGrace.Done():
				log.Warn().Msg("interrupted shutdown grace period")
			}
		}
		log.Info().Msg("shutting down")
		cancelfn()
	}()

	return newCtx
}
