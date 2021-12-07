package cmd

import (
	"net/http"
	"net/http/pprof"

	"github.com/jzelinskie/cobrautil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

func DefaultPreRunE(programName string) cobrautil.CobraRunFunc {
	return cobrautil.CommandStack(
		cobrautil.SyncViperPreRunE(programName),
		cobrautil.ZeroLogPreRunE("log", zerolog.InfoLevel),
		cobrautil.OpenTelemetryPreRunE("otel", zerolog.InfoLevel),
	)
}

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
