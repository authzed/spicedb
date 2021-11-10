package main

import (
	"math/rand"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/jzelinskie/cobrautil"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

var defaultPreRunE = cobrautil.CommandStack(
	cobrautil.SyncViperPreRunE("spicedb"),
	cobrautil.ZeroLogPreRunE("log", zerolog.InfoLevel),
	cobrautil.OpenTelemetryPreRunE("otel", zerolog.InfoLevel),
)

func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func main() {
	rand.Seed(time.Now().UnixNano())

	rootCmd := newRootCmd()
	registerVersionCmd(rootCmd)
	registerServeCmd(rootCmd)
	registerMigrateCmd(rootCmd)
	registerHeadCmd(rootCmd)
	registerDeveloperServiceCmd(rootCmd)
	registerTestserverCmd(rootCmd)

	_ = rootCmd.Execute()
}
