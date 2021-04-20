package main

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const GC_WINDOW = 1 * time.Hour
const NS_CACHE_EXPIRATION = 0 * time.Minute // No caching
const MAX_DEPTH = 50
const REVISION_FUZZING_DURATION = 10 * time.Millisecond

func main() {
	var rootCmd = &cobra.Command{
		Use:               "zed-testserver",
		Short:             "Authzed local testing server",
		PersistentPreRunE: persistentPreRunE,
	}

	var runCmd = &cobra.Command{
		Use:   "run",
		Short: "Runs the Authzed local testing server",
		Run:   runTestServer,
	}

	runCmd.Flags().String("grpc-addr", ":50051", "address to listen on for serving gRPC services")
	runCmd.Flags().StringSlice("load-configs", []string{}, "configuration yaml files to load")

	rootCmd.AddCommand(runCmd)
	rootCmd.PersistentFlags().String("log-level", "info", "verbosity of logging (trace, debug, info, warn, error, fatal, panic)")
	rootCmd.PersistentFlags().Bool("json", false, "output logs as JSON")

	rootCmd.Execute()
}

func runTestServer(cmd *cobra.Command, args []string) {
	grpcServer := grpc.NewServer()

	ds, err := memdb.NewMemdbDatastore(0, REVISION_FUZZING_DURATION, GC_WINDOW, 0)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init datastore")
	}

	// Populate the datastore for any configuration files specified.
	err = validationfile.PopulateFromFiles(ds, cobrautil.MustGetStringSlice(cmd, "load-configs"))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config files")
	}

	nsm, err := namespace.NewCachingNamespaceManager(ds, NS_CACHE_EXPIRATION, nil)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize namespace manager")
	}

	dispatch, err := graph.NewLocalDispatcher(nsm, ds)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize check dispatcher")
	}

	RegisterGrpcServices(grpcServer, ds, nsm, dispatch, MAX_DEPTH)

	go func() {
		addr := cobrautil.MustGetString(cmd, "grpc-addr")
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal().Str("addr", addr).Msg("failed to listen on addr for gRPC server")
		}

		log.Info().Str("addr", addr).Msg("gRPC server started listening")
		grpcServer.Serve(l)
	}()

	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	select {
	case <-signalctx.Done():
		log.Info().Msg("received interrupt")
		grpcServer.GracefulStop()
		return
	}
}

func RegisterGrpcServices(
	srv *grpc.Server,
	ds datastore.Datastore,
	nsm namespace.Manager,
	dispatch graph.Dispatcher,
	maxDepth uint16,
) {
	api.RegisterACLServiceServer(srv, services.NewACLServer(ds, nsm, dispatch, maxDepth))
	api.RegisterNamespaceServiceServer(srv, services.NewNamespaceServer(ds))
	reflection.Register(srv)
}

func persistentPreRunE(cmd *cobra.Command, args []string) error {
	if err := cobrautil.SyncViperPreRunE("zed_testserver")(cmd, args); err != nil {
		return err
	}

	if !cobrautil.MustGetBool(cmd, "json") && terminal.IsTerminal(int(os.Stdout.Fd())) {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	level := strings.ToLower(cobrautil.MustGetString(cmd, "log-level"))
	switch level {
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
		return errors.New("unknown log level")
	}
	log.Info().Str("new level", level).Msg("set log level")

	return nil
}
