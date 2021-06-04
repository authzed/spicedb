package main

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
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

	configFilePaths := cobrautil.MustGetStringSlice(cmd, "load-configs")
	server := &tokenBasedServer{
		configFilePaths: configFilePaths,
	}

	api.RegisterACLServiceServer(grpcServer, server)
	api.RegisterNamespaceServiceServer(grpcServer, server)
	reflection.Register(grpcServer)

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

type model struct {
	datastore        datastore.Datastore
	namespaceManager namespace.Manager
	dispatcher       graph.Dispatcher
}

type tokenBasedServer struct {
	api.UnimplementedACLServiceServer
	api.UnimplementedNamespaceServiceServer

	configFilePaths []string
	modelByToken    sync.Map
}

func (tbs *tokenBasedServer) modelForContext(ctx context.Context) model {
	tokenStr, _ := grpcauth.AuthFromMD(ctx, "bearer")
	cached, hasModel := tbs.modelByToken.Load(tokenStr)
	if hasModel {
		return cached.(model)
	}

	log.Info().Str("token", tokenStr).Msg("initializing new model for token")
	model := tbs.createModel()
	tbs.modelByToken.Store(tokenStr, model)
	return model
}

func (tbs *tokenBasedServer) nsServer(ctx context.Context) api.NamespaceServiceServer {
	model := tbs.modelForContext(ctx)
	return services.NewNamespaceServer(model.datastore)
}

func (tbs *tokenBasedServer) WriteConfig(ctx context.Context, req *api.WriteConfigRequest) (*api.WriteConfigResponse, error) {
	return tbs.nsServer(ctx).WriteConfig(ctx, req)
}

func (tbs *tokenBasedServer) ReadConfig(ctx context.Context, req *api.ReadConfigRequest) (*api.ReadConfigResponse, error) {
	return tbs.nsServer(ctx).ReadConfig(ctx, req)
}

func (tbs *tokenBasedServer) aclServer(ctx context.Context) api.ACLServiceServer {
	model := tbs.modelForContext(ctx)
	return services.NewACLServer(model.datastore, model.namespaceManager, model.dispatcher, MAX_DEPTH)
}

func (tbs *tokenBasedServer) Read(ctx context.Context, req *api.ReadRequest) (*api.ReadResponse, error) {
	return tbs.aclServer(ctx).Read(ctx, req)
}

func (tbs *tokenBasedServer) Write(ctx context.Context, req *api.WriteRequest) (*api.WriteResponse, error) {
	return tbs.aclServer(ctx).Write(ctx, req)
}

func (tbs *tokenBasedServer) Check(ctx context.Context, req *api.CheckRequest) (*api.CheckResponse, error) {
	return tbs.aclServer(ctx).Check(ctx, req)
}

func (tbs *tokenBasedServer) ContentChangeCheck(ctx context.Context, req *api.ContentChangeCheckRequest) (*api.CheckResponse, error) {
	return tbs.aclServer(ctx).ContentChangeCheck(ctx, req)
}

func (tbs *tokenBasedServer) Expand(ctx context.Context, req *api.ExpandRequest) (*api.ExpandResponse, error) {
	return tbs.aclServer(ctx).Expand(ctx, req)
}

func (tbs *tokenBasedServer) createModel() model {
	ds, err := memdb.NewMemdbDatastore(0, REVISION_FUZZING_DURATION, GC_WINDOW, 0)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to init datastore")
	}

	// Populate the datastore for any configuration files specified.
	_, _, err = validationfile.PopulateFromFiles(ds, tbs.configFilePaths)
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

	return model{ds, nsm, dispatch}
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
