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

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/readonly"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const (
	GC_WINDOW                 = 1 * time.Hour
	NS_CACHE_EXPIRATION       = 0 * time.Minute // No caching
	MAX_DEPTH                 = 50
	REVISION_FUZZING_DURATION = 10 * time.Millisecond
)

func main() {
	rootCmd := &cobra.Command{
		Use:               "zed-testserver",
		Short:             "Authzed local testing server",
		PersistentPreRunE: persistentPreRunE,
	}

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Runs the Authzed local testing server",
		Run:   runTestServer,
	}

	runCmd.Flags().String("grpc-addr", ":50051", "address to listen on for serving gRPC services")
	runCmd.Flags().String("readonly-grpc-addr", ":50052", "address to listen on for serving read-only gRPC services")
	runCmd.Flags().StringSlice("load-configs", []string{}, "configuration yaml files to load")

	rootCmd.AddCommand(runCmd)
	rootCmd.PersistentFlags().String("log-level", "info", "verbosity of logging (trace, debug, info, warn, error, fatal, panic)")
	rootCmd.PersistentFlags().Bool("json", false, "output logs as JSON")

	rootCmd.Execute()
}

type srv struct {
	server     *grpc.Server
	isReadOnly bool
}

func runTestServer(cmd *cobra.Command, args []string) {
	configFilePaths := cobrautil.MustGetStringSlice(cmd, "load-configs")
	tokenServer := &tokenBasedServer{
		configFilePaths: configFilePaths,
		modelByToken:    &sync.Map{},
	}

	grpcServer := grpc.NewServer()
	readonlyService := grpc.NewServer()

	for _, srvInfo := range []srv{{grpcServer, false}, {readonlyService, true}} {
		v0.RegisterACLServiceServer(srvInfo.server, tokenServer.WithReadOnly(srvInfo.isReadOnly))
		v0.RegisterNamespaceServiceServer(srvInfo.server, tokenServer.WithReadOnly(srvInfo.isReadOnly))
		v1alpha1.RegisterSchemaServiceServer(srvInfo.server, tokenServer.WithReadOnly(srvInfo.isReadOnly))
		reflection.Register(srvInfo.server)
	}

	go func() {
		addr := cobrautil.MustGetString(cmd, "grpc-addr")
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal().Str("addr", addr).Msg("failed to listen on addr for gRPC server")
		}

		log.Info().Str("addr", addr).Msg("gRPC server started listening")
		grpcServer.Serve(l)
	}()

	go func() {
		addr := cobrautil.MustGetString(cmd, "readonly-grpc-addr")
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal().Str("addr", addr).Msg("failed to listen on readonly addr for gRPC server")
		}

		log.Info().Str("addr", addr).Msg("readonly gRPC server started listening")
		readonlyService.Serve(l)
	}()

	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
	select {
	case <-signalctx.Done():
		log.Info().Msg("received interrupt")
		grpcServer.GracefulStop()
		readonlyService.GracefulStop()
		return
	}
}

type model struct {
	datastore        datastore.Datastore
	namespaceManager namespace.Manager
	dispatcher       graph.Dispatcher
}

type tokenBasedServer struct {
	v0.UnimplementedACLServiceServer
	v0.UnimplementedNamespaceServiceServer
	v1alpha1.UnimplementedSchemaServiceServer

	configFilePaths []string
	modelByToken    *sync.Map
	isReadOnly      bool
}

func (tbs *tokenBasedServer) modelForContext(ctx context.Context) model {
	tokenStr, _ := grpcauth.AuthFromMD(ctx, "bearer")
	_, hasModel := tbs.modelByToken.Load(tokenStr)
	if !hasModel {
		log.Info().Str("token", tokenStr).Msg("initializing new model for token")
		tbs.modelByToken.Store(tokenStr, tbs.createModel())
	}

	loaded, _ := tbs.modelByToken.Load(tokenStr)
	loadedModel := loaded.(model)
	if tbs.isReadOnly {
		return model{readonly.NewReadonlyDatastore(loadedModel.datastore), loadedModel.namespaceManager, loadedModel.dispatcher}
	}

	return loadedModel
}

func (tbs *tokenBasedServer) schemaServer(ctx context.Context) v1alpha1.SchemaServiceServer {
	model := tbs.modelForContext(ctx)
	return v1alpha1svc.NewSchemaServer(model.datastore, v1alpha1svc.PrefixNotRequired)
}

func (tbs *tokenBasedServer) WriteSchema(ctx context.Context, req *v1alpha1.WriteSchemaRequest) (*v1alpha1.WriteSchemaResponse, error) {
	return tbs.schemaServer(ctx).WriteSchema(ctx, req)
}

func (tbs *tokenBasedServer) ReadSchema(ctx context.Context, req *v1alpha1.ReadSchemaRequest) (*v1alpha1.ReadSchemaResponse, error) {
	return tbs.schemaServer(ctx).ReadSchema(ctx, req)
}

func (tbs *tokenBasedServer) nsServer(ctx context.Context) v0.NamespaceServiceServer {
	model := tbs.modelForContext(ctx)
	return v0svc.NewNamespaceServer(model.datastore)
}

func (tbs *tokenBasedServer) WriteConfig(ctx context.Context, req *v0.WriteConfigRequest) (*v0.WriteConfigResponse, error) {
	return tbs.nsServer(ctx).WriteConfig(ctx, req)
}

func (tbs *tokenBasedServer) ReadConfig(ctx context.Context, req *v0.ReadConfigRequest) (*v0.ReadConfigResponse, error) {
	return tbs.nsServer(ctx).ReadConfig(ctx, req)
}

func (tbs *tokenBasedServer) aclServer(ctx context.Context) v0.ACLServiceServer {
	model := tbs.modelForContext(ctx)
	return v0svc.NewACLServer(model.datastore, model.namespaceManager, model.dispatcher, MAX_DEPTH)
}

func (tbs *tokenBasedServer) Read(ctx context.Context, req *v0.ReadRequest) (*v0.ReadResponse, error) {
	return tbs.aclServer(ctx).Read(ctx, req)
}

func (tbs *tokenBasedServer) Write(ctx context.Context, req *v0.WriteRequest) (*v0.WriteResponse, error) {
	return tbs.aclServer(ctx).Write(ctx, req)
}

func (tbs *tokenBasedServer) Check(ctx context.Context, req *v0.CheckRequest) (*v0.CheckResponse, error) {
	return tbs.aclServer(ctx).Check(ctx, req)
}

func (tbs *tokenBasedServer) ContentChangeCheck(ctx context.Context, req *v0.ContentChangeCheckRequest) (*v0.CheckResponse, error) {
	return tbs.aclServer(ctx).ContentChangeCheck(ctx, req)
}

func (tbs *tokenBasedServer) Expand(ctx context.Context, req *v0.ExpandRequest) (*v0.ExpandResponse, error) {
	return tbs.aclServer(ctx).Expand(ctx, req)
}

func (tbs *tokenBasedServer) Lookup(ctx context.Context, req *v0.LookupRequest) (*v0.LookupResponse, error) {
	return tbs.aclServer(ctx).Lookup(ctx, req)
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

func (tbs *tokenBasedServer) WithReadOnly(isReadOnly bool) *tokenBasedServer {
	if !isReadOnly {
		return tbs
	}

	return &tokenBasedServer{
		configFilePaths: tbs.configFilePaths,
		modelByToken:    tbs.modelByToken,
		isReadOnly:      true,
	}
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
