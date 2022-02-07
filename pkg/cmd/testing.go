package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	"github.com/authzed/grpcutil"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const (
	gcWindow                = 1 * time.Hour
	nsCacheExpiration       = 0 * time.Minute // No caching
	maxDepth                = 50
	revisionFuzzingDuration = 10 * time.Millisecond
)

func RegisterTestingFlags(cmd *cobra.Command) {
	cobrautil.RegisterGrpcServerFlags(cmd.Flags(), "grpc", "gRPC", ":50051", true)
	cobrautil.RegisterGrpcServerFlags(cmd.Flags(), "readonly-grpc", "read-only gRPC", ":50052", true)
	cmd.Flags().StringSlice("load-configs", []string{}, "configuration yaml files to load")
}

func NewTestingCommand(programName string) *cobra.Command {
	return &cobra.Command{
		Use:     "serve-testing",
		Short:   "test server with an in-memory datastore",
		Long:    "An in-memory spicedb server which serves completely isolated datastores per client-supplied auth token used.",
		PreRunE: server.DefaultPreRunE(programName),
		RunE:    runTestServer,
	}
}

func runTestServer(cmd *cobra.Command, args []string) error {
	configFilePaths := cobrautil.MustGetStringSliceExpanded(cmd, "load-configs")

	backendMiddleware := &perTokenBackendMiddleware{
		&sync.Map{},
		configFilePaths,
	}

	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		backendMiddleware.UnaryServerInterceptor(false),
	), grpc.ChainStreamInterceptor(
		backendMiddleware.StreamServerInterceptor(false),
	))
	readonlyServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		backendMiddleware.UnaryServerInterceptor(true),
	), grpc.ChainStreamInterceptor(
		backendMiddleware.StreamServerInterceptor(true),
	))

	healthSrv := grpcutil.NewAuthlessHealthServer()

	for _, srv := range []*grpc.Server{grpcServer, readonlyServer} {
		v0.RegisterACLServiceServer(srv, &dummyBackend{})
		healthSrv.SetServicesHealthy(&v0.ACLService_ServiceDesc)
		v0.RegisterNamespaceServiceServer(srv, &dummyBackend{})
		healthSrv.SetServicesHealthy(&v0.NamespaceService_ServiceDesc)
		v1alpha1.RegisterSchemaServiceServer(srv, &dummyBackend{})
		healthSrv.SetServicesHealthy(&v1alpha1.SchemaService_ServiceDesc)
		v1.RegisterSchemaServiceServer(srv, &v1DummyBackend{})
		healthSrv.SetServicesHealthy(&v1.SchemaService_ServiceDesc)
		v1.RegisterPermissionsServiceServer(srv, &v1DummyBackend{})
		healthSrv.SetServicesHealthy(&v1.PermissionsService_ServiceDesc)
		healthpb.RegisterHealthServer(srv, healthSrv)
		reflection.Register(srv)
	}

	go func() {
		if err := cobrautil.GrpcListenFromFlags(cmd, "grpc", grpcServer, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed to start gRPC server")
		}
	}()

	go func() {
		if err := cobrautil.GrpcListenFromFlags(cmd, "readonly-grpc", readonlyServer, zerolog.InfoLevel); err != nil {
			log.Fatal().Err(err).Msg("failed to start gRPC server")
		}
	}()

	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	<-signalctx.Done()

	log.Info().Msg("received interrupt")
	grpcServer.GracefulStop()
	readonlyServer.GracefulStop()

	return nil
}

type dummyBackend struct {
	v0.UnimplementedACLServiceServer
	v0.UnimplementedNamespaceServiceServer
	v1alpha1.UnimplementedSchemaServiceServer
}

type v1DummyBackend struct {
	v1.UnimplementedSchemaServiceServer
	v1.UnimplementedPermissionsServiceServer
}

type perTokenBackendMiddleware struct {
	upstreamByToken *sync.Map
	configFilePaths []string
}

type upstream struct {
	readonlyClients  map[string]interface{}
	readwriteClients map[string]interface{}
}

var bypassServiceWhitelist = map[string]struct{}{
	"/grpc.reflection.v1alpha.ServerReflection/": {},
	"/grpc.health.v1.Health/":                    {},
}

func (ptbm *perTokenBackendMiddleware) methodForContextAndName(ctx context.Context, grpcMethodName string, forReadonly bool) (reflect.Value, error) {
	// If this would have returned an error, we use the zero value of "" to
	// create an isolated test server with no auth token required.
	tokenStr, _ := grpcauth.AuthFromMD(ctx, "bearer")
	untypedUpstream, hasUpstream := ptbm.upstreamByToken.Load(tokenStr)
	if !hasUpstream {
		log.Info().Str("token", tokenStr).Msg("initializing new upstream for token")
		var err error
		untypedUpstream, err = ptbm.createUpstream()
		if err != nil {
			return reflect.Value{}, fmt.Errorf("unable to initialize upstream: %w", err)
		}
		ptbm.upstreamByToken.Store(tokenStr, untypedUpstream)
	}

	allClients := untypedUpstream.(*upstream)

	serviceName, methodName := splitMethodName(grpcMethodName)

	client, ok := allClients.readwriteClients[serviceName]
	if forReadonly {
		client, ok = allClients.readonlyClients[serviceName]
	}
	if !ok {
		return reflect.Value{}, fmt.Errorf("unknown service name: %s", serviceName)
	}

	clientHandle := reflect.ValueOf(client)
	clientMethod := clientHandle.MethodByName(methodName)

	return clientMethod, nil
}

type datastoreInfo struct {
	ds         datastore.Datastore
	isReadonly bool
}

func (ptbm *perTokenBackendMiddleware) createUpstream() (*upstream, error) {
	readwriteDS, err := memdb.NewMemdbDatastore(0, revisionFuzzingDuration, gcWindow, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to init datastore: %w", err)
	}

	// Populate the datastore for any configuration files specified.
	_, _, err = validationfile.PopulateFromFiles(readwriteDS, ptbm.configFilePaths)
	if err != nil {
		return nil, fmt.Errorf("failed to load config files: %w", err)
	}

	readonlyDS := proxy.NewReadonlyDatastore(readwriteDS)

	allClients := &upstream{}
	for _, dsInfo := range []datastoreInfo{{readwriteDS, false}, {readonlyDS, true}} {
		ds := dsInfo.ds

		nsm, err := namespace.NewCachingNamespaceManager(nsCacheExpiration, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize namespace manager: %w", err)
		}

		dispatch := graph.NewLocalOnlyDispatcher(nsm, ds)

		grpcServer := grpc.NewServer(
			grpc.ChainUnaryInterceptor(
				datastoremw.UnaryServerInterceptor(ds),
				consistency.UnaryServerInterceptor(),
				servicespecific.UnaryServerInterceptor,
			),
			grpc.ChainStreamInterceptor(
				datastoremw.StreamServerInterceptor(ds),
				consistency.StreamServerInterceptor(),
				servicespecific.StreamServerInterceptor,
			),
		)

		services.RegisterGrpcServices(
			grpcServer,
			nsm,
			dispatch,
			maxDepth,
			v1alpha1svc.PrefixNotRequired,
			services.V1SchemaServiceEnabled,
		)

		l := bufconn.Listen(1024 * 1024)
		go func() {
			if err := grpcServer.Serve(l); err != nil {
				log.Warn().Err(err).Msg("proxy gRPC service did not shutdown cleanly")
			}
		}()

		conn, err := grpc.DialContext(
			context.Background(),
			"",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return l.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("error creating client for new upstream: %w", err)
		}

		clients := map[string]interface{}{
			"authzed.api.v0.ACLService":          v0.NewACLServiceClient(conn),
			"authzed.api.v0.NamespaceService":    v0.NewNamespaceServiceClient(conn),
			"authzed.api.v1alpha1.SchemaService": v1alpha1.NewSchemaServiceClient(conn),
			"authzed.api.v1.SchemaService":       v1.NewSchemaServiceClient(conn),
			"authzed.api.v1.PermissionsService":  v1.NewPermissionsServiceClient(conn),
		}

		if dsInfo.isReadonly {
			allClients.readonlyClients = clients
		} else {
			allClients.readwriteClients = clients
		}
	}

	return allClients, nil
}

// UnaryServerInterceptor returns a new unary server interceptor that performs per-request exchange of
// the specified consistency configuration for the revision at which to perform the request.
func (ptbm *perTokenBackendMiddleware) UnaryServerInterceptor(forReadonly bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(ctx, req)
			}
		}

		clientMethod, err := ptbm.methodForContextAndName(ctx, info.FullMethod, forReadonly)
		if err != nil {
			return nil, err
		}

		log.Trace().Str("method", info.FullMethod).Msg("receieved unary request")

		results := clientMethod.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)})
		result := results[0].Interface()

		errArg := results[1]

		if errArg.IsNil() {
			err = nil
		} else {
			err = errArg.Interface().(error)
		}

		return result, err
	}
}

// StreamServerInterceptor returns a new stream server interceptor that performs per-request exchange of
// the specified consistency configuration for the revision at which to perform the request.
func (ptbm *perTokenBackendMiddleware) StreamServerInterceptor(forReadonly bool) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(srv, stream)
			}
		}

		ctx := stream.Context()

		if info.IsClientStream {
			panic(fmt.Sprintf("client streaming unsupported for method: %s", info.FullMethod))
		}

		clientMethod, err := ptbm.methodForContextAndName(ctx, info.FullMethod, forReadonly)
		if err != nil {
			return err
		}

		protoType := clientMethod.Type().In(1)
		req := reflect.New(protoType.Elem()).Interface().(proto.Message)

		// We must construct the actual proto type here
		if err := stream.RecvMsg(req); err != nil {
			return fmt.Errorf("unable to recv method request into proto: %w", err)
		}

		log.Trace().Str("method", info.FullMethod).Msg("receieved stream request")

		results := clientMethod.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)})
		result := results[0].Interface().(grpc.ClientStream)

		errArg := results[1]
		if !errArg.IsNil() {
			return errArg.Interface().(error)
		}

		return copyStream(result, stream)
	}
}

// TODO move this to grpcutil
func splitMethodName(fullMethodName string) (string, string) {
	components := strings.Split(fullMethodName, "/")
	numComponents := len(components)
	return components[numComponents-2], components[numComponents-1]
}

// TODO move to grpcutil
func copyStream(in grpc.ClientStream, out grpc.ServerStream) error {
	for {
		// It appears that it doesn't matter what kind of proto this actually is
		message := &v1.CheckPermissionResponse{}
		err := in.RecvMsg(message)
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err
		}

		if err := out.SendMsg(message); err != nil {
			return err
		}
	}
}
