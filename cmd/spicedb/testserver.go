package main

import (
	"context"
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
	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services"
	v1alpha1svc "github.com/authzed/spicedb/internal/services/v1alpha1"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const (
	gcWindow                = 1 * time.Hour
	nsCacheExpiration       = 0 * time.Minute // No caching
	maxDepth                = 50
	revisionFuzzingDuration = 10 * time.Millisecond
)

func registerTestserverCmd(rootCmd *cobra.Command) {
	testserveCmd := &cobra.Command{
		Use:               "serve-testing",
		Short:             "test server with an in-memory datastore",
		Long:              "An in-memory spicedb server which serves completely isolated datastores per client-supplied auth token used.",
		PersistentPreRunE: persistentPreRunE,
		Run:               runTestServer,
	}

	testserveCmd.Flags().String("grpc-addr", ":50051", "address to listen on for serving gRPC services")
	testserveCmd.Flags().String("readonly-grpc-addr", ":50052", "address to listen on for serving read-only gRPC services")
	testserveCmd.Flags().StringSlice("load-configs", []string{}, "configuration yaml files to load")

	rootCmd.AddCommand(testserveCmd)
}

func runTestServer(cmd *cobra.Command, args []string) {
	configFilePaths := cobrautil.MustGetStringSlice(cmd, "load-configs")

	readWriteMiddleware := &perTokenBackendMiddleware{
		&sync.Map{},
		configFilePaths,
		true,
	}
	readOnlyMiddleware := &perTokenBackendMiddleware{
		&sync.Map{},
		configFilePaths,
		true,
	}

	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		readWriteMiddleware.UnaryServerInterceptor(),
	), grpc.ChainStreamInterceptor(
		readWriteMiddleware.StreamServerInterceptor(),
	))
	readonlyServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		readOnlyMiddleware.UnaryServerInterceptor(),
	), grpc.ChainStreamInterceptor(
		readOnlyMiddleware.StreamServerInterceptor(),
	))

	for _, srv := range []*grpc.Server{grpcServer, readonlyServer} {
		v0.RegisterACLServiceServer(srv, &dummyBackend{})
		v0.RegisterNamespaceServiceServer(srv, &dummyBackend{})
		v1alpha1.RegisterSchemaServiceServer(srv, &dummyBackend{})
		v1.RegisterSchemaServiceServer(srv, &v1DummyBackend{})
		v1.RegisterPermissionsServiceServer(srv, &v1DummyBackend{})
		reflection.Register(srv)
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
		readonlyServer.Serve(l)
	}()

	signalctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

	<-signalctx.Done()

	log.Info().Msg("received interrupt")
	grpcServer.GracefulStop()
	readonlyServer.GracefulStop()
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
	isReadOnly      bool
}

type upstream map[string]interface{}

var bypassServiceWhitelist = map[string]struct{}{
	"/grpc.reflection.v1alpha.ServerReflection/": {},
}

func (ptbm *perTokenBackendMiddleware) methodForContextAndName(ctx context.Context, grpcMethodName string) (reflect.Value, error) {
	// If this would have returned an error, we use the zero value of "" to
	// create an isolated test server with no auth token required.
	tokenStr, _ := grpcauth.AuthFromMD(ctx, "bearer")
	untypedUpstream, hasUpstream := ptbm.upstreamByToken.Load(tokenStr)
	if !hasUpstream {
		log.Info().Str("token", tokenStr).Msg("initializing new upstream for token")
		var err error
		untypedUpstream, err = ptbm.createUpstream(ptbm.isReadOnly)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("unable to initialize upstream: %w", err)
		}
		ptbm.upstreamByToken.Store(tokenStr, untypedUpstream)
	}

	upstream := untypedUpstream.(upstream)

	serviceName, methodName := splitMethodName(grpcMethodName)

	client, ok := upstream[serviceName]
	if !ok {
		return reflect.Value{}, fmt.Errorf("unknown service name: %s", serviceName)
	}

	clientHandle := reflect.ValueOf(client)
	clientMethod := clientHandle.MethodByName(methodName)

	return clientMethod, nil
}

func (ptbm *perTokenBackendMiddleware) createUpstream(readonly bool) (upstream, error) {
	ds, err := memdb.NewMemdbDatastore(0, revisionFuzzingDuration, gcWindow, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to init datastore: %w", err)
	}

	// Populate the datastore for any configuration files specified.
	_, _, err = validationfile.PopulateFromFiles(ds, ptbm.configFilePaths)
	if err != nil {
		return nil, fmt.Errorf("failed to load config files: %w", err)
	}

	nsm, err := namespace.NewCachingNamespaceManager(ds, nsCacheExpiration, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize namespace manager: %w", err)
	}

	dispatch := graph.NewLocalOnlyDispatcher(nsm, ds)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(servicespecific.UnaryServerInterceptor),
		grpc.StreamInterceptor(servicespecific.StreamServerInterceptor),
	)

	services.RegisterGrpcServices(grpcServer,
		ds,
		nsm,
		dispatch,
		maxDepth,
		v1alpha1svc.PrefixNotRequired,
		services.V1SchemaServiceEnabled,
	)

	l := bufconn.Listen(1024 * 1024)
	go grpcServer.Serve(l)

	conn, err := grpc.DialContext(
		context.Background(),
		"",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return l.Dial()
		}),
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating client for new upstream: %w", err)
	}

	v1.NewPermissionsServiceClient(conn)

	return map[string]interface{}{
		"authzed.api.v0.ACLService":          v0.NewACLServiceClient(conn),
		"authzed.api.v0.NamespaceService":    v0.NewNamespaceServiceClient(conn),
		"authzed.api.v1alpha1.SchemaService": v1alpha1.NewSchemaServiceClient(conn),
		"authzed.api.v1.SchemaService":       v1.NewSchemaServiceClient(conn),
		"authzed.api.v1.PermissionsService":  v1.NewPermissionsServiceClient(conn),
	}, nil
}

// UnaryServerInterceptor returns a new unary server interceptor that performs per-request exchange of
// the specified consistency configuration for the revision at which to perform the request.
func (ptbm *perTokenBackendMiddleware) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(ctx, req)
			}
		}

		clientMethod, err := ptbm.methodForContextAndName(ctx, info.FullMethod)
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
func (ptbm *perTokenBackendMiddleware) StreamServerInterceptor() grpc.StreamServerInterceptor {
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

		clientMethod, err := ptbm.methodForContextAndName(ctx, info.FullMethod)
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
		if err == io.EOF {
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
