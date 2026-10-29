// Package inprocess runs the SpiceDB testing server in the current process, without networking.
package inprocess

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/grpchelpers"
	"github.com/authzed/spicedb/internal/middleware/pertoken"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	"github.com/authzed/spicedb/internal/services/health"
	v1svc "github.com/authzed/spicedb/internal/services/v1"
	"github.com/authzed/spicedb/internal/testingserver"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	consistencymw "github.com/authzed/spicedb/pkg/middleware/consistency"
)

const listenerBufferSize = 1024 * 1024

// callableServices are the services whose methods can be invoked through Session.Call. The Watch
// service is excluded since its calls never complete on their own.
var callableServices = map[string]struct{}{
	"authzed.api.v1.PermissionsService":  {},
	"authzed.api.v1.SchemaService":       {},
	"authzed.api.v1.ExperimentalService": {},
}

// Server is the testing server run by `serve-testing`, served over an in-memory connection in the
// current process. It requires no networking, which allows it to run in environments such as WASI.
//
// It registers the same services, with the same configuration, as `serve-testing`. Of the
// middleware, it keeps only what affects API behavior (the per-token datastore, consistency and
// service-specific middleware), and omits what is operational (logging, metrics, request IDs,
// memory protection and auth). Avoiding the full server keeps the datastore drivers and telemetry
// exporters out of the build.
//
// Each Session has its own isolated datastore, just as each bearer token does in `serve-testing`.
type Server struct {
	srv                 *grpc.Server
	conn                *grpc.ClientConn
	dispatcher          dispatch.Dispatcher
	datastoreMiddleware *pertoken.MiddlewareForTesting
	nextToken           atomic.Uint64
}

// NewServer starts an in-process testing server with the default `serve-testing` settings. A nil
// caveat type set uses the default type set.
func NewServer(caveatTypeSet *caveattypes.TypeSet) (*Server, error) {
	cts := caveattypes.TypeSetOrDefault(caveatTypeSet)

	dispatcher, err := testingserver.NewDispatcher(cts)
	if err != nil {
		return nil, fmt.Errorf("failed to create dispatcher: %w", err)
	}

	datastoreMiddleware := pertoken.NewMiddleware(nil, cts)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			datastoreMiddleware.UnaryServerInterceptor(),
			consistencymw.UnaryServerInterceptor(testingserver.ConsistencyServiceLabel, testingserver.MismatchingZedTokenOption),
			servicespecific.UnaryServerInterceptor,
		),
		grpc.ChainStreamInterceptor(
			datastoreMiddleware.StreamServerInterceptor(),
			consistencymw.StreamServerInterceptor(testingserver.ConsistencyServiceLabel, testingserver.MismatchingZedTokenOption),
			servicespecific.StreamServerInterceptor,
		),
	)
	healthManager := health.NewHealthManager(dispatcher, datastoreReady{})
	metrics := v1svc.NewMetrics(prometheus.NewRegistry())
	testingserver.RegisterServices(srv, healthManager, dispatcher, testingserver.DefaultLimits(), cts, metrics)

	listener := bufconn.Listen(listenerBufferSize)
	go func() {
		_ = srv.Serve(listener)
	}()

	conn, err := grpchelpers.NewBufferedClient(listener)
	if err != nil {
		srv.Stop()
		_ = dispatcher.Close()
		return nil, err
	}

	return &Server{
		srv:                 srv,
		conn:                conn,
		dispatcher:          dispatcher,
		datastoreMiddleware: datastoreMiddleware,
	}, nil
}

// Close stops the server and releases all sessions.
func (p *Server) Close() error {
	connErr := p.conn.Close()
	p.srv.Stop()
	return errors.Join(connErr, p.dispatcher.Close())
}

// NewSession returns a new session with its own empty datastore.
func (p *Server) NewSession() *Session {
	return &Session{
		server: p,
		token:  "session-" + strconv.FormatUint(p.nextToken.Add(1), 10),
	}
}

type datastoreReady struct{}

func (datastoreReady) ReadyState(context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{IsReady: true}, nil
}

// Session is an isolated datastore within a Server, against which API methods can be
// invoked.
type Session struct {
	server *Server
	token  string
}

// Close discards the session's datastore.
func (s *Session) Close() error {
	return s.server.datastoreMiddleware.DropDatastore(s.token)
}

// Call invokes an API method, named by its full gRPC method name (for example
// "/authzed.api.v1.PermissionsService/CheckPermission"), with a serialized request message, and
// returns the serialized response.
//
// For client-streaming methods, the request is a sequence of framed messages; for
// server-streaming methods, the response is a sequence of framed messages. See AppendFrame and
// ReadFrames for the framing. Errors are gRPC status errors, as the server returned them.
func (s *Session) Call(ctx context.Context, fullMethod string, request []byte) ([]byte, error) {
	return call(ctx, s.server.conn, s.token, fullMethod, request)
}

func call(ctx context.Context, conn *grpc.ClientConn, token, fullMethod string, request []byte) ([]byte, error) {
	method, err := lookupCallableMethod(fullMethod)
	if err != nil {
		return nil, err
	}

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	codec := grpc.ForceCodec(rawCodec{})

	if !method.IsStreamingClient() && !method.IsStreamingServer() {
		var response []byte
		if err := conn.Invoke(ctx, fullMethod, &request, &response, codec); err != nil {
			return nil, err
		}
		return response, nil
	}

	requests := [][]byte{request}
	if method.IsStreamingClient() {
		requests, err = ReadFrames(request)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{
		ClientStreams: method.IsStreamingClient(),
		ServerStreams: method.IsStreamingServer(),
	}, fullMethod, codec)
	if err != nil {
		return nil, err
	}

	for _, req := range requests {
		if err := stream.SendMsg(&req); err != nil {
			// io.EOF means the server ended the stream; its status is returned by RecvMsg below.
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}

	var response []byte
	for {
		var msg []byte
		err := stream.RecvMsg(&msg)
		if errors.Is(err, io.EOF) {
			return response, nil
		}
		if err != nil {
			return nil, err
		}
		if method.IsStreamingServer() {
			response = AppendFrame(response, msg)
		} else {
			response = msg
		}
	}
}

func lookupCallableMethod(fullMethod string) (protoreflect.MethodDescriptor, error) {
	serviceName, methodName, ok := strings.Cut(strings.TrimPrefix(fullMethod, "/"), "/")
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "malformed method name %q: expected /<service>/<method>", fullMethod)
	}
	if _, ok := callableServices[serviceName]; !ok {
		return nil, status.Errorf(codes.Unimplemented, "unknown or unsupported service %q", serviceName)
	}

	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(serviceName))
	if err != nil {
		return nil, status.Errorf(codes.Unimplemented, "unknown service %q", serviceName)
	}
	service, ok := desc.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, "unknown service %q", serviceName)
	}
	method := service.Methods().ByName(protoreflect.Name(methodName))
	if method == nil {
		return nil, status.Errorf(codes.Unimplemented, "unknown method %q", fullMethod)
	}
	return method, nil
}

// AppendFrame appends msg to buf, prefixed with its length as a little-endian uint32.
func AppendFrame(buf, msg []byte) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(msg))) //nolint:gosec // messages are far below 4GiB
	return append(buf, msg...)
}

// ReadFrames splits a sequence of messages framed by AppendFrame.
func ReadFrames(buf []byte) ([][]byte, error) {
	var msgs [][]byte
	for len(buf) > 0 {
		if len(buf) < 4 {
			return nil, errors.New("truncated frame header")
		}
		size := binary.LittleEndian.Uint32(buf)
		buf = buf[4:]
		if uint64(len(buf)) < uint64(size) {
			return nil, fmt.Errorf("truncated frame: expected %d bytes, have %d", size, len(buf))
		}
		msgs = append(msgs, buf[:size])
		buf = buf[size:]
	}
	return msgs, nil
}

// rawCodec passes already-serialized messages through to the wire unchanged. The server decodes
// them with its usual proto codec.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("rawCodec: unexpected message type %T", v)
	}
	return *b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawCodec: unexpected message type %T", v)
	}
	*b = append([]byte(nil), data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }
