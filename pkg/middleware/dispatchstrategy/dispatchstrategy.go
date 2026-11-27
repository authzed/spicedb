// Package dispatchstrategy handles the secondary dispatch strategy override a request can carry
// in its requestmeta.RequestDispatchStrategyKey header: a CEL expression that replaces the
// configured secondary dispatch expression for the request and every dispatch it causes.
//
// The override travels with the request as incoming gRPC metadata. The public API validates it
// (UnaryServerInterceptor, StreamServerInterceptor), dispatch clients forward it to the nodes
// they dispatch to (UnaryClientInterceptor, StreamClientInterceptor), and code that detaches a
// context from its request carries it over (PropagateIfExists).
package dispatchstrategy

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

const metadataKey = string(requestmeta.RequestDispatchStrategyKey)

// Validator checks a request's strategy override before the request runs.
type Validator interface {
	// Validate returns an error if expr cannot override the secondary dispatch expression of
	// every one of the given request kinds (the keys of the secondary dispatch expressions,
	// such as "check").
	Validate(expr string, requestKinds ...string) error
}

// requestKindsByMethod lists, for each API method that accepts a strategy override, the kinds of
// the dispatches it causes. LookupResources also dispatches checks, to filter the resources it
// found.
var requestKindsByMethod = map[string][]string{
	v1.PermissionsService_CheckPermission_FullMethodName:      {"check"},
	v1.PermissionsService_CheckBulkPermissions_FullMethodName: {"check"},
	v1.ExperimentalService_BulkCheckPermission_FullMethodName: {"check"},
	v1.PermissionsService_LookupResources_FullMethodName:      {"lookupresources", "check"},
	v1.PermissionsService_LookupSubjects_FullMethodName:       {"lookupsubjects"},
}

// RequestKinds returns the kinds of the dispatches caused by an API method, or false if the
// method does not accept a strategy override.
func RequestKinds(fullMethod string) ([]string, bool) {
	kinds, ok := requestKindsByMethod[fullMethod]
	return kinds, ok
}

// FromContext returns the strategy override of the request ctx belongs to, if any.
func FromContext(ctx context.Context) (string, bool) {
	values := metadata.ValueFromIncomingContext(ctx, metadataKey)
	if len(values) == 0 {
		return "", false
	}
	return values[0], true
}

// PropagateIfExists copies the strategy override from the source context to the target context,
// if source has one, and returns the updated target context.
func PropagateIfExists(source, target context.Context) context.Context {
	expr, ok := FromContext(source)
	if !ok {
		return target
	}

	targetMD, _ := metadata.FromIncomingContext(target)
	if targetMD == nil {
		targetMD = metadata.New(nil)
	}
	targetMD.Set(metadataKey, expr)
	return metadata.NewIncomingContext(target, targetMD)
}

// forward adds the strategy override of the request ctx belongs to, if any, to the outgoing
// metadata of ctx.
func forward(ctx context.Context) context.Context {
	expr, ok := FromContext(ctx)
	if !ok {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, metadataKey, expr)
}

// UnaryClientInterceptor forwards the strategy override of the request being served to the
// dispatches it makes.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(forward(ctx), method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptor forwards the strategy override of the request being served to the
// dispatches it makes.
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(forward(ctx), desc, cc, method, opts...)
	}
}

// UnaryServerInterceptor rejects, with InvalidArgument, API requests whose strategy override
// validator does not accept. A nil validator means overrides are disabled, and rejects every
// request that has one.
func UnaryServerInterceptor(validator Validator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := validate(ctx, info.FullMethod, validator); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor rejects, with InvalidArgument, API requests whose strategy override
// validator does not accept. A nil validator means overrides are disabled, and rejects every
// request that has one.
func StreamServerInterceptor(validator Validator) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := validate(stream.Context(), info.FullMethod, validator); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func validate(ctx context.Context, fullMethod string, validator Validator) error {
	values := metadata.ValueFromIncomingContext(ctx, metadataKey)
	if len(values) == 0 {
		return nil
	}

	switch {
	case validator == nil:
		return status.Errorf(codes.InvalidArgument, "the %s header is not allowed: dispatch strategy overrides are not enabled on this server", metadataKey)
	case len(values) > 1:
		return status.Errorf(codes.InvalidArgument, "the %s header may be set at most once, found %d values", metadataKey, len(values))
	}

	kinds, ok := RequestKinds(fullMethod)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "the %s header is not supported by %s", metadataKey, fullMethod)
	}

	if err := validator.Validate(values[0], kinds...); err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("invalid %s header: %s", metadataKey, err))
	}
	return nil
}
