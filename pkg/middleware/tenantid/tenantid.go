package tenantid

import (
	"context"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const CtxTenantIDKey = "tenantID"

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			ctx = context.WithValue(ctx, CtxTenantIDKey, "")
		} else {
			tenantID := md.Get(CtxTenantIDKey)
			if len(tenantID) == 0 {
				ctx = context.WithValue(ctx, CtxTenantIDKey, "")
			} else {
				ctx = context.WithValue(ctx, CtxTenantIDKey, tenantID[0])
			}
		}
		return handler(ctx, req)
	}
}

func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			ctx = context.WithValue(ctx, CtxTenantIDKey, "")
		} else {
			tenantID := md.Get(CtxTenantIDKey)
			if len(tenantID) == 0 {
				ctx = context.WithValue(ctx, CtxTenantIDKey, "")
			} else {
				ctx = context.WithValue(ctx, CtxTenantIDKey, tenantID[0])
			}
		}
		wrapped := grpc_middleware.WrapServerStream(stream)
		wrapped.WrappedContext = ctx
		return handler(srv, wrapped)
	}
}
