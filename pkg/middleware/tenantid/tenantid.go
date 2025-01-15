package tenantid

import (
	"context"
	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const CtxTenantIDKey = "tenantID"

func FromContext(ctx context.Context) string {
	tenantID, ok := ctx.Value(CtxTenantIDKey).(string)
	if !ok {
		return ""
	}
	return tenantID
}

func ContextWithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, CtxTenantIDKey, tenantID)
}

func OutgoingContextWithTenantID(ctx context.Context, tenantID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, CtxTenantIDKey, tenantID)
}

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
		wrapped := grpcmiddleware.WrapServerStream(stream)
		wrapped.WrappedContext = ctx
		return handler(srv, wrapped)
	}
}
