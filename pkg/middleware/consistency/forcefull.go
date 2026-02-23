package consistency

import (
	"context"
	"strings"

	"google.golang.org/grpc"

	"github.com/authzed/spicedb/pkg/datalayer"
)

// ForceFullConsistencyUnaryServerInterceptor returns a new unary server interceptor that enforces full consistency
// for all requests, except for those in the bypassServiceWhitelist.
func ForceFullConsistencyUnaryServerInterceptor(serviceLabel string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(ctx, req)
			}
		}
		dl := datalayer.MustFromContext(ctx)
		newCtx := ContextWithHandle(ctx)
		if err := setFullConsistencyRevisionToContext(newCtx, req, dl, serviceLabel, TreatMismatchingTokensAsFullConsistency); err != nil {
			return nil, err
		}

		return handler(newCtx, req)
	}
}

// ForceFullConsistencyStreamServerInterceptor returns a new stream server interceptor that enforces full consistency
// for all requests, except for those in the bypassServiceWhitelist.
func ForceFullConsistencyStreamServerInterceptor(serviceLabel string) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(srv, stream)
			}
		}
		wrapper := &recvWrapper{stream, ContextWithHandle(stream.Context()), serviceLabel, TreatMismatchingTokensAsFullConsistency, setFullConsistencyRevisionToContext}
		return handler(srv, wrapper)
	}
}

func setFullConsistencyRevisionToContext(ctx context.Context, req any, dl datalayer.DataLayer, serviceLabel string, _ MismatchingTokenOption) error {
	handle := ctx.Value(revisionKey)
	if handle == nil {
		return nil
	}

	if _, ok := req.(hasConsistency); ok {
		if serviceLabel != "" {
			ConsistencyCounter.WithLabelValues("full", "request", serviceLabel).Inc()
		}

		databaseRev, err := dl.HeadRevision(ctx)
		if err != nil {
			return rewriteDatastoreError(err)
		}
		handle.(*revisionHandle).revision = databaseRev
	}

	return nil
}
