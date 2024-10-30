package consistency

import (
	"context"
	"strings"

	"google.golang.org/grpc"

	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

// ForceFullConsistencyUnaryServerInterceptor returns a new unary server interceptor that enforces full consistency
// for all requests, except for those in the bypassServiceWhitelist.
func ForceFullConsistencyUnaryServerInterceptor(serviceLabel string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(ctx, req)
			}
		}
		ds := datastoremw.MustFromContext(ctx)
		newCtx := ContextWithHandle(ctx)
		if err := setFullConsistencyRevisionToContext(newCtx, req, ds, serviceLabel); err != nil {
			return nil, err
		}

		return handler(newCtx, req)
	}
}

// ForceFullConsistencyStreamServerInterceptor returns a new stream server interceptor that enforces full consistency
// for all requests, except for those in the bypassServiceWhitelist.
func ForceFullConsistencyStreamServerInterceptor(serviceLabel string) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		for bypass := range bypassServiceWhitelist {
			if strings.HasPrefix(info.FullMethod, bypass) {
				return handler(srv, stream)
			}
		}
		wrapper := &recvWrapper{stream, ContextWithHandle(stream.Context()), serviceLabel, setFullConsistencyRevisionToContext}
		return handler(srv, wrapper)
	}
}

func setFullConsistencyRevisionToContext(ctx context.Context, req interface{}, ds datastore.Datastore, serviceLabel string) error {
	handle := ctx.Value(revisionKey)
	if handle == nil {
		return nil
	}

	switch req.(type) {
	case hasConsistency:
		if serviceLabel != "" {
			ConsistencyCounter.WithLabelValues("full", "request", serviceLabel).Inc()
		}

		databaseRev, err := ds.HeadRevision(ctx)
		if err != nil {
			return rewriteDatastoreError(ctx, err)
		}
		handle.(*revisionHandle).revision = databaseRev
	}

	return nil
}
