package nodeid

import (
	"context"
	"fmt"
	"os"

	"github.com/cespare/xxhash/v2"
	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

const spiceDBPrefix = "spicedb:"

type ctxKeyType struct{}

var nodeIDKey ctxKeyType = struct{}{}

type nodeIDHandle struct {
	nodeID string
}

var defaultNodeID string

func init() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Warn().Err(err).Msg("failed to get hostname, using an empty node ID")
		return
	}

	// Hash the hostname to get the final default node ID.
	hasher := xxhash.New()
	if _, err := hasher.WriteString(hostname); err != nil {
		log.Warn().Err(err).Msg("failed to hash hostname, using an empty node ID")
		return
	}

	defaultNodeID = spiceDBPrefix + fmt.Sprintf("%x", hasher.Sum(nil))
}

// ContextWithHandle adds a placeholder to a context that will later be
// filled by the Node ID.
func ContextWithHandle(ctx context.Context) context.Context {
	return context.WithValue(ctx, nodeIDKey, &nodeIDHandle{})
}

// FromContext reads the node's ID out of a context.Context.
func FromContext(ctx context.Context) (string, error) {
	if c := ctx.Value(nodeIDKey); c != nil {
		handle := c.(*nodeIDHandle)
		if handle.nodeID != "" {
			return handle.nodeID, nil
		}
	}

	if err := setInContext(ctx, defaultNodeID); err != nil {
		return "", err
	}

	return defaultNodeID, nil
}

// setInContext adds a node ID to the given context
func setInContext(ctx context.Context, nodeID string) error {
	handle := ctx.Value(nodeIDKey)
	if handle == nil {
		return nil
	}
	handle.(*nodeIDHandle).nodeID = nodeID
	return nil
}

// UnaryServerInterceptor returns a new unary server interceptor that adds the
// node ID to the context. If empty, spicedb:$hostname is used.
func UnaryServerInterceptor(nodeID string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx := ContextWithHandle(ctx)
		if nodeID != "" {
			if err := setInContext(newCtx, nodeID); err != nil {
				return nil, err
			}
		}
		return handler(newCtx, req)
	}
}

// StreamServerInterceptor returns a new stream server interceptor that adds the
// node ID to the context. If empty, spicedb:$hostname is used.
func StreamServerInterceptor(nodeID string) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrapped := middleware.WrapServerStream(stream)
		wrapped.WrappedContext = ContextWithHandle(wrapped.WrappedContext)
		if nodeID != "" {
			if err := setInContext(wrapped.WrappedContext, nodeID); err != nil {
				return err
			}
		}
		return handler(srv, wrapped)
	}
}
