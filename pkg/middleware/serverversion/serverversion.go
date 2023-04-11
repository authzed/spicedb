package serverversion

import (
	"context"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"github.com/authzed/authzed-go/pkg/responsemeta"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/releases"
)

// HandleServerVersion defines a middleware for returning the version of the server
// when requested via the RequestServerVersion header.
type HandleServerVersion struct {
	// IsEnabled is whether the middleware is enabled.
	IsEnabled bool

	// GetVersion is the function used to retrieve the service version.
	GetVersion func() (string, error)
}

func (r *HandleServerVersion) ServerReporter(ctx context.Context, _ interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	if r.IsEnabled {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if _, isRequestingVersion := md[string(requestmeta.RequestServerVersion)]; isRequestingVersion {
				version, err := r.GetVersion()
				if err != nil {
					log.Ctx(ctx).Err(err).Msg("could not load current service version")
					return interceptors.NoopReporter{}, ctx
				}

				err = responsemeta.SetResponseHeaderMetadata(ctx, map[responsemeta.ResponseMetadataHeaderKey]string{
					responsemeta.ServerVersion: version,
				})
				// if context is cancelled, the stream will be closed, and gRPC will return ErrIllegalHeaderWrite
				// this prevents logging unnecessary error messages
				if err := ctx.Err(); err != nil {
					return interceptors.NoopReporter{}, ctx
				}
				if err != nil {
					log.Ctx(ctx).Warn().Err(err).Msg("serverversion: could not report metadata")
				}
			}
		}
	}

	return interceptors.NoopReporter{}, ctx
}

// UnaryServerInterceptor returns a new interceptor which handles server version requests.
func UnaryServerInterceptor(isEnabled bool) grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(&HandleServerVersion{isEnabled, releases.CurrentVersion})
}

// StreamServerInterceptor returns a new interceptor which handles server version requests.
func StreamServerInterceptor(isEnabled bool) grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(&HandleServerVersion{isEnabled, releases.CurrentVersion})
}
