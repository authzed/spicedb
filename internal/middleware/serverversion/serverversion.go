package serverversion

import (
	"context"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"github.com/authzed/authzed-go/pkg/responsemeta"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/authzed/spicedb/pkg/releases"
)

type handleServerVersion struct {
	isEnabled bool
}

func (r *handleServerVersion) ServerReporter(ctx context.Context, _ interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	if r.isEnabled {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if _, isRequestingVersion := md[string(requestmeta.RequestServerVersion)]; isRequestingVersion {
				version, err := releases.CurrentVersion()
				if err != nil {
					log.Ctx(ctx).Err(err).Msg("could not load current software version")
					return interceptors.NoopReporter{}, ctx
				}

				err = responsemeta.SetResponseHeaderMetadata(ctx, map[responsemeta.ResponseMetadataHeaderKey]string{
					responsemeta.ServerVersion: version,
				})
				if err != nil {
					log.Ctx(ctx).Err(err).Msg("could not report metadata")
				}
			}
		}
	}

	return interceptors.NoopReporter{}, ctx
}

// UnaryServerInterceptor returns a new interceptor which handles server version requests.
func UnaryServerInterceptor(isEnabled bool) grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(&handleServerVersion{isEnabled})
}

// StreamServerInterceptor returns a new interceptor which handles server version requests.
func StreamServerInterceptor(isEnabled bool) grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(&handleServerVersion{isEnabled})
}
