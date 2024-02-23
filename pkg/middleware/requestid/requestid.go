package requestid

import (
	"context"

	log "github.com/authzed/spicedb/internal/logging"

	"github.com/authzed/authzed-go/pkg/responsemeta"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/rs/xid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// MetadataKey is the key in which request IDs are passed to metadata.
const MetadataKey = "x-request-id"

// Option instances control how the middleware is initialized.
type Option func(*handleRequestID)

// GenerateIfMissing will instruct the middleware to create a request ID if one
// isn't already on the incoming request.
//
// default: false
func GenerateIfMissing(enable bool) Option {
	return func(reporter *handleRequestID) {
		reporter.generateIfMissing = enable
	}
}

// IDGenerator functions are used to generate request IDs if a new one is needed.
type IDGenerator func() string

// GenerateRequestID generates a new request ID.
func GenerateRequestID() string {
	return xid.New().String()
}

type handleRequestID struct {
	generateIfMissing  bool
	requestIDGenerator IDGenerator
}

func (r *handleRequestID) ServerReporter(ctx context.Context, _ interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	var requestID string
	var haveRequestID bool
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		var requestIDs []string
		requestIDs, haveRequestID = md[MetadataKey]
		if haveRequestID {
			requestID = requestIDs[0]
		}
	}

	if !haveRequestID && r.generateIfMissing {
		requestID, haveRequestID = r.requestIDGenerator(), true

		// Inject the newly generated request ID into the metadata
		if md == nil {
			md = metadata.New(nil)
		}

		md.Set(MetadataKey, requestID)
		ctx = metadata.NewIncomingContext(ctx, md)
	}

	if haveRequestID {
		ctx = metadata.AppendToOutgoingContext(ctx, MetadataKey, requestID)
		err := responsemeta.SetResponseHeaderMetadata(ctx, map[responsemeta.ResponseMetadataHeaderKey]string{
			responsemeta.RequestID: requestID,
		})
		// if context is cancelled, the stream will be closed, and gRPC will return ErrIllegalHeaderWrite
		// this prevents logging unnecessary error messages
		if ctx.Err() != nil {
			return interceptors.NoopReporter{}, ctx
		}
		if err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("requestid: could not report metadata")
		}
	}

	return interceptors.NoopReporter{}, ctx
}

// UnaryServerInterceptor returns a new interceptor which handles request IDs according
// to the provided options.
func UnaryServerInterceptor(opts ...Option) grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(createReporter(opts))
}

// StreamServerInterceptor returns a new interceptor which handles request IDs according
// to the provided options.
func StreamServerInterceptor(opts ...Option) grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(createReporter(opts))
}

func createReporter(opts []Option) *handleRequestID {
	reporter := &handleRequestID{
		requestIDGenerator: GenerateRequestID,
	}

	for _, opt := range opts {
		opt(reporter)
	}

	return reporter
}
