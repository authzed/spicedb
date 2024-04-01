package requestid

import (
	"context"

	log "github.com/authzed/spicedb/internal/logging"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"github.com/authzed/authzed-go/pkg/responsemeta"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/rs/xid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const metadataKey = string(requestmeta.RequestIDKey)

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

func (r *handleRequestID) ClientReporter(ctx context.Context, meta interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	haveRequestID, requestID, ctx := r.fromContextOrGenerate(ctx)

	if haveRequestID {
		ctx = requestmeta.SetRequestHeaders(ctx, map[requestmeta.RequestMetadataHeaderKey]string{
			requestmeta.RequestIDKey: requestID,
		})
	}

	return interceptors.NoopReporter{}, ctx
}

func (r *handleRequestID) ServerReporter(ctx context.Context, _ interceptors.CallMeta) (interceptors.Reporter, context.Context) {
	haveRequestID, requestID, ctx := r.fromContextOrGenerate(ctx)

	if haveRequestID {
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

func (r *handleRequestID) fromContextOrGenerate(ctx context.Context) (bool, string, context.Context) {
	haveRequestID, requestID, md := fromContext(ctx)

	if !haveRequestID && r.generateIfMissing {
		requestID = r.requestIDGenerator()
		haveRequestID = true

		// Inject the newly generated request ID into the metadata
		if md == nil {
			md = metadata.New(nil)
		}

		md.Set(metadataKey, requestID)
		ctx = metadata.NewIncomingContext(ctx, md)
	}

	return haveRequestID, requestID, ctx
}

func fromContext(ctx context.Context) (bool, string, metadata.MD) {
	var requestID string
	var haveRequestID bool
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		var requestIDs []string
		requestIDs, haveRequestID = md[metadataKey]
		if haveRequestID {
			requestID = requestIDs[0]
		}
	}

	return haveRequestID, requestID, md
}

// PropagateIfExists copies the request ID from the source context to the target context if it exists.
// The updated target context is returned.
func PropagateIfExists(source, target context.Context) context.Context {
	exists, requestID, _ := fromContext(source)

	if exists {
		targetMD, _ := metadata.FromIncomingContext(target)
		if targetMD == nil {
			targetMD = metadata.New(nil)
		}

		targetMD.Set(metadataKey, requestID)
		return metadata.NewIncomingContext(target, targetMD)
	}

	return target
}

// UnaryServerInterceptor returns a new interceptor which handles server request IDs according
// to the provided options.
func UnaryServerInterceptor(opts ...Option) grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(createReporter(opts))
}

// StreamServerInterceptor returns a new interceptor which handles server request IDs according
// to the provided options.
func StreamServerInterceptor(opts ...Option) grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(createReporter(opts))
}

// UnaryClientInterceptor returns a new interceptor which handles client request IDs according
// to the provided options.
func UnaryClientInterceptor(opts ...Option) grpc.UnaryClientInterceptor {
	return interceptors.UnaryClientInterceptor(createReporter(opts))
}

// StreamClientInterceptor returns a new interceptor which handles client requestIDs according
// to the provided options.
func StreamClientInterceptor(opts ...Option) grpc.StreamClientInterceptor {
	return interceptors.StreamClientInterceptor(createReporter(opts))
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
