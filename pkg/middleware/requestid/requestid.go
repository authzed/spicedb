package requestid

import (
	"context"
	"math/rand"

	"github.com/authzed/authzed-go/pkg/responsemeta"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RequestIDMetadataKey is the key in which request IDs are passed to metadata.
const RequestIDMetadataKey = "x-request-id"

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

// WithIDGenerator gives the middleware a function to use for generating requestIDs.
//
// default: 32 character hex string
func WithIDGenerator(genFunc IDGenerator) Option {
	return func(reporter *handleRequestID) {
		reporter.requestIDGenerator = genFunc
	}
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
		requestIDs, haveRequestID = md[RequestIDMetadataKey]
		if haveRequestID {
			requestID = requestIDs[0]
		}
	}

	if !haveRequestID && r.generateIfMissing {
		requestID, haveRequestID = r.requestIDGenerator(), true

		// Inject the newly generated request ID into the metadata
		md.Set(RequestIDMetadataKey, requestID)
		ctx = metadata.NewIncomingContext(ctx, md)
	}

	if haveRequestID {
		ctx = metadata.AppendToOutgoingContext(ctx, RequestIDMetadataKey, requestID)
		err := responsemeta.SetResponseHeaderMetadata(ctx, map[responsemeta.ResponseMetadataHeaderKey]string{
			responsemeta.RequestID: requestID,
		})
		if err != nil {
			log.Ctx(ctx).Err(err).Msg("could not report metadata")
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
		requestIDGenerator: func() string {
			return randSeq(32)
		},
	}

	for _, opt := range opts {
		opt(reporter)
	}

	return reporter
}

var letters = []rune("0123456789abcdef")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
