package logging

import (
	"context"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/tags"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type fieldSpec struct {
	metadataKey string
	tagKey      string
}

// ExtractMetadataField creates a specification for converting gRPC metadata fields
// to log tags.
func ExtractMetadataField(metadataKey, tagKey string) fieldSpec {
	return fieldSpec{metadataKey, tagKey}
}

type extractMetadata struct {
	fields []fieldSpec
}

func (r *extractMetadata) ServerReporter(
	ctx context.Context, _ interface{}, _ interceptors.GRPCType, _ string, _ string) (interceptors.Reporter, context.Context) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		metadataTags := tags.NewTags()
		logContext := log.With()
		for _, field := range r.fields {
			value, ok := md[field.metadataKey]
			if ok {
				joinedValue := strings.Join(value, ",")
				metadataTags.Set(field.tagKey, joinedValue)

				logContext = logContext.Str(field.tagKey, joinedValue)
			}
		}

		ctx = tags.SetInContext(ctx, metadataTags)

		loggerForContext := logContext.Logger()
		ctx = loggerForContext.WithContext(ctx)
	}

	return interceptors.NoopReporter{}, ctx
}

// UnaryServerInterceptor creates an interceptor for extracting fields from requests
// and setting them as log tags.
func UnaryServerInterceptor(fields ...fieldSpec) grpc.UnaryServerInterceptor {
	return interceptors.UnaryServerInterceptor(&extractMetadata{fields})
}

// StreamServerInterceptor creates an interceptor for extracting fields from requests
// and setting them as log tags.
func StreamServerInterceptor(fields ...fieldSpec) grpc.StreamServerInterceptor {
	return interceptors.StreamServerInterceptor(&extractMetadata{fields})
}
