package requestmeta

import (
	"context"

	"google.golang.org/grpc/metadata"
)

// RequestMetadataHeaderKey defines a key in the request metadata header.
type RequestMetadataHeaderKey string

// BoolRequestMetadataHeaderKey defines a key for a boolean value in the request metadata header.
type BoolRequestMetadataHeaderKey RequestMetadataHeaderKey

const (
	// RequestServerVersion, if specified in a request header, asks SpiceDB to return its
	// server version in the response header (if supported).
	// Value: `1`
	RequestServerVersion BoolRequestMetadataHeaderKey = "io.spicedb.requestversion"

	// RequestDebugInformation, if specified in a request header, asks SpiceDB to return debug information
	// for the API call (if applicable and supported).
	// NOTE: deprecated in favor of setting with_tracing on Check requests.
	// Value: `1`
	RequestDebugInformation BoolRequestMetadataHeaderKey = "io.spicedb.requestdebuginfo"

	// RequestOverlapKey, if specified in a request header, indicates to SpiceDB
	// that all requests with the same overlap value should be protected from
	// the New Enemy Problem. This is only used with the CockroachDB datastore,
	// and only if user-provided request overlap is enabled.
	RequestOverlapKey RequestMetadataHeaderKey = "io.spicedb.requestoverlapkey"

	// RequestIDKey, if specified in a request header, will propagate the given string value
	// through SpiceDB for the lifetime of the request. This can be used to correlate logs
	// and traces with a specific request.
	RequestIDKey RequestMetadataHeaderKey = "x-request-id"
)

// AddRequestHeaders returns a new context with the given values as request headers.
func AddRequestHeaders(ctx context.Context, keys ...BoolRequestMetadataHeaderKey) context.Context {
	values := make(map[RequestMetadataHeaderKey]string, len(keys))
	for _, key := range keys {
		values[RequestMetadataHeaderKey(key)] = "1"
	}
	return SetRequestHeaders(ctx, values)
}

// SetRequestHeaders returns a new context with the given values as request headers.
func SetRequestHeaders(ctx context.Context, values map[RequestMetadataHeaderKey]string) context.Context {
	pairs := make([]string, 0, len(values)*2)
	for key, value := range values {
		pairs = append(pairs, string(key))
		pairs = append(pairs, value)
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

// WithOverlapKey returns a new context with the overlap key set.
func WithOverlapKey(ctx context.Context, key string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, string(RequestOverlapKey), key)
}

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, string(RequestIDKey), requestID)
}
