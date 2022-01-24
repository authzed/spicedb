package responsemeta

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ResponseMetadataHeaderKey defines a key in the response metadata header.
type ResponseMetadataHeaderKey string

const (
	// RequestID is the key in the response header metadata for the request's tracking ID, if any.
	RequestID ResponseMetadataHeaderKey = "io.spicedb.respmeta.requestid"
)

// ResponseMetadataTrailerKey defines a key in the response metadata trailer.
type ResponseMetadataTrailerKey string

const (
	// DispatchedOperationsCount is the key in the response trailer metadata for
	// the number of dispatched operations that were needed to perform the overall
	// API call.
	DispatchedOperationsCount ResponseMetadataTrailerKey = "io.spicedb.respmeta.dispatchedoperationscount"

	// CachedOperationsCount is the key in the response trailer metadata for
	// the number of *cached* operations that would have been otherwise dispatched
	// to perform the overall API call.
	CachedOperationsCount ResponseMetadataTrailerKey = "io.spicedb.respmeta.cachedoperationscount"
)

// SetResponseHeaderMetadata sets the external response metadata header on the given context.
func SetResponseHeaderMetadata(ctx context.Context, values map[ResponseMetadataHeaderKey]string) error {
	pairs := make([]string, 0, len(values)*2)
	for key, value := range values {
		pairs = append(pairs, string(key))
		pairs = append(pairs, value)
	}
	return grpc.SetHeader(ctx, metadata.Pairs(pairs...))
}

// SetResponseTrailerMetadata sets the external response metadata trailer on the given context.
func SetResponseTrailerMetadata(ctx context.Context, values map[ResponseMetadataTrailerKey]string) error {
	pairs := make([]string, 0, len(values)*2)
	for key, value := range values {
		pairs = append(pairs, string(key))
		pairs = append(pairs, value)
	}
	return grpc.SetTrailer(ctx, metadata.Pairs(pairs...))
}

// GetIntResponseTrailerMetadata retrieves an integer value for the given key in the trailer
// metadata of a SpiceDB API response.
func GetIntResponseTrailerMetadata(trailer metadata.MD, key ResponseMetadataTrailerKey) (int, error) {
	values := trailer.Get(string(key))
	if len(values) == 0 {
		return -1, fmt.Errorf("key `%s` not found in trailer", key)
	}
	if len(values) != 1 {
		return -1, fmt.Errorf("key `%s` found multiple times in trailer", key)
	}

	return strconv.Atoi(values[0])
}
