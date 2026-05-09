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
	// ServerVersion is the key in the response header metadata holding the version of the server
	// handling the API request, if requested via a request header.
	ServerVersion ResponseMetadataHeaderKey = "io.spicedb.debug.version"
)

// ResponseMetadataTrailerKey defines a key in the response metadata trailer.
type ResponseMetadataTrailerKey string

const (
	// RequestID is the key in the response trailer metadata for the request's tracking ID.
	// Uses trailer to maintain gRPC retry policy compatibility (see gRPC proposal A6).
	RequestID ResponseMetadataTrailerKey = "x-request-id"

	// RequestIDLegacy is the legacy key in the response trailer metadata for backward compatibility.
	// Kept to avoid breaking existing clients during transition period.
	RequestIDLegacy ResponseMetadataTrailerKey = "io.spicedb.respmeta.requestid"

	// DispatchedOperationsCount is the key in the response trailer metadata for
	// the number of dispatched operations that were needed to perform the overall
	// API call.
	DispatchedOperationsCount ResponseMetadataTrailerKey = "io.spicedb.respmeta.dispatchedoperationscount"

	// CachedOperationsCount is the key in the response trailer metadata for
	// the number of *cached* operations that would have been otherwise dispatched
	// to perform the overall API call.
	CachedOperationsCount ResponseMetadataTrailerKey = "io.spicedb.respmeta.cachedoperationscount"

	// DebugInformation contains the JSON-encoded form of the debug information for the API call,
	// if requested and supported.
	//
	// NOTE: deprecated in favor of the Check response containing the trace. The JSON will now
	// contain a note indicating to look on the response object itself.
	DebugInformation ResponseMetadataTrailerKey = "io.spicedb.respmeta.debuginfo"
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

// ListResponseTrailerMetadata retrieves the string value(s) for the given key in the trailer
// metadata of a SpiceDB API response.
func ListResponseTrailerMetadata(trailer metadata.MD, key ResponseMetadataTrailerKey) ([]string, error) {
	values := trailer.Get(string(key))
	if len(values) == 0 {
		return []string{}, fmt.Errorf("key `%s` not found in trailer", key)
	}

	return values, nil
}

// GetResponseTrailerMetadata retrieves a string value for the given key in the trailer
// metadata of a SpiceDB API response.
func GetResponseTrailerMetadata(trailer metadata.MD, key ResponseMetadataTrailerKey) (string, error) {
	values, err := ListResponseTrailerMetadata(trailer, key)
	if err != nil {
		return "", err
	}

	if len(values) != 1 {
		return "", fmt.Errorf("key `%s` found multiple times in trailer", key)
	}

	return values[0], nil
}

// GetResponseTrailerMetadataOrNil retrieves a string value for the given key in the trailer
// metadata of a SpiceDB API response or nil if not found.
func GetResponseTrailerMetadataOrNil(trailer metadata.MD, key ResponseMetadataTrailerKey) (*string, error) {
	values := trailer.Get(string(key))
	if len(values) == 0 {
		return nil, nil
	}

	if len(values) != 1 {
		return nil, fmt.Errorf("key `%s` found multiple times in trailer", key)
	}

	vle := values[0]
	return &vle, nil
}

// GetIntResponseTrailerMetadata retrieves an integer value for the given key in the trailer
// metadata of a SpiceDB API response.
func GetIntResponseTrailerMetadata(trailer metadata.MD, key ResponseMetadataTrailerKey) (int, error) {
	found, err := GetResponseTrailerMetadata(trailer, key)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(found)
}
