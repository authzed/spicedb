package dispatch

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// ErrMaxDepth is returned from CheckDepth when the max depth is exceeded.
var ErrMaxDepth = errors.New("max depth exceeded: this usually indicates a recursive or too deep data dependency")

// Dispatcher interface describes a method for passing subchecks off to additional machines.
type Dispatcher interface {
	Check
	Expand
	Lookup
	ReachableResources
	LookupSubjects

	// Close closes the dispatcher.
	Close() error

	// IsReady returns true when dispatcher is able to respond to requests
	IsReady() bool
}

// Check interface describes just the methods required to dispatch check requests.
type Check interface {
	// DispatchCheck submits a single check request and returns its result.
	DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error)
}

// Expand interface describes just the methods required to dispatch expand requests.
type Expand interface {
	// DispatchExpand submits a single expand request and returns its result.
	DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error)
}

// Lookup interface describes just the methods required to dispatch lookup requests.
type Lookup interface {
	// DispatchLookup submits a single lookup request and returns its result.
	DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error)
}

// ReachableResourcesStream is an alias for the stream to which reachable resources will be written.
type ReachableResourcesStream = Stream[*v1.DispatchReachableResourcesResponse]

// ReachableResources interface describes just the methods required to dispatch reachable resources requests.
type ReachableResources interface {
	// DispatchReachableResources submits a single reachable resources request, writing its results to the specified stream.
	DispatchReachableResources(
		req *v1.DispatchReachableResourcesRequest,
		stream ReachableResourcesStream,
	) error
}

// LookupSubjectsStream is an alias for the stream to which found subjects will be written.
type LookupSubjectsStream = Stream[*v1.DispatchLookupSubjectsResponse]

// LookupSubjects interface describes just the methods required to dispatch lookup subjects requests.
type LookupSubjects interface {
	// DispatchLookupSubjects submits a single lookup subjects request, writing its results to the specified stream.
	DispatchLookupSubjects(
		req *v1.DispatchLookupSubjectsRequest,
		stream LookupSubjectsStream,
	) error
}

// HasMetadata is an interface for requests containing resolver metadata.
type HasMetadata interface {
	zerolog.LogObjectMarshaler

	GetMetadata() *v1.ResolverMeta
}

// CheckDepth returns ErrMaxDepth if there is insufficient depth remaining to dispatch.
func CheckDepth(ctx context.Context, req HasMetadata) error {
	metadata := req.GetMetadata()
	if metadata == nil {
		log.Ctx(ctx).Warn().Object("request", req).Msg("request missing metadata")
		return fmt.Errorf("request missing metadata")
	}

	if metadata.DepthRemaining == 0 {
		return ErrMaxDepth
	}

	return nil
}

// AddResponseMetadata adds the metadata found in the incoming metadata to the existing
// metadata, *modifying it in place*.
func AddResponseMetadata(existing *v1.ResponseMeta, incoming *v1.ResponseMeta) {
	existing.DispatchCount += incoming.DispatchCount
	existing.CachedDispatchCount += incoming.CachedDispatchCount
	existing.DepthRequired = max(existing.DepthRequired, incoming.DepthRequired)
}

func max(x, y uint32) uint32 {
	if x < y {
		return y
	}
	return x
}
