package dispatch

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"

	log "github.com/authzed/spicedb/internal/logging"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// ReadyState represents the ready state of the dispatcher.
type ReadyState struct {
	// Message is a human-readable status message for the current state.
	Message string

	// IsReady indicates whether the datastore is ready.
	IsReady bool
}

// Dispatcher interface describes a method for passing subchecks off to additional machines.
type Dispatcher interface {
	Check
	Expand
	LookupSubjects
	LookupResources2

	// Close closes the dispatcher.
	Close() error

	// ReadyState returns true when dispatcher is able to respond to requests
	ReadyState() ReadyState
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

type LookupResources2Stream = Stream[*v1.DispatchLookupResources2Response]

type LookupResources2 interface {
	DispatchLookupResources2(
		req *v1.DispatchLookupResources2Request,
		stream LookupResources2Stream,
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

// DispatchableRequest is an interface for requests.
type DispatchableRequest interface {
	zerolog.LogObjectMarshaler

	GetMetadata() *v1.ResolverMeta
}

// CheckDepth returns ErrMaxDepth if there is insufficient depth remaining to dispatch.
func CheckDepth(ctx context.Context, req DispatchableRequest) error {
	metadata := req.GetMetadata()
	if metadata == nil {
		log.Ctx(ctx).Warn().Object("request", req).Msg("request missing metadata")
		return fmt.Errorf("request missing metadata")
	}

	if metadata.DepthRemaining == 0 {
		return NewMaxDepthExceededError(req)
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
