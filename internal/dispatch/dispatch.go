package dispatch

import (
	"context"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/pkg/tuple"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
)

// ErrMaxDepth is returned from CheckDepth when the max depth is exceeded.
var ErrMaxDepth = errors.New("max depth exceeded")

// Dispatcher interface describes a method for passing subchecks off to additional machines.
type Dispatcher interface {
	Check
	Expand
	Lookup

	// Close closes the dispatcher.
	Close() error
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

// CheckRequestToKey converts a check request into a cache key
func CheckRequestToKey(req *v1.DispatchCheckRequest) string {
	return fmt.Sprintf("check//%s@%s@%s", tuple.StringONR(req.ObjectAndRelation), tuple.StringONR(req.Subject), req.Metadata.AtRevision)
}

// LookupRequestToKey converts a lookup request into a cache key
func LookupRequestToKey(req *v1.DispatchLookupRequest) string {
	return fmt.Sprintf("lookup//%s#%s@%s@%s", req.ObjectRelation.Namespace, req.ObjectRelation.Relation, tuple.StringONR(req.Subject), req.Metadata.AtRevision)
}

// ExpandRequestToKey converts an expand request into a cache key
func ExpandRequestToKey(req *v1.DispatchExpandRequest) string {
	return fmt.Sprintf("expand//%s@%s", tuple.StringONR(req.ObjectAndRelation), req.Metadata.AtRevision)
}
