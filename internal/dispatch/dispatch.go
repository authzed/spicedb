package dispatch

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
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

type cachePrefix string

const (
	checkViaRelationPrefix   cachePrefix = "cr"
	checkViaCanonicalPrefix  cachePrefix = "cc"
	lookupPrefix             cachePrefix = "l"
	expandPrefix             cachePrefix = "e"
	reachableResourcesPrefix cachePrefix = "rr"
	lookupSubjectsPrefix     cachePrefix = "ls"
)

var cachePrefixes = []cachePrefix{checkViaRelationPrefix, checkViaCanonicalPrefix, lookupPrefix, expandPrefix, reachableResourcesPrefix}

// CheckRequestToKey converts a check request into a cache key based on the relation
func CheckRequestToKey(req *v1.DispatchCheckRequest) string {
	return fmt.Sprintf("%s//%s@%s@%s", checkViaRelationPrefix, tuple.StringONR(req.ResourceAndRelation), tuple.StringONR(req.Subject), req.Metadata.AtRevision)
}

// CheckRequestToKeyWithCanonical converts a check request into a cache key based
// on the canonical key.
func CheckRequestToKeyWithCanonical(req *v1.DispatchCheckRequest, canonicalKey string) string {
	if canonicalKey == "" {
		panic(fmt.Sprintf("given empty canonical key for request: %s => %s", req.ResourceAndRelation, tuple.StringONR(req.Subject)))
	}

	// NOTE: canonical cache keys are only unique *within* a version of a namespace.
	return fmt.Sprintf("%s//%s:%s#%s@%s@%s", checkViaCanonicalPrefix, req.ResourceAndRelation.Namespace, req.ResourceAndRelation.ObjectId, canonicalKey, tuple.StringONR(req.Subject), req.Metadata.AtRevision)
}

// LookupRequestToKey converts a lookup request into a cache key
func LookupRequestToKey(req *v1.DispatchLookupRequest) string {
	return fmt.Sprintf("%s//%s#%s@%s@%s", lookupPrefix, req.ObjectRelation.Namespace, req.ObjectRelation.Relation, tuple.StringONR(req.Subject), req.Metadata.AtRevision)
}

// ExpandRequestToKey converts an expand request into a cache key
func ExpandRequestToKey(req *v1.DispatchExpandRequest) string {
	return fmt.Sprintf("%s//%s@%s", expandPrefix, tuple.StringONR(req.ResourceAndRelation), req.Metadata.AtRevision)
}

// ReachableResourcesRequestToKey converts a reachable resources request into a cache key
func ReachableResourcesRequestToKey(req *v1.DispatchReachableResourcesRequest) string {
	return fmt.Sprintf("%s//%s#%s@%s#%s:[%s]@%s",
		reachableResourcesPrefix,
		req.ResourceRelation.Namespace,
		req.ResourceRelation.Relation,
		req.SubjectRelation.Namespace,
		req.SubjectRelation.Relation,
		strings.Join(req.SubjectIds, ","),
		req.Metadata.AtRevision,
	)
}

// LookupSubjectsRequestToKey converts a lookup subjects request into a cache key
func LookupSubjectsRequestToKey(req *v1.DispatchLookupSubjectsRequest) string {
	return stringz.Join("",
		string(lookupSubjectsPrefix),
		"//",
		req.ResourceRelation.Namespace,
		"#",
		req.ResourceRelation.Relation,
		"@",
		req.SubjectRelation.Namespace,
		"#",
		req.SubjectRelation.Relation,
		":",
		strings.Join(req.ResourceIds, ","),
		"@",
		req.Metadata.AtRevision,
	)
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
