package graph

import (
	"context"
	"errors"
	"sync"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewConcurrentLookup creates and instance of ConcurrentLookup.
func NewConcurrentLookup(c dispatch.Check, r dispatch.ReachableResources, concurrencyLimit uint16) *ConcurrentLookup {
	return &ConcurrentLookup{c, r, concurrencyLimit}
}

// ConcurrentLookup exposes a method to perform Lookup requests, and delegates subproblems to the
// provided dispatch.Lookup instance.
type ConcurrentLookup struct {
	c                dispatch.Check
	r                dispatch.ReachableResources
	concurrencyLimit uint16
}

// ValidatedLookupRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedLookupRequest struct {
	*v1.DispatchLookupRequest
	Revision datastore.Revision
}

type collectingStream struct {
	checker *parallelChecker
	req     ValidatedLookupRequest
	context context.Context

	dispatchCount       uint32
	cachedDispatchCount uint32
	depthRequired       uint32

	mu sync.Mutex
}

func (ls *collectingStream) Context() context.Context {
	return ls.context
}

func (ls *collectingStream) Publish(result *v1.DispatchReachableResourcesResponse) error {
	if result == nil {
		return spiceerrors.MustBugf("got nil result for Lookup publish")
	}

	func() {
		ls.mu.Lock()
		defer ls.mu.Unlock()

		ls.dispatchCount += result.Metadata.DispatchCount
		ls.cachedDispatchCount += result.Metadata.CachedDispatchCount
		ls.depthRequired = max(result.Metadata.DepthRequired, ls.depthRequired)
	}()

	for _, found := range result.Resources {
		if found.ResultStatus == v1.ReachableResource_HAS_PERMISSION {
			ls.checker.AddResolvedResource(&v1.ResolvedResource{
				ResourceId:     found.ResourceId,
				Permissionship: v1.ResolvedResource_HAS_PERMISSION,
			})
			continue
		}

		ls.checker.QueueToCheck(found.ResourceId)
	}
	return nil
}

func (cl *ConcurrentLookup) LookupViaReachability(ctx context.Context, req ValidatedLookupRequest) (*v1.DispatchLookupResponse, error) {
	if req.Subject.ObjectId == tuple.PublicWildcard {
		resp := lookupResultError(NewErrInvalidArgument(errors.New("cannot perform lookup on wildcard")), emptyMetadata)
		return resp.Resp, resp.Err
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	checker := newParallelChecker(cancelCtx, cancel, cl.c, req, cl.concurrencyLimit)
	stream := &collectingStream{checker, req, cancelCtx, 0, 0, 0, sync.Mutex{}}

	// Start the checker.
	checker.Start()

	// Dispatch to the reachability API to find all reachable objects and queue them
	// either for checks, or directly as results.
	// NOTE: This dispatch call is blocking until all results have been sent to the specified
	// stream.
	err := cl.r.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
		ResourceRelation: req.ObjectRelation,
		SubjectRelation: &core.RelationReference{
			Namespace: req.Subject.Namespace,
			Relation:  req.Subject.Relation,
		},
		SubjectIds: []string{req.Subject.ObjectId},
		Metadata:   req.Metadata,
	}, stream)
	if err != nil {
		resp := lookupResultError(err, emptyMetadata)
		return resp.Resp, resp.Err
	}

	// Wait for the checker to finish.
	allowed, err := checker.Wait()
	if err != nil {
		resp := lookupResultError(err, emptyMetadata)
		return resp.Resp, resp.Err
	}

	res := lookupResult(allowed, req, &v1.ResponseMeta{
		DispatchCount:       stream.dispatchCount + checker.DispatchCount() + 1, // +1 for the lookup
		CachedDispatchCount: stream.cachedDispatchCount + checker.CachedDispatchCount(),
		DepthRequired:       max(stream.depthRequired, checker.DepthRequired()) + 1, // +1 for the lookup
	})
	return res.Resp, res.Err
}

func lookupResult(foundResources []*v1.ResolvedResource, req ValidatedLookupRequest, subProblemMetadata *v1.ResponseMeta) LookupResult {
	limitedResources := limitedSlice(foundResources, req.Limit)

	return LookupResult{
		&v1.DispatchLookupResponse{
			Metadata:          ensureMetadata(subProblemMetadata),
			ResolvedResources: limitedResources,
		},
		nil,
	}
}

func limitedSlice[T any](slice []T, limit uint32) []T {
	if len(slice) > int(limit) {
		return slice[0:limit]
	}

	return slice
}

func lookupResultError(err error, subProblemMetadata *v1.ResponseMeta) LookupResult {
	return LookupResult{
		&v1.DispatchLookupResponse{
			Metadata: ensureMetadata(subProblemMetadata),
		},
		err,
	}
}
