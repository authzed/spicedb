package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/shopspring/decimal"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
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
	Revision decimal.Decimal
}

type collectingStream struct {
	checker *ParallelChecker
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
		panic("Got nil result")
	}

	func() {
		ls.mu.Lock()
		defer ls.mu.Unlock()

		ls.dispatchCount += result.Metadata.DispatchCount
		ls.cachedDispatchCount += result.Metadata.CachedDispatchCount
		ls.depthRequired = max(result.Metadata.DepthRequired, ls.depthRequired)
	}()

	for _, id := range result.Resource.ResourceIds {
		resource := &core.ObjectAndRelation{
			Namespace: ls.req.ObjectRelation.Namespace,
			ObjectId:  id,
			Relation:  ls.req.ObjectRelation.Relation,
		}

		if result.Resource.ResultStatus == v1.ReachableResource_HAS_PERMISSION {
			ls.checker.AddResult(resource)
			continue
		}

		ls.checker.QueueCheck(resource, &v1.ResolverMeta{
			AtRevision:     ls.req.Revision.String(),
			DepthRemaining: ls.req.Metadata.DepthRemaining,
		})
	}
	return nil
}

func (cl *ConcurrentLookup) LookupViaReachability(ctx context.Context, req ValidatedLookupRequest) (*v1.DispatchLookupResponse, error) {
	if req.Subject.ObjectId == tuple.PublicWildcard {
		resp := lookupResultError(NewErrInvalidArgument(errors.New("cannot perform lookup on wildcard")), emptyMetadata)
		return resp.Resp, resp.Err
	}

	cancelCtx, checkCancel := context.WithCancel(ctx)
	defer checkCancel()

	checker := NewParallelChecker(cancelCtx, cl.c, req.Subject, cl.concurrencyLimit)
	stream := &collectingStream{checker, req, cancelCtx, 0, 0, 0, sync.Mutex{}}

	// Start the checker.
	checker.Start()

	// Dispatch to the reachability API to find all reachable objects and queue them
	// either for checks, or directly as results.
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
		resp := lookupResultError(NewErrInvalidArgument(fmt.Errorf("error in reachablility: %w", err)), emptyMetadata)
		return resp.Resp, resp.Err
	}

	// Wait for the checker to finish.
	allowed, err := checker.Wait()
	if err != nil {
		resp := lookupResultError(err, emptyMetadata)
		return resp.Resp, resp.Err
	}

	res := lookupResult(limitedSlice(allowed.AsSlice(), req.Limit), &v1.ResponseMeta{
		DispatchCount:       stream.dispatchCount + checker.DispatchCount() + 1, // +1 for the lookup
		CachedDispatchCount: stream.cachedDispatchCount + checker.CachedDispatchCount(),
		DepthRequired:       max(stream.depthRequired, checker.DepthRequired()) + 1, // +1 for the lookup
	})
	return res.Resp, res.Err
}

func lookupResult(resolvedONRs []*core.ObjectAndRelation, subProblemMetadata *v1.ResponseMeta) LookupResult {
	return LookupResult{
		&v1.DispatchLookupResponse{
			Metadata:     ensureMetadata(subProblemMetadata),
			ResolvedOnrs: resolvedONRs,
		},
		nil,
	}
}

func limitedSlice(slice []*core.ObjectAndRelation, limit uint32) []*core.ObjectAndRelation {
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
