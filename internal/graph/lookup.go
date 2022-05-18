package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
	"google.golang.org/grpc/metadata"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const MaxConcurrentSlowLookupChecks = 10

// NewConcurrentLookup creates and instance of ConcurrentLookup.
func NewConcurrentLookup(c dispatch.Check, r dispatch.ReachableResources) *ConcurrentLookup {
	return &ConcurrentLookup{c: c, r: r}
}

// ConcurrentLookup exposes a method to perform Lookup requests, and delegates subproblems to the
// provided dispatch.Lookup instance.
type ConcurrentLookup struct {
	c dispatch.Check
	r dispatch.ReachableResources
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
}

func (ls *collectingStream) Send(result *v1.DispatchReachableResourcesResponse) error {
	if result == nil {
		panic("Got nil result")
	}

	if result.Resource.ResultStatus == v1.ReachableResource_HAS_PERMISSION {
		ls.checker.AddResult(result.Resource.Resource)
		return nil
	}

	ls.checker.QueueCheck(result.Resource.Resource, &v1.ResolverMeta{
		AtRevision:     ls.req.Revision.String(),
		DepthRemaining: ls.req.Metadata.DepthRemaining,
	})
	return nil
}

func (ls *collectingStream) SetHeader(metadata.MD) error {
	return nil
}

func (ls *collectingStream) SendHeader(metadata.MD) error {
	return nil
}

func (ls *collectingStream) SetTrailer(metadata.MD) {
}

func (ls *collectingStream) Context() context.Context {
	return ls.context
}

func (ls *collectingStream) SendMsg(m interface{}) error {
	return fmt.Errorf("not implemented")
}

func (ls *collectingStream) RecvMsg(m interface{}) error {
	return fmt.Errorf("not implemented")
}

func (cl *ConcurrentLookup) LookupViaReachability(ctx context.Context, req ValidatedLookupRequest) (*v1.DispatchLookupResponse, error) {
	if req.Subject.ObjectId == tuple.PublicWildcard {
		resp := lookupResultError(req, NewErrInvalidArgument(errors.New("cannot perform lookup on wildcard")), emptyMetadata)
		return resp.Resp, resp.Err
	}

	cancelCtx, checkCancel := context.WithCancel(ctx)
	defer checkCancel()

	checker := NewParallelChecker(cancelCtx, cl.c, req.Subject, 10)
	stream := &collectingStream{checker, req, cancelCtx}

	// Start the checker.
	checker.Start()

	// TODO(jschorr): Collect dispatch count and depth required.
	// Dispatch to the reachability API to find all reachable objects.
	err := cl.r.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
		ObjectRelation: req.ObjectRelation,
		Subject:        req.Subject,
		Metadata:       req.Metadata,
	}, stream)
	if err != nil {
		resp := lookupResultError(req, NewErrInvalidArgument(fmt.Errorf("error in reachablility: %w", err)), emptyMetadata)
		return resp.Resp, resp.Err
	}

	// Wait for the checker to finish.
	allowed, err := checker.Finish()
	if err != nil {
		resp := lookupResultError(req, err, emptyMetadata)
		return resp.Resp, resp.Err
	}

	res := lookupResult(req, limitedSlice(allowed.AsSlice(), req.Limit), &v1.ResponseMeta{
		DispatchCount:       0,
		CachedDispatchCount: 0,
		DepthRequired:       0,
	})
	return res.Resp, res.Err
}

func lookupResult(req ValidatedLookupRequest, resolvedONRs []*core.ObjectAndRelation, subProblemMetadata *v1.ResponseMeta) LookupResult {
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

func lookupResultError(req ValidatedLookupRequest, err error, subProblemMetadata *v1.ResponseMeta) LookupResult {
	return LookupResult{
		&v1.DispatchLookupResponse{
			Metadata: ensureMetadata(subProblemMetadata),
		},
		err,
	}
}
