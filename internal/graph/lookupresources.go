package graph

import (
	"context"
	"errors"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewCursoredLookupResources creates and instance of CursoredLookupResources.
func NewCursoredLookupResources(c dispatch.Check, r dispatch.ReachableResources, concurrencyLimit uint16) *CursoredLookupResources {
	return &CursoredLookupResources{c, r, concurrencyLimit}
}

// CursoredLookupResources exposes a method to perform LookupResources requests, and delegates subproblems to the
// provided dispatch.Lookup instance.
type CursoredLookupResources struct {
	c                dispatch.Check
	r                dispatch.ReachableResources
	concurrencyLimit uint16
}

// ValidatedLookupResourcesRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedLookupResourcesRequest struct {
	*v1.DispatchLookupResourcesRequest
	Revision datastore.Revision
}

func (cl *CursoredLookupResources) LookupResources(
	req ValidatedLookupResourcesRequest,
	parentStream dispatch.LookupResourcesStream,
) error {
	if req.Subject.ObjectId == tuple.PublicWildcard {
		return NewErrInvalidArgument(errors.New("cannot perform lookup resources on wildcard"))
	}

	lookupContext := parentStream.Context()
	limits := newLimitTracker(req.OptionalLimit)
	reachableResourcesCursor := req.OptionalCursor

	// Loop until the limit has been exhausted or no additional reachable resources are found (see below)
	for !limits.hasExhaustedLimit() {
		errCanceledBecauseNoAdditionalResourcesNeeded := errors.New("canceled because no additional reachable resources are needed")

		// Create a new context for just the reachable resources. This is necessary because we don't want the cancelation
		// of the reachable resources to cancel the lookup resources. The checking stream manually cancels the reachable
		// resources context once the expected number of results has been reached.
		reachableContext, cancelReachable := branchContext(lookupContext)

		// Create a new handling stream that consumes the reachable resources results and publishes them
		// to the parent stream, as found resources if they are properly checked.
		checkingStream := newCheckingResourceStream(lookupContext, reachableContext, func() {
			cancelReachable(errCanceledBecauseNoAdditionalResourcesNeeded)
		}, req, cl.c, parentStream, limits, cl.concurrencyLimit)

		err := cl.r.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
			ResourceRelation: req.ObjectRelation,
			SubjectRelation: &core.RelationReference{
				Namespace: req.Subject.Namespace,
				Relation:  req.Subject.Relation,
			},
			SubjectIds:     []string{req.Subject.ObjectId},
			Metadata:       req.Metadata,
			OptionalCursor: reachableResourcesCursor,
		}, checkingStream)
		if err != nil {
			// If the reachable resources was canceled explicitly by the checking stream because the limit has been
			// reached, then this error can safely be ignored. Otherwise, it must be returned.
			isAllowedCancelErr := errors.Is(context.Cause(reachableContext), errCanceledBecauseNoAdditionalResourcesNeeded)
			if !isAllowedCancelErr {
				return err
			}
		}

		reachableCount, newCursor, err := checkingStream.waitForPublishing()
		if err != nil {
			return err
		}

		reachableResourcesCursor = newCursor
		if reachableCount == 0 {
			return nil
		}
	}

	return nil
}
