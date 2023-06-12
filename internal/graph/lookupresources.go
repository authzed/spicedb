package graph

import (
	"context"
	"errors"

	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
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

// reachableResourcesLimit is a limit set on the reachable resources calls to ensure caching
// stores smaller chunks.
const reachableResourcesLimit = 1000

func (cl *CursoredLookupResources) LookupResources(
	req ValidatedLookupResourcesRequest,
	parentStream dispatch.LookupResourcesStream,
) error {
	if req.Subject.ObjectId == tuple.PublicWildcard {
		return NewErrInvalidArgument(errors.New("cannot perform lookup resources on wildcard"))
	}

	lookupContext := parentStream.Context()

	// Create a new context for just the reachable resources. This is necessary because we don't want the cancelation
	// of the reachable resources to cancel the lookup resources. We manually cancel the reachable resources context
	// ourselves once the lookup resources operation has completed.
	ds := datastoremw.MustFromContext(lookupContext)

	newContextForReachable := datastoremw.ContextWithDatastore(context.Background(), ds)
	reachableContext, cancelReachable := context.WithCancel(newContextForReachable)
	defer cancelReachable()

	limits, _ := newLimitTracker(lookupContext, req.OptionalLimit)
	reachableResourcesCursor := req.OptionalCursor

	// Loop until the limit has been exhausted or no additional reachable resources are found (see below)
	for !limits.hasExhaustedLimit() {
		// Create a new handling stream that consumes the reachable resources results and publishes them
		// to the parent stream, as found resources if they are properly checked.
		checkingStream := newCheckingResourceStream(lookupContext, reachableContext, req, cl.c, parentStream, limits, cl.concurrencyLimit)

		err := cl.r.DispatchReachableResources(&v1.DispatchReachableResourcesRequest{
			ResourceRelation: req.ObjectRelation,
			SubjectRelation: &core.RelationReference{
				Namespace: req.Subject.Namespace,
				Relation:  req.Subject.Relation,
			},
			SubjectIds:     []string{req.Subject.ObjectId},
			Metadata:       req.Metadata,
			OptionalCursor: reachableResourcesCursor,
			OptionalLimit:  reachableResourcesLimit,
		}, checkingStream)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}

		reachableCount, newCursor, err := checkingStream.waitForPublishing()
		if err != nil {
			return err
		}

		reachableResourcesCursor = newCursor

		if reachableCount < reachableResourcesLimit {
			return nil
		}
	}

	return nil
}
