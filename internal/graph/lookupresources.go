package graph

import (
	"context"
	"errors"
	"sync"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datastore"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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

type collectingStream struct {
	req     ValidatedLookupResourcesRequest
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
		return spiceerrors.MustBugf("got nil result for LookupResources publish")
	}

	return nil
}

func (cl *CursoredLookupResources) LookupResources(
	req ValidatedLookupResourcesRequest,
	stream dispatch.LookupResourcesStream,
) error {
	if req.Subject.ObjectId == tuple.PublicWildcard {
		return NewErrInvalidArgument(errors.New("cannot perform lookup resources on wildcard"))
	}

	return nil
}
