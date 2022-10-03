package caveat

import (
	"context"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type caveatEvaluatorDispatcher struct {
	delegate dispatch.Dispatcher
}

func (c caveatEvaluatorDispatcher) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	return c.delegate.DispatchCheck(ctx, req)
}

func (c caveatEvaluatorDispatcher) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	// TODO not implemented
	panic("not implemented")
}

func (c caveatEvaluatorDispatcher) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	// TODO not implemented
	panic("not implemented")
}

func (c caveatEvaluatorDispatcher) DispatchReachableResources(req *v1.DispatchReachableResourcesRequest, stream dispatch.ReachableResourcesStream) error {
	// TODO not implemented
	panic("not implemented")
}

func (c caveatEvaluatorDispatcher) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	// TODO not implemented
	panic("not implemented")
}

func (c caveatEvaluatorDispatcher) Close() error {
	return c.delegate.Close()
}

func (c caveatEvaluatorDispatcher) IsReady() bool {
	return c.delegate.IsReady()
}

// NewCaveatEvaluatingDispatcher takes a dispatcher and evaluates
// the evaluates any potential caveated membership result.
func NewCaveatEvaluatingDispatcher(dispatch dispatch.Dispatcher) dispatch.Dispatcher {
	return &caveatEvaluatorDispatcher{delegate: dispatch}
}
