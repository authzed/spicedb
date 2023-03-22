package caching

import (
	"context"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

const errMessage = "fake delegate should never be called, call SetDelegate on the parent dispatcher"

type fakeDelegate struct{}

func (fd fakeDelegate) ReadyState() dispatch.ReadyState {
	return dispatch.ReadyState{}
}

func (fd fakeDelegate) Close() error {
	return spiceerrors.MustBugf(errMessage)
}

func (fd fakeDelegate) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	return &v1.DispatchCheckResponse{}, spiceerrors.MustBugf(errMessage)
}

func (fd fakeDelegate) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{}, spiceerrors.MustBugf(errMessage)
}

func (fd fakeDelegate) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	return &v1.DispatchLookupResponse{}, spiceerrors.MustBugf(errMessage)
}

func (fd fakeDelegate) DispatchReachableResources(req *v1.DispatchReachableResourcesRequest, stream dispatch.ReachableResourcesStream) error {
	return spiceerrors.MustBugf(errMessage)
}

func (fd fakeDelegate) DispatchLookupSubjects(req *v1.DispatchLookupSubjectsRequest, stream dispatch.LookupSubjectsStream) error {
	return spiceerrors.MustBugf(errMessage)
}

var _ dispatch.Dispatcher = fakeDelegate{}
