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

func (fd fakeDelegate) DispatchCheck(_ context.Context, _ *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	return &v1.DispatchCheckResponse{}, spiceerrors.MustBugf(errMessage)
}

func (fd fakeDelegate) DispatchExpand(_ context.Context, _ *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return &v1.DispatchExpandResponse{}, spiceerrors.MustBugf(errMessage)
}

func (fd fakeDelegate) DispatchReachableResources(_ *v1.DispatchReachableResourcesRequest, _ dispatch.ReachableResourcesStream) error {
	return spiceerrors.MustBugf(errMessage)
}

func (fd fakeDelegate) DispatchLookupResources(_ *v1.DispatchLookupResourcesRequest, _ dispatch.LookupResourcesStream) error {
	return spiceerrors.MustBugf(errMessage)
}

func (fd fakeDelegate) DispatchLookupSubjects(_ *v1.DispatchLookupSubjectsRequest, _ dispatch.LookupSubjectsStream) error {
	return spiceerrors.MustBugf(errMessage)
}

var _ dispatch.Dispatcher = fakeDelegate{}
