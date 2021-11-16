package caching

import (
	"context"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
)

const errMessage = "fake delegate should never be called, call SetDelegate on the parent dispatcher"

type fakeDelegate struct{}

func (fd fakeDelegate) Close() error {
	panic(errMessage)
}

func (fd fakeDelegate) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	panic(errMessage)
}

func (fd fakeDelegate) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	panic(errMessage)
}

func (fd fakeDelegate) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	panic(errMessage)
}

var _ dispatch.Dispatcher = fakeDelegate{}
