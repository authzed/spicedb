package services

import (
	"context"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

type aclServer struct {
	api.UnimplementedACLServiceServer

	ds datastore.TupleDatastore
}

// NewACLServer creates an instance of the ACL server.
func NewACLServer(ds datastore.TupleDatastore) api.ACLServiceServer {
	s := &aclServer{ds: ds}
	return s
}

func (as *aclServer) Write(ctxt context.Context, req *api.WriteRequest) (*api.WriteResponse, error) {
	_, err := as.ds.WriteTuples(req.WriteConditions, req.Updates)
	switch err {
	case datastore.ErrPreconditionFailed:
		return nil, status.Errorf(codes.FailedPrecondition, "A write precondition failed.")
	case nil:
		return &api.WriteResponse{
			Revision: &api.Zookie{
				Token: "not implemented",
			},
		}, nil
	default:
		log.Printf("Unknown error writing tuples: %s", err)
		return nil, status.Errorf(codes.Unknown, "Unknown error.")
	}
}
