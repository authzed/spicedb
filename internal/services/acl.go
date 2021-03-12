package services

import (
	"context"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/graph"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/zookie"
)

type aclServer struct {
	api.UnimplementedACLServiceServer

	ds       datastore.Datastore
	dispatch graph.Dispatcher
}

// NewACLServer creates an instance of the ACL server.
func NewACLServer(ds datastore.Datastore, dispatch graph.Dispatcher) api.ACLServiceServer {
	s := &aclServer{ds: ds, dispatch: dispatch}
	return s
}

func (as *aclServer) Write(ctxt context.Context, req *api.WriteRequest) (*api.WriteResponse, error) {
	revision, err := as.ds.WriteTuples(req.WriteConditions, req.Updates)
	switch err {
	case datastore.ErrPreconditionFailed:
		return nil, status.Errorf(codes.FailedPrecondition, "A write precondition failed.")
	case nil:
		return &api.WriteResponse{
			Revision: zookie.NewFromRevision(revision),
		}, nil
	default:
		log.Printf("Unknown error writing tuples: %s", err)
		return nil, status.Errorf(codes.Unknown, "Unknown error.")
	}
}

func (as *aclServer) Read(ctxt context.Context, req *api.ReadRequest) (*api.ReadResponse, error) {
	// TODO load the revision from the request or datastore.
	atRevision := ^uint64(0) - 1

	var allTuplesetResults []*api.ReadResponse_Tupleset

	for _, tuplesetFilter := range req.Tuplesets {
		queryBuilder := as.ds.QueryTuples(tuplesetFilter.Namespace, atRevision)
		for _, filter := range tuplesetFilter.Filters {
			switch filter {
			case api.RelationTupleFilter_OBJECT_ID:
				queryBuilder = queryBuilder.WithObjectID(tuplesetFilter.ObjectId)
			case api.RelationTupleFilter_RELATION:
				queryBuilder = queryBuilder.WithRelation(tuplesetFilter.Relation)
			case api.RelationTupleFilter_USERSET:
				queryBuilder = queryBuilder.WithUserset(tuplesetFilter.Userset)
			default:
				return nil, status.Errorf(codes.InvalidArgument, "unknown tupleset filter type: %s", filter)
			}
		}

		tupleIterator, err := queryBuilder.Execute()
		if err != nil {
			// TODO switch on known error types here
			return nil, status.Errorf(codes.Internal, "unable to retrieve tuples: %s", err)
		}

		defer tupleIterator.Close()

		tuplesetResult := &api.ReadResponse_Tupleset{}
		for tuple := tupleIterator.Next(); tuple != nil; tuple = tupleIterator.Next() {
			tuplesetResult.Tuples = append(tuplesetResult.Tuples, tuple)
		}
		if tupleIterator.Err() != nil {
			return nil, status.Errorf(codes.Internal, "error when reading tuples: %s", err)
		}

		allTuplesetResults = append(allTuplesetResults, tuplesetResult)
	}

	return &api.ReadResponse{
		Tuplesets: allTuplesetResults,
		Revision:  zookie.NewFromRevision(atRevision),
	}, nil
}

func (as *aclServer) Check(ctx context.Context, req *api.CheckRequest) (*api.CheckResponse, error) {
	// TODO load the revision from the request or datastore.
	atRevision := ^uint64(0) - 1

	cr := as.dispatch.Check(ctx, graph.CheckRequest{
		Start:      req.TestUserset,
		Goal:       req.User.GetUserset(),
		AtRevision: atRevision,
	})

	switch cr.Err {
	case graph.ErrNamespaceNotFound:
		fallthrough
	case graph.ErrRelationNotFound:
		return nil, status.Errorf(codes.FailedPrecondition, "error when performing check: %s", cr.Err)
	case graph.ErrRequestCanceled:
		return nil, status.Errorf(codes.Canceled, "error when performing check: %s", cr.Err)
	case nil:
		break
	default:
		return nil, status.Errorf(codes.Unknown, "error when performing check: %s", cr.Err)
	}

	membership := api.CheckResponse_NOT_MEMBER
	if cr.IsMember {
		membership = api.CheckResponse_MEMBER
	}

	return &api.CheckResponse{
		IsMember:   cr.IsMember,
		Revision:   zookie.NewFromRevision(atRevision),
		Membership: membership,
	}, nil
}
