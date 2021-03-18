package services

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/graph"
	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/zookie"
)

type aclServer struct {
	api.UnimplementedACLServiceServer

	ds           datastore.Datastore
	dispatch     graph.Dispatcher
	defaultDepth uint16
}

const (
	maxUInt16 = int(^uint16(0))

	depthRemainingHeader = "depth-remaining"

	errInvalidCheck  = "error when performing check: %s"
	errInvalidExpand = "error when performing expand: %s"
)

var errInvalidDepthRemaining = fmt.Errorf("invalid %s header", depthRemainingHeader)

// NewACLServer creates an instance of the ACL server.
func NewACLServer(ds datastore.Datastore, dispatch graph.Dispatcher, defaultDepth uint16) api.ACLServiceServer {
	s := &aclServer{ds: ds, dispatch: dispatch, defaultDepth: defaultDepth}
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
	atRevision := uint64(9223372036854775800)

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
	atRevision := uint64(9223372036854775800)

	depth, err := as.calculateRequestDepth(ctx)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, errInvalidExpand, err)
	}

	cr := as.dispatch.Check(ctx, graph.CheckRequest{
		Start:          req.TestUserset,
		Goal:           req.User.GetUserset(),
		AtRevision:     atRevision,
		DepthRemaining: depth,
	})

	switch cr.Err {
	case graph.ErrNamespaceNotFound:
		fallthrough
	case graph.ErrRelationNotFound:
		return nil, status.Errorf(codes.FailedPrecondition, errInvalidCheck, cr.Err)
	case graph.ErrRequestCanceled:
		return nil, status.Errorf(codes.Canceled, errInvalidCheck, cr.Err)
	case nil:
		break
	default:
		return nil, status.Errorf(codes.Unknown, errInvalidCheck, cr.Err)
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

func (as *aclServer) Expand(ctx context.Context, req *api.ExpandRequest) (*api.ExpandResponse, error) {
	// TODO load the revision from the request or datastore.
	atRevision := uint64(9223372036854775800)

	depth, err := as.calculateRequestDepth(ctx)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, errInvalidExpand, err)
	}

	resp := as.dispatch.Expand(ctx, graph.ExpandRequest{
		Start:          req.Userset,
		AtRevision:     atRevision,
		DepthRemaining: depth,
	})

	switch resp.Err {
	case graph.ErrNamespaceNotFound:
		fallthrough
	case graph.ErrRelationNotFound:
		return nil, status.Errorf(codes.FailedPrecondition, errInvalidExpand, resp.Err)
	case graph.ErrRequestCanceled:
		return nil, status.Errorf(codes.Canceled, errInvalidExpand, resp.Err)
	case nil:
		break
	default:
		return nil, status.Errorf(codes.Unknown, errInvalidExpand, resp.Err)
	}

	return &api.ExpandResponse{
		TreeNode: resp.Tree,
		Revision: zookie.NewFromRevision(atRevision),
	}, nil
}

func (as *aclServer) calculateRequestDepth(ctx context.Context) (uint16, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if matching := md.Get(depthRemainingHeader); len(matching) > 0 {
			if len(matching) > 1 {
				return 0, errInvalidDepthRemaining
			}

			// We have one and only one depth-remaining header, let's check the format
			decoded, err := strconv.Atoi(matching[0])
			if err != nil {
				return 0, errInvalidDepthRemaining
			}

			if decoded < 1 || decoded > maxUInt16 {
				return 0, errInvalidDepthRemaining
			}

			return uint16(decoded), nil
		}
	}

	return as.defaultDepth, nil
}
