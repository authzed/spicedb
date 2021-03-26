package services

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/rs/zerolog/log"
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

	depthRemainingHeader = "authzed-depth-remaining"
)

var (
	errInvalidZookie         = errors.New("invalid revision requested")
	errInvalidDepthRemaining = fmt.Errorf("invalid %s header", depthRemainingHeader)
)

// NewACLServer creates an instance of the ACL server.
func NewACLServer(ds datastore.Datastore, dispatch graph.Dispatcher, defaultDepth uint16) api.ACLServiceServer {
	s := &aclServer{ds: ds, dispatch: dispatch, defaultDepth: defaultDepth}
	return s
}

func (as *aclServer) Write(ctx context.Context, req *api.WriteRequest) (*api.WriteResponse, error) {
	err := req.Validate()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	}

	revision, err := as.ds.WriteTuples(req.WriteConditions, req.Updates)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	return &api.WriteResponse{
		Revision: zookie.NewFromRevision(revision),
	}, nil
}

func (as *aclServer) Read(ctx context.Context, req *api.ReadRequest) (*api.ReadResponse, error) {
	err := req.Validate()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	}

	for _, tuplesetFilter := range req.Tuplesets {
		for _, filter := range tuplesetFilter.Filters {
			switch filter {
			case api.RelationTupleFilter_OBJECT_ID:
				if tuplesetFilter.ObjectId == "" {
					return nil, status.Errorf(
						codes.InvalidArgument,
						"object ID filter specified but not object ID provided.",
					)
				}
			case api.RelationTupleFilter_RELATION:
				if tuplesetFilter.Relation == "" {
					return nil, status.Errorf(
						codes.InvalidArgument,
						"relation filter specified but not relation provided.",
					)
				}
			case api.RelationTupleFilter_USERSET:
				if tuplesetFilter.Userset == nil {
					return nil, status.Errorf(
						codes.InvalidArgument,
						"userset filter specified but not userset provided.",
					)
				}
			default:
				return nil, status.Errorf(
					codes.InvalidArgument,
					"unknown tupleset filter type: %s",
					filter,
				)
			}
		}
	}

	var atRevision uint64
	if req.AtRevision != nil {
		// Read should attempt to use the exact revision requested
		decoded, err := zookie.Decode(req.AtRevision)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "bad request revision: %s", err)
		}

		atRevision = decoded.GetV1().Revision
	} else {
		// No revision provided, we'll pick one
		var err error
		atRevision, err = as.ds.Revision(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "unable to pick request revision: %s", err)
		}
	}

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
	err := req.Validate()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	}

	atRevision, err := as.pickBestRevision(ctx, req.AtRevision)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	return as.commonCheck(ctx, atRevision, req.TestUserset, req.User.GetUserset())
}

func (as *aclServer) ContentChangeCheck(ctx context.Context, req *api.ContentChangeCheckRequest) (*api.CheckResponse, error) {
	err := req.Validate()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	}

	atRevision, err := as.ds.SyncRevision(ctx)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	return as.commonCheck(ctx, atRevision, req.TestUserset, req.User.GetUserset())
}

func (as *aclServer) commonCheck(
	ctx context.Context,
	atRevision uint64,
	start *api.ObjectAndRelation,
	goal *api.ObjectAndRelation,
) (*api.CheckResponse, error) {
	depth, err := as.calculateRequestDepth(ctx)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	cr := as.dispatch.Check(ctx, graph.CheckRequest{
		Start:          start,
		Goal:           goal,
		AtRevision:     atRevision,
		DepthRemaining: depth,
	})
	if cr.Err != nil {
		return nil, rewriteACLError(cr.Err)
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
	err := req.Validate()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	}

	atRevision, err := as.pickBestRevision(ctx, req.AtRevision)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	depth, err := as.calculateRequestDepth(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	resp := as.dispatch.Expand(ctx, graph.ExpandRequest{
		Start:          req.Userset,
		AtRevision:     atRevision,
		DepthRemaining: depth,
	})
	if resp.Err != nil {
		return nil, rewriteACLError(resp.Err)
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

			// We have one and only one depth remaining header, let's check the format
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

func (as *aclServer) pickBestRevision(ctx context.Context, requested *api.Zookie) (uint64, error) {
	databaseRev, err := as.ds.Revision(ctx)
	if err != nil {
		return 0, err
	}

	if requested != nil {
		decoded, err := zookie.Decode(requested)
		if err != nil {
			return 0, errInvalidZookie
		}

		requestedRev := decoded.GetV1().Revision
		if requestedRev > databaseRev {
			return requestedRev, nil
		}
		return databaseRev, nil
	}

	return databaseRev, nil
}

func rewriteACLError(err error) error {
	switch err {
	case errInvalidZookie:
		return status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	case graph.ErrNamespaceNotFound:
		fallthrough
	case graph.ErrRelationNotFound:
		return status.Errorf(codes.FailedPrecondition, "data error: %s", err)
	case graph.ErrRequestCanceled:
		return status.Errorf(codes.Canceled, "request canceled: %s", err)
	case datastore.ErrPreconditionFailed:
		return status.Errorf(codes.FailedPrecondition, "failed precondition: %s", err)
	case graph.ErrAlwaysFail:
		fallthrough
	default:
		log.Err(err)
		return err
	}
}
