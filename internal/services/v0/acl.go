package v0

import (
	"context"
	"errors"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/zookie"
)

type aclServer struct {
	v0.UnimplementedACLServiceServer

	ds           datastore.Datastore
	nsm          namespace.Manager
	dispatch     graph.Dispatcher
	defaultDepth uint32
}

const (
	maxUInt16          = int(^uint16(0))
	lookupDefaultLimit = uint32(25)
	lookupMaximumLimit = uint32(100)

	DepthRemainingHeader = "authzed-depth-remaining"
	ForcedRevisionHeader = "authzed-forced-revision"
)

var (
	errInvalidZookie         = errors.New("invalid revision requested")
	errInvalidDepthRemaining = fmt.Errorf("invalid %s header", DepthRemainingHeader)
	errInvalidForcedRevision = fmt.Errorf("invalid %s header", ForcedRevisionHeader)
)

// NewACLServer creates an instance of the ACL server.
func NewACLServer(ds datastore.Datastore, nsm namespace.Manager, dispatch graph.Dispatcher, defaultDepth uint32) v0.ACLServiceServer {
	s := &aclServer{ds: ds, nsm: nsm, dispatch: dispatch, defaultDepth: defaultDepth}
	return s
}

func (as *aclServer) Write(ctx context.Context, req *v0.WriteRequest) (*v0.WriteResponse, error) {
	err := req.Validate()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	}

	for _, mutation := range req.Updates {
		err := validateTupleWrite(ctx, mutation.Tuple, as.nsm)
		if err != nil {
			return nil, rewriteACLError(err)
		}
	}

	revision, err := as.ds.WriteTuples(ctx, req.WriteConditions, req.Updates)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	return &v0.WriteResponse{
		Revision: zookie.NewFromRevision(revision),
	}, nil
}

func (as *aclServer) Read(ctx context.Context, req *v0.ReadRequest) (*v0.ReadResponse, error) {
	err := req.Validate()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	}

	for _, tuplesetFilter := range req.Tuplesets {
		checkedRelation := false
		for _, filter := range tuplesetFilter.Filters {
			switch filter {
			case v0.RelationTupleFilter_OBJECT_ID:
				if tuplesetFilter.ObjectId == "" {
					return nil, status.Errorf(
						codes.InvalidArgument,
						"object ID filter specified but not object ID provided.",
					)
				}
			case v0.RelationTupleFilter_RELATION:
				if tuplesetFilter.Relation == "" {
					return nil, status.Errorf(
						codes.InvalidArgument,
						"relation filter specified but not relation provided.",
					)
				}
				if err := as.nsm.CheckNamespaceAndRelation(
					ctx,
					tuplesetFilter.Namespace,
					tuplesetFilter.Relation,
					false, // Disallow ellipsis
				); err != nil {
					return nil, rewriteACLError(err)
				}
				checkedRelation = true
			case v0.RelationTupleFilter_USERSET:
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

		if !checkedRelation {
			if err := as.nsm.CheckNamespaceAndRelation(
				ctx,
				tuplesetFilter.Namespace,
				datastore.Ellipsis,
				true, // Allow ellipsis
			); err != nil {
				return nil, rewriteACLError(err)
			}
		}
	}

	var atRevision decimal.Decimal
	if req.AtRevision != nil {
		// Read should attempt to use the exact revision requested
		decoded, err := zookie.DecodeRevision(req.AtRevision)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "bad request revision: %s", err)
		}

		atRevision = decoded
	} else {
		// No revision provided, we'll pick one
		var err error
		atRevision, err = as.ds.Revision(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "unable to pick request revision: %s", err)
		}
	}

	err = as.ds.CheckRevision(ctx, atRevision)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	var allTuplesetResults []*v0.ReadResponse_Tupleset

	for _, tuplesetFilter := range req.Tuplesets {
		queryBuilder := as.ds.QueryTuples(tuplesetFilter.Namespace, atRevision)
		for _, filter := range tuplesetFilter.Filters {
			switch filter {
			case v0.RelationTupleFilter_OBJECT_ID:
				queryBuilder = queryBuilder.WithObjectID(tuplesetFilter.ObjectId)
			case v0.RelationTupleFilter_RELATION:
				queryBuilder = queryBuilder.WithRelation(tuplesetFilter.Relation)
			case v0.RelationTupleFilter_USERSET:
				queryBuilder = queryBuilder.WithUserset(tuplesetFilter.Userset)
			default:
				return nil, status.Errorf(codes.InvalidArgument, "unknown tupleset filter type: %s", filter)
			}
		}

		tupleIterator, err := queryBuilder.Execute(ctx)
		if err != nil {
			return nil, rewriteACLError(err)
		}

		defer tupleIterator.Close()

		tuplesetResult := &v0.ReadResponse_Tupleset{}
		for tuple := tupleIterator.Next(); tuple != nil; tuple = tupleIterator.Next() {
			tuplesetResult.Tuples = append(tuplesetResult.Tuples, tuple)
		}
		if tupleIterator.Err() != nil {
			return nil, status.Errorf(codes.Internal, "error when reading tuples: %s", err)
		}

		allTuplesetResults = append(allTuplesetResults, tuplesetResult)
	}

	return &v0.ReadResponse{
		Tuplesets: allTuplesetResults,
		Revision:  zookie.NewFromRevision(atRevision),
	}, nil
}

func (as *aclServer) Check(ctx context.Context, req *v0.CheckRequest) (*v0.CheckResponse, error) {
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

func (as *aclServer) ContentChangeCheck(ctx context.Context, req *v0.ContentChangeCheckRequest) (*v0.CheckResponse, error) {
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
	atRevision decimal.Decimal,
	start *v0.ObjectAndRelation,
	goal *v0.ObjectAndRelation,
) (*v0.CheckResponse, error) {
	err := as.nsm.CheckNamespaceAndRelation(ctx, start.Namespace, start.Relation, false)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	err = as.nsm.CheckNamespaceAndRelation(ctx, goal.Namespace, goal.Relation, true)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	cr := as.dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		Metadata: &v1.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: as.defaultDepth,
		},
		ObjectAndRelation: start,
		Subject:           goal,
	})
	if cr.Err != nil {
		return nil, rewriteACLError(cr.Err)
	}

	var membership v0.CheckResponse_Membership
	switch cr.Resp.Membership {
	case v1.DispatchCheckResponse_MEMBER:
		membership = v0.CheckResponse_MEMBER
	case v1.DispatchCheckResponse_NOT_MEMBER:
		membership = v0.CheckResponse_NOT_MEMBER
	default:
		membership = v0.CheckResponse_UNKNOWN
	}

	return &v0.CheckResponse{
		IsMember:   membership == v0.CheckResponse_MEMBER,
		Revision:   zookie.NewFromRevision(atRevision),
		Membership: membership,
	}, nil
}

func (as *aclServer) Expand(ctx context.Context, req *v0.ExpandRequest) (*v0.ExpandResponse, error) {
	err := req.Validate()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	}

	err = as.nsm.CheckNamespaceAndRelation(ctx, req.Userset.Namespace, req.Userset.Relation, false)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	atRevision, err := as.pickBestRevision(ctx, req.AtRevision)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	resp := as.dispatch.DispatchExpand(ctx, &v1.DispatchExpandRequest{
		Metadata: &v1.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: as.defaultDepth,
		},
		ObjectAndRelation: req.Userset,
		ExpansionMode:     v1.DispatchExpandRequest_SHALLOW,
	})
	if resp.Err != nil {
		return nil, rewriteACLError(resp.Err)
	}

	return &v0.ExpandResponse{
		TreeNode: resp.Resp.TreeNode,
		Revision: zookie.NewFromRevision(atRevision),
	}, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (as *aclServer) Lookup(ctx context.Context, req *v0.LookupRequest) (*v0.LookupResponse, error) {
	err := req.Validate()
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)
	}

	err = as.nsm.CheckNamespaceAndRelation(ctx, req.User.Namespace, req.User.Relation, true)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	err = as.nsm.CheckNamespaceAndRelation(ctx, req.ObjectRelation.Namespace, req.ObjectRelation.Relation, false)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	atRevision, err := as.pickBestRevision(ctx, req.AtRevision)
	if err != nil {
		return nil, rewriteACLError(err)
	}

	limit := req.Limit
	if limit == 0 {
		limit = lookupDefaultLimit
	} else if limit > lookupMaximumLimit {
		limit = lookupMaximumLimit
	}

	resp := as.dispatch.DispatchLookup(ctx, &v1.DispatchLookupRequest{
		Metadata: &v1.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: as.defaultDepth,
		},
		ObjectRelation: req.ObjectRelation,
		Subject:        req.User,
		Limit:          limit,
		DirectStack:    nil,
		TtuStack:       nil,
	})
	if resp.Err != nil {
		return nil, rewriteACLError(resp.Err)
	}

	resolvedObjectIDs := []string{}
	for _, found := range resp.Resp.ResolvedOnrs {
		if found.Namespace != req.ObjectRelation.Namespace {
			return nil, rewriteACLError(fmt.Errorf("got invalid resolved object %v (expected %v)", found, req.ObjectRelation))
		}

		resolvedObjectIDs = append(resolvedObjectIDs, found.ObjectId)
	}

	return &v0.LookupResponse{
		Revision:          zookie.NewFromRevision(atRevision),
		ResolvedObjectIds: resolvedObjectIDs,
	}, nil
}

func (as *aclServer) pickBestRevision(ctx context.Context, requested *v0.Zookie) (decimal.Decimal, error) {
	// Check if our caller has already picked a revision for us
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if forcedRevisions := md.Get(ForcedRevisionHeader); len(forcedRevisions) > 0 {
			if len(forcedRevisions) > 1 {
				return decimal.Zero, errInvalidForcedRevision
			}

			// We have a revision header, parse it
			parsedRevision, err := decimal.NewFromString(forcedRevisions[0])
			if err != nil {
				return decimal.Zero, errInvalidForcedRevision
			}

			return parsedRevision, nil
		}
	}

	// Calculate a revision as we see fit
	databaseRev, err := as.ds.Revision(ctx)
	if err != nil {
		return decimal.Zero, err
	}

	if requested != nil {
		requestedRev, err := zookie.DecodeRevision(requested)
		if err != nil {
			return decimal.Zero, errInvalidZookie
		}

		if requestedRev.GreaterThan(databaseRev) {
			return requestedRev, nil
		}
		return databaseRev, nil
	}

	return databaseRev, nil
}

func rewriteACLError(err error) error {
	var nsNotFoundError sharederrors.UnknownNamespaceError = nil
	var relNotFoundError sharederrors.UnknownRelationError = nil

	switch {
	case err == errInvalidZookie:
		return status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)

	case errors.As(err, &nsNotFoundError):
		fallthrough
	case errors.As(err, &relNotFoundError):
		fallthrough
	case errors.As(err, &datastore.ErrPreconditionFailed{}):
		return status.Errorf(codes.FailedPrecondition, "failed precondition: %s", err)

	case errors.As(err, &graph.ErrRequestCanceled{}):
		return status.Errorf(codes.Canceled, "request canceled: %s", err)

	case errors.As(err, &graph.ErrAlwaysFail{}):
		fallthrough

	case errors.As(err, &datastore.ErrInvalidRevision{}):
		return status.Errorf(codes.OutOfRange, "invalid zookie: %s", err)

	case errors.As(err, &datastore.ErrReadOnly{}):
		return serviceerrors.ErrServiceReadOnly

	default:
		if _, ok := err.(invalidRelationError); ok {
			return status.Errorf(codes.InvalidArgument, "%s", err.Error())
		}

		log.Err(err)
		return err
	}
}
