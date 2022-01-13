package v0

import (
	"context"
	"errors"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1_api "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zookie"
)

type aclServer struct {
	v0.UnimplementedACLServiceServer
	shared.WithUnaryServiceSpecificInterceptor

	ds           datastore.Datastore
	nsm          namespace.Manager
	dispatch     dispatch.Dispatcher
	defaultDepth uint32
}

const (
	lookupDefaultLimit = uint32(25)
	lookupMaximumLimit = uint32(100)
)

var errInvalidZookie = errors.New("invalid revision requested")

// NewACLServer creates an instance of the ACL server.
func NewACLServer(ds datastore.Datastore, nsm namespace.Manager, dispatch dispatch.Dispatcher, defaultDepth uint32) v0.ACLServiceServer {
	middleware := []grpc.UnaryServerInterceptor{
		usagemetrics.UnaryServerInterceptor(),
		handwrittenvalidation.UnaryServerInterceptor,
	}

	middleware = append(middleware, grpcutil.DefaultUnaryMiddleware...)

	s := &aclServer{
		ds:           ds,
		nsm:          nsm,
		dispatch:     dispatch,
		defaultDepth: defaultDepth,
		WithUnaryServiceSpecificInterceptor: shared.WithUnaryServiceSpecificInterceptor{
			Unary: grpcmw.ChainUnaryServer(middleware...),
		},
	}
	return s
}

func (as *aclServer) Write(ctx context.Context, req *v0.WriteRequest) (*v0.WriteResponse, error) {
	atRevision, err := as.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	for _, mutation := range req.Updates {
		err := validateTupleWrite(ctx, mutation.Tuple, as.nsm, atRevision)
		if err != nil {
			return nil, rewriteACLError(ctx, err)
		}
	}

	preconditions := make([]*v1_api.Precondition, 0, len(req.WriteConditions))
	for _, cond := range req.WriteConditions {
		preconditions = append(preconditions, &v1_api.Precondition{
			Operation: v1_api.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.MustToFilter(cond),
		})
	}

	mutations := make([]*v1_api.RelationshipUpdate, 0, len(req.Updates))
	for _, mut := range req.Updates {
		mutations = append(mutations, tuple.UpdateToRelationshipUpdate(mut))
	}

	usagemetrics.SetInContext(ctx, &v1.ResponseMeta{
		// One request per precondition, and one request for the actual writing of tuples.
		DispatchCount: uint32(len(preconditions)) + 1,
	})

	revision, err := as.ds.WriteTuples(ctx, preconditions, mutations)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	return &v0.WriteResponse{
		Revision: zookie.NewFromRevision(revision),
	}, nil
}

func (as *aclServer) Read(ctx context.Context, req *v0.ReadRequest) (*v0.ReadResponse, error) {
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
		atRevision, err = as.ds.OptimizedRevision(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "unable to pick request revision: %s", err)
		}
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
					atRevision,
				); err != nil {
					return nil, rewriteACLError(ctx, err)
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
				atRevision,
			); err != nil {
				return nil, rewriteACLError(ctx, err)
			}
		}
	}

	err := as.ds.CheckRevision(ctx, atRevision)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	allTuplesetResults := make([]*v0.ReadResponse_Tupleset, 0, len(req.Tuplesets))
	for _, tuplesetFilter := range req.Tuplesets {
		queryFilter := &v1_api.RelationshipFilter{
			ResourceType: tuplesetFilter.Namespace,
		}
		for _, filter := range tuplesetFilter.Filters {
			switch filter {
			case v0.RelationTupleFilter_OBJECT_ID:
				queryFilter.OptionalResourceId = tuplesetFilter.ObjectId
			case v0.RelationTupleFilter_RELATION:
				queryFilter.OptionalRelation = tuplesetFilter.Relation
			case v0.RelationTupleFilter_USERSET:
				queryFilter.OptionalSubjectFilter = tuple.UsersetToSubjectFilter(tuplesetFilter.Userset)
			default:
				return nil, status.Errorf(codes.InvalidArgument, "unknown tupleset filter type: %s", filter)
			}
		}

		tupleIterator, err := as.ds.QueryTuples(ctx, queryFilter, atRevision)
		if err != nil {
			return nil, rewriteACLError(ctx, err)
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

	usagemetrics.SetInContext(ctx, &v1.ResponseMeta{
		DispatchCount: uint32(len(allTuplesetResults)),
	})

	return &v0.ReadResponse{
		Tuplesets: allTuplesetResults,
		Revision:  zookie.NewFromRevision(atRevision),
	}, nil
}

func (as *aclServer) Check(ctx context.Context, req *v0.CheckRequest) (*v0.CheckResponse, error) {
	atRevision, err := as.pickBestRevision(ctx, req.AtRevision)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	return as.commonCheck(ctx, atRevision, req.TestUserset, req.User.GetUserset())
}

func (as *aclServer) ContentChangeCheck(ctx context.Context, req *v0.ContentChangeCheckRequest) (*v0.CheckResponse, error) {
	atRevision, err := as.ds.HeadRevision(ctx)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	return as.commonCheck(ctx, atRevision, req.TestUserset, req.User.GetUserset())
}

func (as *aclServer) commonCheck(
	ctx context.Context,
	atRevision decimal.Decimal,
	start *v0.ObjectAndRelation,
	goal *v0.ObjectAndRelation,
) (*v0.CheckResponse, error) {
	// Perform our preflight checks in parallel
	errG, checksCtx := errgroup.WithContext(ctx)
	errG.Go(func() error {
		return as.nsm.CheckNamespaceAndRelation(
			checksCtx,
			start.Namespace,
			start.Relation,
			false,
			atRevision,
		)
	})
	errG.Go(func() error {
		return as.nsm.CheckNamespaceAndRelation(
			checksCtx,
			goal.Namespace,
			goal.Relation,
			true,
			atRevision,
		)
	})
	if err := errG.Wait(); err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	cr, err := as.dispatch.DispatchCheck(ctx, &v1.DispatchCheckRequest{
		Metadata: &v1.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: as.defaultDepth,
		},
		ObjectAndRelation: start,
		Subject:           goal,
	})

	usagemetrics.SetInContext(ctx, cr.Metadata)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	var membership v0.CheckResponse_Membership
	switch cr.Membership {
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
	atRevision, err := as.pickBestRevision(ctx, req.AtRevision)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	err = as.nsm.CheckNamespaceAndRelation(ctx, req.Userset.Namespace, req.Userset.Relation, false, atRevision)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	resp, err := as.dispatch.DispatchExpand(ctx, &v1.DispatchExpandRequest{
		Metadata: &v1.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: as.defaultDepth,
		},
		ObjectAndRelation: req.Userset,
		ExpansionMode:     v1.DispatchExpandRequest_SHALLOW,
	})
	usagemetrics.SetInContext(ctx, resp.Metadata)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	return &v0.ExpandResponse{
		TreeNode: resp.TreeNode,
		Revision: zookie.NewFromRevision(atRevision),
	}, nil
}

func (as *aclServer) Lookup(ctx context.Context, req *v0.LookupRequest) (*v0.LookupResponse, error) {
	atRevision, err := as.pickBestRevision(ctx, req.AtRevision)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	err = as.nsm.CheckNamespaceAndRelation(ctx, req.User.Namespace, req.User.Relation, true, atRevision)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	err = as.nsm.CheckNamespaceAndRelation(ctx, req.ObjectRelation.Namespace, req.ObjectRelation.Relation, false, atRevision)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	limit := req.Limit
	if limit == 0 {
		limit = lookupDefaultLimit
	} else if limit > lookupMaximumLimit {
		limit = lookupMaximumLimit
	}

	resp, err := as.dispatch.DispatchLookup(ctx, &v1.DispatchLookupRequest{
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
	usagemetrics.SetInContext(ctx, resp.Metadata)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	resolvedObjectIDs := make([]string, 0, len(resp.ResolvedOnrs))
	for _, found := range resp.ResolvedOnrs {
		if found.Namespace != req.ObjectRelation.Namespace {
			return nil, rewriteACLError(
				ctx,
				fmt.Errorf("got invalid resolved object %v (expected %v)", found, req.ObjectRelation),
			)
		}

		resolvedObjectIDs = append(resolvedObjectIDs, found.ObjectId)
	}

	return &v0.LookupResponse{
		Revision:          zookie.NewFromRevision(atRevision),
		ResolvedObjectIds: resolvedObjectIDs,
	}, nil
}

func (as *aclServer) pickBestRevision(ctx context.Context, requested *v0.Zookie) (decimal.Decimal, error) {
	// Calculate a revision as we see fit
	databaseRev, err := as.ds.OptimizedRevision(ctx)
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

func rewriteACLError(ctx context.Context, err error) error {
	var nsNotFoundError sharederrors.UnknownNamespaceError
	var relNotFoundError sharederrors.UnknownRelationError

	switch {
	case errors.Is(err, errInvalidZookie):
		return status.Errorf(codes.InvalidArgument, "invalid argument: %s", err)

	case errors.As(err, &nsNotFoundError):
		fallthrough
	case errors.As(err, &relNotFoundError):
		fallthrough
	case errors.As(err, &datastore.ErrPreconditionFailed{}):
		return status.Errorf(codes.FailedPrecondition, "failed precondition: %s", err)

	case errors.As(err, &graph.ErrRequestCanceled{}):
		return status.Errorf(codes.Canceled, "request canceled: %s", err)

	case errors.As(err, &graph.ErrInvalidArgument{}):
		return status.Errorf(codes.InvalidArgument, "%s", err)

	case errors.As(err, &datastore.ErrInvalidRevision{}):
		return status.Errorf(codes.OutOfRange, "invalid zookie: %s", err)

	case errors.As(err, &datastore.ErrReadOnly{}):
		return serviceerrors.ErrServiceReadOnly

	case errors.As(err, &graph.ErrRelationMissingTypeInfo{}):
		return status.Errorf(codes.FailedPrecondition, "failed precondition: %s", err)

	case errors.As(err, &graph.ErrAlwaysFail{}):
		log.Ctx(ctx).Err(err)
		return status.Errorf(codes.Internal, "internal error: %s", err)

	default:
		if errors.As(err, &invalidRelationError{}) {
			return status.Errorf(codes.InvalidArgument, "%s", err)
		}

		log.Ctx(ctx).Err(err)
		return err
	}
}
