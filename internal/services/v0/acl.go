package v0

import (
	"context"
	"errors"
	"fmt"
	"sync"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"
	grpcmw "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/handwrittenvalidation"
	"github.com/authzed/spicedb/internal/middleware/usagemetrics"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/services/serviceerrors"
	"github.com/authzed/spicedb/internal/services/shared"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zookie"
)

type aclServer struct {
	v0.UnimplementedACLServiceServer
	shared.WithUnaryServiceSpecificInterceptor

	dispatch     dispatch.Dispatcher
	defaultDepth uint32
}

const (
	lookupDefaultLimit = uint32(25)
	lookupMaximumLimit = uint32(100)
)

var errInvalidZookie = errors.New("invalid revision requested")

// NewACLServer creates an instance of the ACL server.
func NewACLServer(dispatch dispatch.Dispatcher, defaultDepth uint32) v0.ACLServiceServer {
	middleware := []grpc.UnaryServerInterceptor{
		usagemetrics.UnaryServerInterceptor(),
		handwrittenvalidation.UnaryServerInterceptor,
	}

	middleware = append(middleware, grpcutil.DefaultUnaryMiddleware...)

	s := &aclServer{
		dispatch:     dispatch,
		defaultDepth: defaultDepth,
		WithUnaryServiceSpecificInterceptor: shared.WithUnaryServiceSpecificInterceptor{
			Unary: grpcmw.ChainUnaryServer(middleware...),
		},
	}
	return s
}

func (as *aclServer) Write(ctx context.Context, req *v0.WriteRequest) (*v0.WriteResponse, error) {
	preconditions := make([]*v1.Precondition, 0, len(req.WriteConditions))
	for _, cond := range req.WriteConditions {
		preconditions = append(preconditions, &v1.Precondition{
			Operation: v1.Precondition_OPERATION_MUST_MATCH,
			Filter:    tuple.MustToFilter(core.ToCoreRelationTuple(cond)),
		})
	}

	mutations := make([]*v1.RelationshipUpdate, 0, len(req.Updates))
	for _, mut := range req.Updates {
		mutations = append(mutations, tuple.UpdateToRelationshipUpdate(core.ToCoreRelationTupleUpdate(mut)))
	}

	ds := datastoremw.MustFromContext(ctx)
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		for _, mutation := range req.Updates {
			err := validateTupleWrite(ctx, mutation.Tuple, rwt)
			if err != nil {
				return err
			}
		}
		err := shared.CheckPreconditions(ctx, rwt, preconditions)
		if err != nil {
			return err
		}

		return rwt.WriteRelationships(mutations)
	})
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		// One request per precondition, and one request for the actual writing of tuples.
		DispatchCount: uint32(len(preconditions)) + 1,
	})

	return &v0.WriteResponse{
		Revision: core.ToV0Zookie(zookie.NewFromRevision(revision)),
	}, nil
}

func (as *aclServer) Read(ctx context.Context, req *v0.ReadRequest) (*v0.ReadResponse, error) {
	atRevision, _ := consistency.MustRevisionFromContext(ctx)
	ds := datastoremw.MustFromContext(ctx)
	reader := ds.SnapshotReader(atRevision)

	errG, groupCtx := errgroup.WithContext(ctx)
	for _, tuplesetFilter := range req.Tuplesets {
		tuplesetFilter := tuplesetFilter
		errG.Go(func() error {
			checkedRelation := false
			for _, filter := range tuplesetFilter.Filters {
				switch filter {
				case v0.RelationTupleFilter_OBJECT_ID:
					if tuplesetFilter.ObjectId == "" {
						return status.Errorf(
							codes.InvalidArgument,
							"object ID filter specified but not object ID provided.",
						)
					}
				case v0.RelationTupleFilter_RELATION:
					if tuplesetFilter.Relation == "" {
						return status.Errorf(
							codes.InvalidArgument,
							"relation filter specified but not relation provided.",
						)
					}
					if err := namespace.CheckNamespaceAndRelation(
						groupCtx,
						tuplesetFilter.Namespace,
						tuplesetFilter.Relation,
						false, // Disallow ellipsis
						reader,
					); err != nil {
						return err
					}
					checkedRelation = true
				case v0.RelationTupleFilter_USERSET:
					if tuplesetFilter.Userset == nil {
						return status.Errorf(
							codes.InvalidArgument,
							"userset filter specified but not userset provided.",
						)
					}
				default:
					return status.Errorf(
						codes.InvalidArgument,
						"unknown tupleset filter type: %s",
						filter,
					)
				}
			}

			if !checkedRelation {
				if err := namespace.CheckNamespaceAndRelation(
					groupCtx,
					tuplesetFilter.Namespace,
					datastore.Ellipsis,
					true, // Allow ellipsis
					reader,
				); err != nil {
					return err
				}
			}
			return nil
		})
	}

	errG.Go(func() error {
		return ds.CheckRevision(groupCtx, atRevision)
	})
	if err := errG.Wait(); err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	queryG, queryCtx := errgroup.WithContext(ctx)
	allTuplesetResults := make([]*v0.ReadResponse_Tupleset, 0, len(req.Tuplesets))
	resultsMu := sync.Mutex{}
	for _, tuplesetFilter := range req.Tuplesets {
		tuplesetFilter := tuplesetFilter
		queryG.Go(func() error {
			queryFilter := &v1.RelationshipFilter{
				ResourceType: tuplesetFilter.Namespace,
			}
			for _, filter := range tuplesetFilter.Filters {
				switch filter {
				case v0.RelationTupleFilter_OBJECT_ID:
					queryFilter.OptionalResourceId = tuplesetFilter.ObjectId
				case v0.RelationTupleFilter_RELATION:
					queryFilter.OptionalRelation = tuplesetFilter.Relation
				case v0.RelationTupleFilter_USERSET:
					queryFilter.OptionalSubjectFilter = tuple.UsersetToSubjectFilter(core.ToCoreObjectAndRelation(tuplesetFilter.Userset))
				default:
					return status.Errorf(codes.InvalidArgument, "unknown tupleset filter type: %s", filter)
				}
			}

			tupleIterator, err := reader.QueryRelationships(queryCtx, queryFilter)
			if err != nil {
				return err
			}

			defer tupleIterator.Close()

			tuplesetResult := &v0.ReadResponse_Tupleset{}
			for tuple := tupleIterator.Next(); tuple != nil; tuple = tupleIterator.Next() {
				tuplesetResult.Tuples = append(tuplesetResult.Tuples, core.ToV0RelationTuple(tuple))
			}
			if tupleIterator.Err() != nil {
				return status.Errorf(codes.Internal, "error when reading tuples: %s", err)
			}

			resultsMu.Lock()
			defer resultsMu.Unlock()
			allTuplesetResults = append(allTuplesetResults, tuplesetResult)

			return nil
		})
	}
	if err := queryG.Wait(); err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	usagemetrics.SetInContext(ctx, &dispatchv1.ResponseMeta{
		DispatchCount: uint32(len(allTuplesetResults)),
	})

	return &v0.ReadResponse{
		Tuplesets: allTuplesetResults,
		Revision:  core.ToV0Zookie(zookie.NewFromRevision(atRevision)),
	}, nil
}

func (as *aclServer) Check(ctx context.Context, req *v0.CheckRequest) (*v0.CheckResponse, error) {
	atRevision, _ := consistency.MustRevisionFromContext(ctx)
	return as.commonCheck(ctx, atRevision, core.ToCoreObjectAndRelation(req.TestUserset), core.ToCoreObjectAndRelation(req.User.GetUserset()))
}

func (as *aclServer) ContentChangeCheck(ctx context.Context, req *v0.ContentChangeCheckRequest) (*v0.CheckResponse, error) {
	// the implementation is the same as `Check`, but the consistency middleware will set the revision to the head
	// revision for this request, so it is always fully consistent.
	atRevision, _ := consistency.MustRevisionFromContext(ctx)
	return as.commonCheck(ctx, atRevision, core.ToCoreObjectAndRelation(req.TestUserset), core.ToCoreObjectAndRelation(req.User.GetUserset()))
}

func (as *aclServer) commonCheck(
	ctx context.Context,
	atRevision decimal.Decimal,
	start *core.ObjectAndRelation,
	goal *core.ObjectAndRelation,
) (*v0.CheckResponse, error) {
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	// Perform our preflight checks in parallel
	errG, checksCtx := errgroup.WithContext(ctx)
	errG.Go(func() error {
		return namespace.CheckNamespaceAndRelation(
			checksCtx,
			start.Namespace,
			start.Relation,
			false,
			ds,
		)
	})
	errG.Go(func() error {
		return namespace.CheckNamespaceAndRelation(
			checksCtx,
			goal.Namespace,
			goal.Relation,
			true,
			ds,
		)
	})
	if err := errG.Wait(); err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	cr, err := as.dispatch.DispatchCheck(ctx, &dispatchv1.DispatchCheckRequest{
		Metadata: &dispatchv1.ResolverMeta{
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
	case dispatchv1.DispatchCheckResponse_MEMBER:
		membership = v0.CheckResponse_MEMBER
	case dispatchv1.DispatchCheckResponse_NOT_MEMBER:
		membership = v0.CheckResponse_NOT_MEMBER
	default:
		membership = v0.CheckResponse_UNKNOWN
	}

	return &v0.CheckResponse{
		IsMember:   membership == v0.CheckResponse_MEMBER,
		Revision:   core.ToV0Zookie(zookie.NewFromRevision(atRevision)),
		Membership: membership,
	}, nil
}

func (as *aclServer) Expand(ctx context.Context, req *v0.ExpandRequest) (*v0.ExpandResponse, error) {
	atRevision, _ := consistency.MustRevisionFromContext(ctx)
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	err := namespace.CheckNamespaceAndRelation(ctx, req.Userset.Namespace, req.Userset.Relation, false, ds)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	resp, err := as.dispatch.DispatchExpand(ctx, &dispatchv1.DispatchExpandRequest{
		Metadata: &dispatchv1.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: as.defaultDepth,
		},
		ObjectAndRelation: core.ToCoreObjectAndRelation(req.Userset),
		ExpansionMode:     dispatchv1.DispatchExpandRequest_SHALLOW,
	})
	usagemetrics.SetInContext(ctx, resp.Metadata)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	return &v0.ExpandResponse{
		TreeNode: core.ToV0RelationTupleTreeNode(resp.TreeNode),
		Revision: core.ToV0Zookie(zookie.NewFromRevision(atRevision)),
	}, nil
}

func (as *aclServer) Lookup(ctx context.Context, req *v0.LookupRequest) (*v0.LookupResponse, error) {
	atRevision, _ := consistency.MustRevisionFromContext(ctx)
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(atRevision)

	err := namespace.CheckNamespaceAndRelation(ctx, req.User.Namespace, req.User.Relation, true, ds)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	err = namespace.CheckNamespaceAndRelation(ctx, req.ObjectRelation.Namespace, req.ObjectRelation.Relation, false, ds)
	if err != nil {
		return nil, rewriteACLError(ctx, err)
	}

	limit := req.Limit
	if limit == 0 {
		limit = lookupDefaultLimit
	} else if limit > lookupMaximumLimit {
		limit = lookupMaximumLimit
	}

	resp, err := as.dispatch.DispatchLookup(ctx, &dispatchv1.DispatchLookupRequest{
		Metadata: &dispatchv1.ResolverMeta{
			AtRevision:     atRevision.String(),
			DepthRemaining: as.defaultDepth,
		},
		ObjectRelation: core.ToCoreRelationReference(req.ObjectRelation),
		Subject:        core.ToCoreObjectAndRelation(req.User),
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
		Revision:          core.ToV0Zookie(zookie.NewFromRevision(atRevision)),
		ResolvedObjectIds: resolvedObjectIDs,
	}, nil
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
	case errors.As(err, &shared.ErrPreconditionFailed{}):
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

		log.Ctx(ctx).Err(err).Msg("received unexpected error")
		return err
	}
}
