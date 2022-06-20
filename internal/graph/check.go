package graph

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1_proto "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/generic/sync"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/dispatch"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// NewConcurrentChecker creates an instance of ConcurrentChecker.
func NewConcurrentChecker(d dispatch.Check) *ConcurrentChecker {
	return &ConcurrentChecker{d: d, concurrencyLimit: -1}
}

// ConcurrentChecker exposes a method to perform Check requests, and delegates subproblems to the
// provided dispatch.Check instance.
type ConcurrentChecker struct {
	d                dispatch.Check
	concurrencyLimit int
}

func onrEqual(lhs, rhs *core.ObjectAndRelation) bool {
	// Properties are sorted by highest to lowest cardinality to optimize for short-circuiting.
	return lhs.ObjectId == rhs.ObjectId && lhs.Relation == rhs.Relation && lhs.Namespace == rhs.Namespace
}

// ValidatedCheckRequest represents a request after it has been validated and parsed for internal
// consumption.
type ValidatedCheckRequest struct {
	*v1.DispatchCheckRequest
	Revision decimal.Decimal
}

// Check performs a check request with the provided request and context
func (cc *ConcurrentChecker) Check(ctx context.Context, req ValidatedCheckRequest, relation *core.Relation) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
	var directFunc CheckFunc

	if relation.GetTypeInformation() == nil && relation.GetUsersetRewrite() == nil {
		directFunc = checkError(fmt.Errorf("found relation `%s` without type information; to fix, please re-write your schema", relation.Name))
	} else {
		if req.Subject.ObjectId == tuple.PublicWildcard {
			directFunc = checkError(NewErrInvalidArgument(errors.New("cannot perform check on wildcard")))
		} else if onrEqual(req.Subject, req.ResourceAndRelation) {
			// If we have found the goal's ONR, then we know that the ONR is a member.
			directFunc = alwaysMember
		} else if relation.UsersetRewrite == nil {
			directFunc = cc.checkDirect(ctx, req)
		} else {
			directFunc = cc.checkUsersetRewrite(ctx, req, relation.UsersetRewrite)
		}
	}

	resp, err := union(ctx, []CheckFunc{directFunc}, cc.concurrencyLimit)
	if err != nil {
		return nil, err
	}

	return &v1.DispatchCheckResponse{
		Metadata:   addCallToResponseMetadata(resp.Metadata),
		Membership: resp.Membership,
	}, nil
}

func (cc *ConcurrentChecker) dispatch(req ValidatedCheckRequest) CheckFunc {
	return func(ctx context.Context) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
		log.Ctx(ctx).Trace().Object("dispatch", req).Send()
		return cc.d.DispatchCheck(ctx, req.DispatchCheckRequest)
	}
}

func onrEqualOrWildcard(tpl, target *core.ObjectAndRelation) bool {
	return onrEqual(tpl, target) || (tpl.Namespace == target.Namespace && tpl.ObjectId == tuple.PublicWildcard)
}

func (cc *ConcurrentChecker) checkDirect(ctx context.Context, req ValidatedCheckRequest) CheckFunc {
	return func(ctx context.Context) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
		log.Ctx(ctx).Trace().Object("direct", req).Send()
		ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)

		// TODO(jschorr): Use type information to further optimize this query.
		it, err := ds.QueryRelationships(ctx, &v1_proto.RelationshipFilter{
			ResourceType:       req.ResourceAndRelation.Namespace,
			OptionalResourceId: req.ResourceAndRelation.ObjectId,
			OptionalRelation:   req.ResourceAndRelation.Relation,
		})
		if err != nil {
			return nil, NewCheckFailureErr(err, emptyMetadata)
		}
		defer it.Close()

		var requestsToDispatch []CheckFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			if onrEqualOrWildcard(tpl.Subject, req.Subject) {
				return &v1.DispatchCheckResponse{
					Metadata:   emptyMetadata,
					Membership: v1.DispatchCheckResponse_MEMBER,
				}, nil
			}
			if tpl.Subject.Relation != Ellipsis {
				// We need to recursively call check here, potentially changing namespaces
				requestsToDispatch = append(requestsToDispatch, cc.dispatch(ValidatedCheckRequest{
					&v1.DispatchCheckRequest{
						ResourceAndRelation: tpl.Subject,
						Subject:             req.Subject,

						Metadata: decrementDepth(req.Metadata),
					},
					req.Revision,
				}))
			}
		}
		if it.Err() != nil {
			return nil, NewCheckFailureErr(it.Err(), emptyMetadata)
		}
		return union(ctx, requestsToDispatch, cc.concurrencyLimit)
	}
}

func (cc *ConcurrentChecker) checkUsersetRewrite(ctx context.Context, req ValidatedCheckRequest, usr *core.UsersetRewrite) CheckFunc {
	switch rw := usr.RewriteOperation.(type) {
	case *core.UsersetRewrite_Union:
		return cc.checkSetOperation(ctx, req, rw.Union, union)
	case *core.UsersetRewrite_Intersection:
		return cc.checkSetOperation(ctx, req, rw.Intersection, all)
	case *core.UsersetRewrite_Exclusion:
		return cc.checkSetOperation(ctx, req, rw.Exclusion, difference)
	default:
		return AlwaysFail
	}
}

func (cc *ConcurrentChecker) checkSetOperation(ctx context.Context, req ValidatedCheckRequest, so *core.SetOperation, reducer Reducer) CheckFunc {
	var requests []CheckFunc
	for _, childOneof := range so.Child {
		switch child := childOneof.ChildType.(type) {
		case *core.SetOperation_Child_XThis:
			return checkError(errors.New("use of _this is unsupported; please rewrite your schema"))
		case *core.SetOperation_Child_ComputedUserset:
			requests = append(requests, cc.checkComputedUserset(ctx, req, child.ComputedUserset, nil))
		case *core.SetOperation_Child_UsersetRewrite:
			requests = append(requests, cc.checkUsersetRewrite(ctx, req, child.UsersetRewrite))
		case *core.SetOperation_Child_TupleToUserset:
			requests = append(requests, cc.checkTupleToUserset(ctx, req, child.TupleToUserset))
		case *core.SetOperation_Child_XNil:
			requests = append(requests, notMember)
		default:
			return checkError(fmt.Errorf("unknown set operation child `%T` in check", child))
		}
	}
	return func(ctx context.Context) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
		log.Ctx(ctx).Trace().Object("setOperation", req).Stringer("operation", so).Send()
		return reducer(ctx, requests, cc.concurrencyLimit)
	}
}

func (cc *ConcurrentChecker) checkComputedUserset(
	ctx context.Context,
	req ValidatedCheckRequest,
	cu *core.ComputedUserset,
	tpl *core.RelationTuple,
) CheckFunc {
	var start *core.ObjectAndRelation
	if cu.Object == core.ComputedUserset_TUPLE_USERSET_OBJECT {
		if tpl == nil {
			panic("computed userset for tupleset without tuple")
		}

		start = tpl.Subject
	} else if cu.Object == core.ComputedUserset_TUPLE_OBJECT {
		if tpl != nil {
			start = tpl.ResourceAndRelation
		} else {
			start = req.ResourceAndRelation
		}
	}

	targetOnr := &core.ObjectAndRelation{
		Namespace: start.Namespace,
		ObjectId:  start.ObjectId,
		Relation:  cu.Relation,
	}

	// If we will be dispatching to the goal's ONR, then we know that the ONR is a member.
	if onrEqual(req.Subject, targetOnr) {
		return alwaysMember
	}

	// Check if the target relation exists. If not, return nothing.
	ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
	err := namespace.CheckNamespaceAndRelation(ctx, start.Namespace, cu.Relation, true, ds)
	if err != nil {
		if errors.As(err, &namespace.ErrRelationNotFound{}) {
			return notMember
		}

		return checkError(err)
	}

	return cc.dispatch(ValidatedCheckRequest{
		&v1.DispatchCheckRequest{
			ResourceAndRelation: targetOnr,
			Subject:             req.Subject,
			Metadata:            decrementDepth(req.Metadata),
		},
		req.Revision,
	})
}

func (cc *ConcurrentChecker) checkTupleToUserset(ctx context.Context, req ValidatedCheckRequest, ttu *core.TupleToUserset) CheckFunc {
	return func(ctx context.Context) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
		log.Ctx(ctx).Trace().Object("ttu", req).Send()
		ds := datastoremw.MustFromContext(ctx).SnapshotReader(req.Revision)
		it, err := ds.QueryRelationships(ctx, &v1_proto.RelationshipFilter{
			ResourceType:       req.ResourceAndRelation.Namespace,
			OptionalResourceId: req.ResourceAndRelation.ObjectId,
			OptionalRelation:   ttu.Tupleset.Relation,
		})
		if err != nil {
			return nil, NewCheckFailureErr(err, emptyMetadata)
		}
		defer it.Close()

		var requestsToDispatch []CheckFunc
		for tpl := it.Next(); tpl != nil; tpl = it.Next() {
			requestsToDispatch = append(requestsToDispatch, cc.checkComputedUserset(ctx, req, ttu.ComputedUserset, tpl))
		}
		if it.Err() != nil {
			return nil, NewCheckFailureErr(it.Err(), emptyMetadata)
		}

		return union(ctx, requestsToDispatch, cc.concurrencyLimit)
	}
}

func notMemberIfAny(response v1.DispatchCheckResponse_Membership) v1.DispatchCheckResponse_Membership {
	if response == v1.DispatchCheckResponse_NOT_MEMBER {
		return v1.DispatchCheckResponse_NOT_MEMBER
	}
	// In an intersection, being a member of one request does not determine the final resolution
	return v1.DispatchCheckResponse_UNKNOWN
}

// all returns whether all of the lazy checks pass, and is used for intersection.
func all(
	ctx context.Context,
	requests []CheckFunc,
	concurrencyLimit int,
) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
	return processSubrequests(
		ctx,
		requests,
		v1.DispatchCheckResponse_MEMBER,
		notMemberIfAny,
		concurrencyLimit,
	)
}

type earlyDeterminationFunc func(response v1.DispatchCheckResponse_Membership) v1.DispatchCheckResponse_Membership

func processSubrequests(
	parentCtx context.Context,
	requests []CheckFunc,
	defaultDetermination v1.DispatchCheckResponse_Membership,
	requestDetermination earlyDeterminationFunc,
	concurrencyLimit int,
) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
	if len(requests) == 0 {
		return &v1.DispatchCheckResponse{
			Metadata:   emptyMetadata,
			Membership: v1.DispatchCheckResponse_NOT_MEMBER,
		}, nil
	}

	responseMetadata := emptyMetadata
	resultChan := make(chan *v1.DispatchCheckResponse, len(requests))

	cancelCtx, cancelFn := context.WithCancel(parentCtx)
	g, ctx := sync.WithContext[dispatch.MetadataError](cancelCtx)
	g.SetLimit(concurrencyLimit)

	defer func() {
		start := time.Now()

		cancelFn()
		if err := g.Wait(); err != nil {
			log.Trace().Err(err).Msg("error waiting for goroutines to cancel")
		}
		close(resultChan)

		log.Trace().Stringer("cleanupTime", time.Since(start)).Msg("finished check set dispatch cleanup")
	}()

	// Start a goroutine to feed requests to the errgroup
	go func() {
		for _, req := range requests {
			// Make a copy of the loop var that we will close over
			req := req

			if ctx.Err() != nil {
				break
			}
			g.Go(func() dispatch.MetadataError {
				// We could have been queued up waiting, and the group got closed in the interim.
				if ctx.Err() != nil {
					return nil
				}

				resp, err := req(ctx)
				if err != nil {
					log.Trace().Err(err).Msg("subrequest errored")
					return err
				}
				log.Trace().Int32("result", int32(resp.Membership)).Msg("subrequest response")

				resultChan <- resp

				return nil
			})
		}
	}()

	for i := 0; i < len(requests); i++ {
		select {
		case result := <-resultChan:
			responseMetadata = combineResponseMetadata(responseMetadata, result.Metadata)
			earlyDetermination := requestDetermination(result.Membership)
			if earlyDetermination != v1.DispatchCheckResponse_UNKNOWN {
				return &v1.DispatchCheckResponse{
					Metadata:   responseMetadata,
					Membership: earlyDetermination,
				}, nil
			}
		case <-ctx.Done():
			if err := g.Wait(); err != nil {
				return nil, dispatch.WrapWithMetadata(err, responseMetadata)
			}
			log.Trace().Msg("not an error case")
		}
	}

	return &v1.DispatchCheckResponse{
		Metadata:   responseMetadata,
		Membership: defaultDetermination,
	}, nil
}

// checkError returns the error.
func checkError(err error) CheckFunc {
	return func(ctx context.Context) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
		return nil, NewCheckFailureErr(err, emptyMetadata)
	}
}

// alwaysMember returns that the check always passes.
func alwaysMember(ctx context.Context) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
	return &v1.DispatchCheckResponse{
		Metadata:   emptyMetadata,
		Membership: v1.DispatchCheckResponse_MEMBER,
	}, nil
}

// notMember returns that the check always returns false.
func notMember(ctx context.Context) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
	return &v1.DispatchCheckResponse{
		Metadata:   emptyMetadata,
		Membership: v1.DispatchCheckResponse_NOT_MEMBER,
	}, nil
}

func memberIfAny(response v1.DispatchCheckResponse_Membership) v1.DispatchCheckResponse_Membership {
	if response == v1.DispatchCheckResponse_MEMBER {
		return v1.DispatchCheckResponse_MEMBER
	}

	return v1.DispatchCheckResponse_UNKNOWN
}

// union returns whether any one of the lazy checks pass, and is used for union.
func union(
	ctx context.Context,
	requests []CheckFunc,
	concurrencyLimit int,
) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
	return processSubrequests(
		ctx,
		requests,
		v1.DispatchCheckResponse_NOT_MEMBER,
		memberIfAny,
		concurrencyLimit,
	)
}

// difference returns whether the first lazy check passes and none of the subsequent checks pass.
func difference(
	ctx context.Context,
	requests []CheckFunc,
	concurrencyLimit int,
) (*v1.DispatchCheckResponse, dispatch.MetadataError) {
	if len(requests) == 0 {
		return &v1.DispatchCheckResponse{
			Metadata:   emptyMetadata,
			Membership: v1.DispatchCheckResponse_NOT_MEMBER,
		}, nil
	}

	baseResponse, err := requests[0](ctx)
	if err != nil {
		return nil, err
	}

	if baseResponse.Membership != v1.DispatchCheckResponse_MEMBER {
		return baseResponse, nil
	}

	rhsResponse, err := union(ctx, requests[1:], concurrencyLimit)
	if err != nil {
		return nil, NewCheckFailureErr(err, combineResponseMetadata(baseResponse.Metadata, err.GetMetadata()))
	}

	var finalDetermination v1.DispatchCheckResponse_Membership
	switch rhsResponse.Membership {
	case v1.DispatchCheckResponse_MEMBER:
		finalDetermination = v1.DispatchCheckResponse_NOT_MEMBER
	case v1.DispatchCheckResponse_NOT_MEMBER:
		finalDetermination = v1.DispatchCheckResponse_MEMBER
	}

	return &v1.DispatchCheckResponse{
		Metadata:   combineResponseMetadata(baseResponse.Metadata, rhsResponse.Metadata),
		Membership: finalDetermination,
	}, nil
}
