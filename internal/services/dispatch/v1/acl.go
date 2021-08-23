package dispatch

import (
	"context"
	"errors"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/authzed/spicedb/internal/graph"
	"github.com/authzed/spicedb/internal/namespace"
	v1 "github.com/authzed/spicedb/internal/proto/dispatch/v1"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type dispatchServer struct {
	v1.UnimplementedDispatchServiceServer

	checker       graph.Checker
	expander      graph.Expander
	lookupHandler graph.LookupHandler

	nsm namespace.Manager
}

const (
	errBadRevision     = "invalid request revision: %s"
	errUnknownRelation = "invalid request relation: %s"
)

func NewDispatchServer(checker graph.Checker, expander graph.Expander, lookupHandler graph.LookupHandler) v1.DispatchServiceServer {
	return &dispatchServer{
		checker:       checker,
		expander:      expander,
		lookupHandler: lookupHandler,
	}
}

func (ds *dispatchServer) DispatchCheck(ctx context.Context, req *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	relation, err := ds.loadRelation(ctx, req.ObjectAndRelation.Namespace, req.ObjectAndRelation.Relation)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, errUnknownRelation, err)
	}

	asyncCheck := ds.checker.Check(ctx, req, relation)
	checkResult := graph.Any(ctx, []graph.ReduceableCheckFunc{asyncCheck})

	return checkResult.Resp, rewriteGraphError(checkResult.Err)
}

func (ds *dispatchServer) DispatchExpand(ctx context.Context, req *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	relation, err := ds.loadRelation(ctx, req.ObjectAndRelation.Namespace, req.ObjectAndRelation.Relation)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, errUnknownRelation, err)
	}

	asyncExpand := ds.expander.Expand(ctx, req, relation)
	expandResult := graph.ExpandOne(ctx, asyncExpand)

	return expandResult.Resp, rewriteGraphError(expandResult.Err)
}

func (ds *dispatchServer) DispatchLookup(ctx context.Context, req *v1.DispatchLookupRequest) (*v1.DispatchLookupResponse, error) {
	asyncLookup := ds.lookupHandler.Lookup(ctx, req)
	lookupResult := graph.LookupAny(ctx, req.Limit, []graph.ReduceableLookupFunc{asyncLookup})

	return lookupResult.Resp, rewriteGraphError(lookupResult.Err)
}

func (ds *dispatchServer) loadRelation(ctx context.Context, nsName, relationName string) (*v0.Relation, error) {
	// Load namespace and relation from the datastore
	ns, _, err := ds.nsm.ReadNamespace(ctx, nsName)
	if err != nil {
		return nil, graph.NewRelationNotFoundErr(nsName, relationName)
	}

	var relation *v0.Relation
	for _, candidate := range ns.Relation {
		if candidate.Name == relationName {
			relation = candidate
			break
		}
	}

	if relation == nil {
		return nil, graph.NewRelationNotFoundErr(nsName, relationName)
	}

	return relation, nil
}

func rewriteGraphError(err error) error {
	switch {
	case errors.As(err, &graph.ErrRequestCanceled{}):
		return status.Errorf(codes.Canceled, "request canceled: %s", err)

	case err == nil:
		return nil

	case errors.As(err, &graph.ErrAlwaysFail{}):
		fallthrough
	default:
		log.Err(err)
		return err
	}
}
