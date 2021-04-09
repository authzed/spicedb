package graph

import (
	"context"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const errDispatch = "error dispatching request: %w"

var errMaxDepth = errors.New("max depth has been reached")

// NewLocalDispatcher creates a dispatcher that checks everything in the same
// process on the same machine.
func NewLocalDispatcher(nsm namespace.Manager, ds datastore.GraphDatastore) (Dispatcher, error) {
	return &localDispatcher{nsm: nsm, ds: ds}, nil
}

type localDispatcher struct {
	nsm namespace.Manager
	ds  datastore.GraphDatastore
}

func (ld *localDispatcher) loadRelation(ctx context.Context, nsName, relationName string) (*pb.Relation, error) {
	// Load namespace and relation from the datastore
	ns, _, err := ld.nsm.ReadNamespace(ctx, nsName)
	if err != nil {
		return nil, rewriteError(err)
	}

	var relation *pb.Relation
	for _, candidate := range ns.Relation {
		if candidate.Name == relationName {
			relation = candidate
			break
		}
	}

	if relation == nil {
		return nil, ErrRelationNotFound
	}

	return relation, nil
}

func (ld *localDispatcher) Check(ctx context.Context, req CheckRequest) CheckResult {
	if req.DepthRemaining < 1 {
		return CheckResult{Err: fmt.Errorf(errDispatch, errMaxDepth)}
	}

	relation, err := ld.loadRelation(ctx, req.Start.Namespace, req.Start.Relation)
	if err != nil {
		return CheckResult{Err: err}
	}

	chk := newConcurrentChecker(ld, ld.ds)

	asyncCheck := chk.check(ctx, req, relation)
	return Any(ctx, []ReduceableCheckFunc{asyncCheck})
}

func (ld *localDispatcher) Expand(ctx context.Context, req ExpandRequest) ExpandResult {
	if req.DepthRemaining < 1 {
		return ExpandResult{Err: fmt.Errorf(errDispatch, errMaxDepth)}
	}

	relation, err := ld.loadRelation(ctx, req.Start.Namespace, req.Start.Relation)
	if err != nil {
		return ExpandResult{Tree: nil, Err: err}
	}

	expand := newConcurrentExpander(ld, ld.ds)

	asyncExpand := expand.expand(ctx, req, relation)
	return ExpandAny(ctx, req.Start, []ReduceableExpandFunc{asyncExpand})
}

func rewriteError(original error) error {
	switch original {
	case datastore.ErrNamespaceNotFound:
		return ErrNamespaceNotFound
	case ErrNamespaceNotFound:
		fallthrough
	case ErrRelationNotFound:
		return original
	default:
		return fmt.Errorf(errDispatch, original)
	}
}
