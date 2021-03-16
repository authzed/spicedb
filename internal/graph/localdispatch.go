package graph

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errDispatchCheck = "error dispatching check request: %w"
)

// NewLocalDispatcher creates a dispatcher that checks everything in the same
// process on the same machine.
func NewLocalDispatcher(ds datastore.Datastore) (Dispatcher, error) {
	return &localDispatcher{ds: ds}, nil
}

type localDispatcher struct {
	ds datastore.Datastore
}

func (ld *localDispatcher) loadRelation(nsName, relationName string) (*pb.Relation, error) {
	// Load namespace and relation from the datastore
	ns, _, err := ld.ds.ReadNamespace(nsName)
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
	relation, err := ld.loadRelation(req.Start.Namespace, req.Start.Relation)
	if err != nil {
		return CheckResult{IsMember: false, Err: err}
	}

	chk := newConcurrentChecker(ld, ld.ds)

	asyncCheck := chk.check(req, relation)
	return Any(ctx, []ReduceableCheckFunc{asyncCheck})
}

func (ld *localDispatcher) Expand(ctx context.Context, req ExpandRequest) ExpandResult {
	relation, err := ld.loadRelation(req.Start.Namespace, req.Start.Relation)
	if err != nil {
		return ExpandResult{Tree: nil, Err: err}
	}

	expand := newConcurrentExpander(ld, ld.ds)

	asyncExpand := expand.expand(req, relation)
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
		return fmt.Errorf(errDispatchCheck, original)
	}
}
