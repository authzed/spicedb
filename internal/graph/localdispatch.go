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

func errResult(err error) CheckResult {
	return CheckResult{IsMember: false, Err: err}
}

func (ld *localDispatcher) Check(ctx context.Context, req CheckRequest) CheckResult {
	// Load namespace and relation from the datastore
	ns, _, err := ld.ds.ReadNamespace(req.Start.Namespace)
	if err != nil {
		return errResult(rewriteError(err))
	}

	var relation *pb.Relation
	for _, candidate := range ns.Relation {
		if candidate.Name == req.Start.Relation {
			relation = candidate
			break
		}
	}

	if relation == nil {
		return errResult(ErrRelationNotFound)
	}

	chk := newLazyChecker(ld, ld.ds)

	asyncCheck := chk.check(req, relation)
	return Any(ctx, []ReduceableCheck{asyncCheck})
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
