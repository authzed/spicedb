package querysignals

import (
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// Bind uses an already-loaded schema. Legacy readers receive request-local evidence.
// Acquisition never issues datastore reads or queries.
func Bind(owner *signals.Owner, loaded any, revision datastore.Revision, hash string) (*signals.View, error) {
	scope := signal.Scope{Revision: revision, SchemaHash: hash}

	if stored, ok := loaded.(interface {
		StoredSchema() *datastore.ReadOnlyStoredSchema
	}); ok {
		return owner.Bind(stored.StoredSchema(), scope)
	}

	return owner.Local(scope), nil
}

// Plan runs the decision phase on a memoized read view after dispatch wrapping.
func Plan(co query.CanonicalOutline, params queryopt.RequestParams, view *signals.View, batched bool) (query.CanonicalOutline, error) {
	pipeline, err := queryopt.NewPipeline(queryopt.DecisionOptimizers(), view.Registry())
	if err != nil {
		return query.CanonicalOutline{}, err
	}

	co = query.UniqueOutlineOccurrences(co)
	ctx := optimization.NewContext(params, view.ForPlan(), nil)
	facts := query.DescribeOutline(co.Root)
	ctx.Key = func(n query.Outline, op query.Operation) signal.ExecutionKey {
		filter := query.NoObjectFilter().String()

		if op == query.OperationIterResources {
			filter = query.ResourceFilter(
				query.ObjectAndRelation{ObjectType: params.SubjectType, Relation: params.SubjectRelation},
				query.NoObjectFilter(),
			)
		}

		strategy := "default"

		if batched {
			strategy += "/batched"
		}

		role := "body"

		if n.Type == dispatch.DispatchIteratorType {
			role = "dispatch-wrapper"
		}

		return signal.ExecutionKey{
			Identity:     facts[n.ID].Identity,
			Operation:    query.SignalOperation(op),
			Filter:       filter,
			Mode:         "single",
			Strategy:     strategy,
			Role:         role,
			Dependencies: signal.EncodeFootprint(facts[n.ID].Footprint),
		}
	}
	return pipeline.Run(co, ctx)
}
