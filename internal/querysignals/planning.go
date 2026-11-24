package querysignals

import (
	"errors"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryopt/optimization"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// ErrLegacySchemaUnsupported is returned when query planning is attempted over a
// schema that was not read from unified schema storage.
var ErrLegacySchemaUnsupported = errors.New("query planning requires unified schema storage: legacy schema reads are not supported")

// Bind uses an already-loaded schema. Legacy schema readers are not supported and fail.
// Acquisition never issues datastore reads or queries.
func Bind(owner *signals.Owner, loaded any, revision datastore.Revision, hash string) (*signals.View, error) {
	scope := signal.Scope{Revision: revision, SchemaHash: hash}

	stored, ok := loaded.(interface {
		StoredSchema() *datastore.ReadOnlyStoredSchema
	})
	if !ok {
		return nil, ErrLegacySchemaUnsupported
	}

	s := stored.StoredSchema()

	// Callers without a real hash, such as legacy ZedTokens, bind to the hash of the loaded schema.
	// TODO: remove once all callers have moved over to tokens and cursors that carry a schema hash.
	if datalayer.SchemaHash(hash).IsBypassSentinel() && s != nil && s.Get().GetV1() != nil {
		scope.SchemaHash = s.Get().GetV1().SchemaHash
	}

	return owner.Bind(s, scope)
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
