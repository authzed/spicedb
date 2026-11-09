package queryplan

import (
	"context"

	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/querysignals"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryprofile"
	"github.com/authzed/spicedb/pkg/query/signals"
	queryschema "github.com/authzed/spicedb/pkg/schema/v2"
)

// Planner prepares schema-built and received plans with a resolved execution policy.
// Schema-owned caches retain only fixed plans; evidence and hint decisions stay local.
type Planner struct {
	// Profile configures construction, optimization, reuse and execution capabilities.
	Profile queryprofile.Profile

	// Signals owns schema-scoped observations for policies that collect evidence.
	Signals *signals.Owner
}

// Request supplies the schema and request-local inputs shared by planning entrypoints.
type Request struct {
	// Schema is the already-loaded schema at the requested revision.
	Schema datalayer.SchemaReader

	// Revision scopes revision-bound signals and dispatch results.
	Revision datastore.Revision

	// SchemaHash identifies the schema selected by the request snapshot.
	SchemaHash datalayer.SchemaHash

	// Params identifies the logical operation and normalized subject shape.
	Params queryopt.RequestParams

	// CaveatContext contains the original request values used by learned planning.
	CaveatContext map[string]any
}

// Prepared holds an immutable iterator and independent execution options.
// Flush must run after execution, including failed or partially consumed requests.
type Prepared struct {
	// Iterator is ready for execution and must not be mutated by the caller.
	Iterator query.Iterator

	// Options attaches policy, hint accounting and observations to a fresh context.
	Options []query.ContextOption

	// HintLedger accounts for compiler and runtime decisions in this request.
	HintLedger *planhint.Ledger
	collector  *querysignals.Collector
}

// Flush publishes this request's buffered observations once.
func (p *Prepared) Flush() {
	if p.collector != nil {
		p.collector.Flush()
	}
}

// FromSchema builds a dispatch-wrapped plan for a validated relation or permission.
func (p Planner) FromSchema(ctx context.Context, request Request, definition, permission string) (*Prepared, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	profile := p.Profile.ForOperation(request.Params.Operation)

	// Fixed plans depend only on schema, subject shape and resolved deployment
	// limits. Capture those immutable inputs in the shared compilation flight,
	// keeping caveat values, revisions and execution observations out of the cache.
	if profile.ReuseCompiledPlan(request.Params.Operation) {
		reader, params := request.Schema, request.Params
		key := Key{
			Profile:      profile,
			Operation:    params.Operation,
			ResourceType: definition,
			Permission:   permission,
			Subject:      query.ObjectType{Type: params.SubjectType, Subrelation: params.SubjectRelation},
		}
		iterator, ledger, err := Load(ctx, reader, key, func(shared context.Context) (query.CanonicalOutline, error) {
			schema, err := query.LoadSchema(shared, reader)
			if err != nil {
				return query.CanonicalOutline{}, err
			}

			return schemaOutline(schema, profile, definition, permission, params)
		})
		if err != nil {
			return nil, err
		}

		return prepared(profile, iterator, ledger, nil), nil
	}

	schema, err := query.LoadSchema(ctx, request.Schema)
	if err != nil {
		return nil, err
	}

	outline, err := schemaOutline(schema, profile, definition, permission, request.Params)
	if err != nil {
		return nil, err
	}

	var collector *querysignals.Collector

	// Sample request context before the decision phase, then use one memoized
	// signal view for its recommendations. Execution observations are published
	// at request exit, so this request cannot feed back into its own decisions.
	if profile.CollectSignals() {
		view, err := querysignals.Bind(p.Signals, request.Schema, request.Revision, string(request.SchemaHash))
		if err != nil {
			return nil, err
		}

		var fields []string

		for _, caveat := range schema.Caveats() {
			for _, parameter := range caveat.Parameters() {
				fields = append(fields, parameter.Name())
			}
		}

		querysignals.ObserveRequestContext(view, request.CaveatContext, querysignals.ContextOptions{Fields: fields})
		outline, err = querysignals.Plan(outline, request.Params, view, true, profile.DecisionOptimizers())
		if err != nil {
			return nil, err
		}

		collector = querysignals.NewCollector(view, querysignals.CollectorOptions{})
	}

	iterator, err := outline.Compile()
	if err != nil {
		return nil, err
	}

	return prepared(profile, iterator, outline.HintLedger, collector), nil
}

func schemaOutline(
	schema *queryschema.Schema,
	profile queryprofile.Profile,
	definition, permission string,
	params queryopt.RequestParams,
) (query.CanonicalOutline, error) {
	outline, err := query.BuildOutlineFromSchema(schema, definition, permission, profile.BuildOptions()...)
	if err != nil {
		return query.CanonicalOutline{}, err
	}

	optimized, err := queryopt.ApplyOptimizations(outline, profile.StructuralOptimizers(params), params)
	if err != nil {
		return query.CanonicalOutline{}, err
	}

	// Establish dispatch boundaries after structural transforms and before learned
	// recommendations, so execution identities describe the final wrapped tree.
	return dispatch.ApplyDispatchWrap(optimized, params)
}

func prepared(profile queryprofile.Profile, iterator query.Iterator, ledger *planhint.Ledger, collector *querysignals.Collector) *Prepared {
	options := append(profile.ContextOptions(), query.WithHintLedger(ledger))

	if collector != nil {
		options = append(options, query.WithExecutionObserver(collector))
	}

	return &Prepared{Iterator: iterator, Options: options, HintLedger: ledger, collector: collector}
}
