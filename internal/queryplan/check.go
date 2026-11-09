package queryplan

import (
	"context"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/pkg/datalayer"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
)

// CheckRequest provides the public Check inputs after API validation.
type CheckRequest struct {
	// Request carries the schema, revision and original caveat values.
	Request

	// Permission is the validated relation or permission on Resource.
	Permission string

	// Resource identifies the object being checked.
	Resource query.Object

	// Subject identifies the normalized subject being checked.
	Subject query.ObjectAndRelation

	// Dispatcher supplies result caching and dispatch execution.
	Dispatcher dispatch.Dispatcher

	// Reader reads relationships at Request.Revision.
	Reader datalayer.RevisionedReader

	// CaveatRunner evaluates caveats during execution when the policy permits it.
	CaveatRunner *caveats.CaveatRunner

	// MaximumDepth bounds recursive dispatch execution.
	MaximumDepth int32

	// DispatchChunkSize limits targets sent in one dispatch batch.
	DispatchChunkSize uint16
}

// Check probes eligible root results, prepares the plan on a miss, and executes it.
// Its returned path remains symbolic when request caveats belong at the API boundary.
func (p Planner) Check(ctx context.Context, request CheckRequest) (*query.Path, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	values := request.CaveatContext

	// Symbolic policies omit request values from dispatch identity and evaluate the
	// resulting expression at the public boundary, allowing reuse across contexts.
	if p.Profile.SymbolicCheckResults() {
		values = nil
	}

	planContext := dispatch.NewPlanContext(request.Revision.String(), request.SchemaHash, values, request.MaximumDepth, 0)

	// A request-independent policy can return a validated root hit before schema
	// conversion or compilation. Learned planning still runs to maintain observations
	// and request-specific recommendations even when execution will hit the cache.
	if p.Profile.ReuseCompiledPlan(query.OperationCheck) {
		cached, found, err := request.Dispatcher.LookupPlanCheck(ctx, dispatch.PlanCheckLookup{
			CanonicalKey: request.Resource.ObjectType + "#" + request.Permission,
			Resource:     &core.ObjectAndRelation{Namespace: request.Resource.ObjectType, ObjectId: request.Resource.ObjectID},
			Subject: &core.ObjectAndRelation{
				Namespace: request.Subject.ObjectType,
				ObjectId:  request.Subject.ObjectID,
				Relation:  request.Subject.Relation,
			},
			PlanContext: planContext,
		})
		if err != nil {
			return nil, err
		}

		if found {
			return query.PathFromProto(cached)
		}
	}

	request.Params = queryopt.RequestParams{
		Operation:       query.OperationCheck,
		SubjectType:     request.Subject.ObjectType,
		SubjectRelation: request.Subject.Relation,
	}
	plan, err := p.FromSchema(ctx, request.Request, request.Resource.ObjectType, request.Permission)
	if err != nil {
		return nil, err
	}

	defer plan.Flush()
	execution := dispatch.NewQueryContext(
		ctx,
		request.Dispatcher,
		planContext,
		query.NewQueryDatastoreReader(request.Reader),
		request.CaveatRunner,
		request.DispatchChunkSize,
		plan.Options...,
	)
	return execution.Check(plan.Iterator, request.Resource, request.Subject)
}
