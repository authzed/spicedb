package graph

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/testfixtures"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

const queryPlanSelfEdgeSchema = `
definition user {}

definition team {
	relation member: user
}
`

// recordingPlanDispatcher delegates to a real dispatcher and records the
// DispatchQueryPlan requests the sender makes, so a test can prove a hop was
// actually dispatched rather than evaluated locally.
type recordingPlanDispatcher struct {
	dispatch.Dispatcher
	planCalls []*v1.DispatchQueryPlanRequest
}

func (d *recordingPlanDispatcher) DispatchQueryPlan(req *v1.DispatchQueryPlanRequest, stream dispatch.PlanStream) error {
	d.planCalls = append(d.planCalls, req)
	return d.Dispatcher.DispatchQueryPlan(req, stream)
}

// TestQueryPlanSelfEdgeAcrossDispatch runs the reflexive identity cases through
// a real localDispatcher, the way the v1 permissions service does, so the alias
// that decides identity runs on the receiving side of a dispatch hop.
//
// The receiver builds a fresh query.Context and pre-seals its top-level
// operation, so it only knows the request's target if it restores it from
// PlanContext.target_subject_relation. Without that, the first case below comes
// back empty. team#member is deliberately non-recursive: aliases under a
// recursion are not dispatch-wrapped, so a recursive relation would decide
// identity on the sender and never exercise the receiver.
//
// Expected values are the classic dispatcher's.
func TestQueryPlanSelfEdgeAcrossDispatch(t *testing.T) {
	rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
	require.NoError(t, err)
	ds, revision := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, queryPlanSelfEdgeSchema,
		[]tuple.Relationship{
			tuple.MustParse("team:eng#member@user:alice"),
		})

	ctx := log.Logger.WithContext(datalayer.ContextWithHandle(t.Context()))
	require.NoError(t, datalayer.SetInContext(ctx, datalayer.NewDataLayer(ds)))

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: queryPlanSelfEdgeSchema,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)
	fullSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, compiled.CaveatDefinitions)
	require.NoError(t, err)

	local, err := NewLocalOnlyDispatcher(MustNewDefaultDispatcherParametersForTesting())
	require.NoError(t, err)
	t.Cleanup(func() { _ = local.Close() })

	// newQueryContext mirrors the v1 permissions service's query plan path:
	// optimize, wrap dispatch-eligible aliases, compile, and run on a
	// DispatchExecutor.
	newQueryContext := func(t *testing.T, op query.Operation, resourceType, permission string, subject query.ObjectType) (*query.Context, query.Iterator, *recordingPlanDispatcher) {
		t.Helper()
		co, err := query.BuildOutlineFromSchema(fullSchema, resourceType, permission)
		require.NoError(t, err)
		params := queryopt.RequestParams{
			Operation:       op,
			SubjectType:     subject.Type,
			SubjectRelation: subject.Subrelation,
		}
		optimized, err := queryopt.ApplyOptimizations(co, queryopt.OptimizersForRequest(params), params)
		require.NoError(t, err)
		optimized, err = dispatch.ApplyDispatchWrap(optimized, params)
		require.NoError(t, err)
		it, err := optimized.Compile()
		require.NoError(t, err)

		recorder := &recordingPlanDispatcher{Dispatcher: local}
		qctx := dispatch.NewQueryContext(
			ctx,
			recorder,
			dispatch.NewPlanContext(revision.String(), datalayer.NoSchemaHashForTesting, nil, 50, 0),
			query.NewQueryDatastoreReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, datalayer.NoSchemaHashForTesting)),
			caveats.NewCaveatRunner(caveattypes.Default.TypeSet),
			100,
		)
		return qctx, it, recorder
	}

	lookupSubjects := func(t *testing.T, resourceID string, target query.ObjectType) ([]string, *recordingPlanDispatcher) {
		t.Helper()
		qctx, it, recorder := newQueryContext(t, query.OperationIterSubjects, "team", "member", target)
		pathSeq, err := qctx.IterSubjects(it, query.NewObject("team", resourceID), target)
		require.NoError(t, err)
		paths, err := query.CollectAll(pathSeq)
		require.NoError(t, err)

		found := make([]string, 0, len(paths))
		for _, path := range paths {
			found = append(found, fmt.Sprintf("%s:%s#%s", path.Subject.ObjectType, path.Subject.ObjectID, path.Subject.Relation))
		}
		sort.Strings(found)
		return found, recorder
	}

	t.Run("lookup subjects includes identity decided on the receiver", func(t *testing.T) {
		found, recorder := lookupSubjects(t, "eng", query.ObjectType{Type: "team", Subrelation: "member"})

		// No relationship points at team:eng#member; the identity comes from
		// the target alone, decided by the receiver's alias.
		require.Equal(t, []string{"team:eng#member"}, found)

		require.NotEmpty(t, recorder.planCalls, "the alias must have been dispatched, not evaluated locally")
		first := recorder.planCalls[0]
		require.Equal(t, v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS, first.Operation)
		require.NotNil(t, first.PlanContext.TargetSubjectRelation)
		require.Equal(t, "team", first.PlanContext.TargetSubjectRelation.Namespace)
		require.Equal(t, "member", first.PlanContext.TargetSubjectRelation.Relation)
	})

	t.Run("lookup subjects omits identity for a differently typed target", func(t *testing.T) {
		found, recorder := lookupSubjects(t, "eng", query.ObjectType{Type: "user", Subrelation: tuple.Ellipsis})

		require.Equal(t, []string{"user:alice#..."}, found)
		require.NotEmpty(t, recorder.planCalls, "the alias must have been dispatched, not evaluated locally")
	})
}
