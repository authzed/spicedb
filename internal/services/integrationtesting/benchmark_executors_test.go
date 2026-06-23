//go:build integration

package integrationtesting_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	bm "github.com/authzed/spicedb/pkg/benchmarks"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	schemav2 "github.com/authzed/spicedb/pkg/schema/v2"
)

// executorScenarios is the curated list of registry benchmarks exercised by
// BenchmarkExecutors. Kept small and Check-only because each engine spins up a
// fresh database container per scenario.
var executorScenarios = []string{
	"WideArrow",
	"DoubleWideArrow",
	"DeepArrow",
	"LookupIntersection",
	"ShareWith",
}

// classicDispatchDepthRemaining matches the convention used elsewhere in the
// dispatcher unit tests.
const classicDispatchDepthRemaining = 50

// BenchmarkExecutors compares three in-process Check paths against the same
// datastore for each engine:
//
//	dispatch           — graph.LocalOnlyDispatcher.DispatchCheck (today's path).
//	queryplan_local    — query.NewLocalContext + LocalExecutor (no dispatch hop).
//	queryplan_dispatch — dispatch.NewQueryContext + DispatchExecutor (alias
//	                     boundaries cross back through the dispatcher).
//
// gRPC is intentionally bypassed so the numbers reflect the executor
// difference, not transport overhead. Hierarchy:
// engine → scenario → operation → variant.
func BenchmarkExecutors(b *testing.B) {
	for _, engineID := range enginesToBenchmark {
		if testing.Short() && engineID != "memory" {
			continue
		}
		b.Run(engineID, func(b *testing.B) {
			for _, scenarioName := range executorScenarios {
				b.Run(scenarioName, func(b *testing.B) {
					h := newExecutorHandle(b, engineID, scenarioName)
					b.Run("Check", func(b *testing.B) {
						runExecutorCheck(b, h)
					})
				})
			}
		})
	}
}

// executorHandle bundles everything the per-variant benchmark loops need: the
// datastore, the populated revision/schema, a local-only dispatcher for the
// classic path, and an advised iterator for the query-plan paths.
type executorHandle struct {
	ds         datastore.Datastore
	revision   datastore.Revision
	schemaHash datalayer.SchemaHash
	schema     *schemav2.Schema

	check bm.CheckQuery

	dispatcher dispatch.Dispatcher
	dlCtx      context.Context

	iterator query.Iterator
}

func newExecutorHandle(b *testing.B, engineID, scenarioName string) *executorHandle {
	b.Helper()
	b.StopTimer()

	rde := testdatastore.RunDatastoreEngine(b, engineID)
	ds := rde.NewDatastore(b, config.DatastoreConfigInitFunc(
		b,
		dsconfig.WithWatchBufferLength(0),
		dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
		dsconfig.WithRevisionQuantization(10),
		dsconfig.WithMaxRetries(50),
		dsconfig.WithWriteAcquisitionTimeout(5*time.Second),
	))
	b.Cleanup(func() { ds.Close() })

	benchmark, ok := bm.Get(scenarioName)
	require.True(b, ok, "benchmark %q not found in registry", scenarioName)

	queries, err := benchmark.Setup(b.Context(), ds)
	require.NoError(b, err)
	require.NotEmpty(b, queries.Checks, "scenario %q must register a CheckQuery", scenarioName)
	check := queries.Checks[0]

	rev, err := ds.HeadRevision(b.Context())
	require.NoError(b, err)
	schemaHash := datalayer.SchemaHash(rev.SchemaHash)

	fullSchema, err := bm.ReadSchema(b.Context(), ds, rev.Revision)
	require.NoError(b, err)

	// Local-only dispatcher for the classic variant. No caching, no
	// singleflight — matches the "dispatch" path in pkg/query/benchmarks but
	// against whatever engine we're running.
	dispatcher, err := graph.NewLocalOnlyDispatcher(graph.MustNewDefaultDispatcherParametersForTesting())
	require.NoError(b, err)
	b.Cleanup(func() { _ = dispatcher.Close() })

	// The dispatcher and the QP/DispatchExecutor variant both pull the
	// datalayer off the context.
	dlCtx := datalayer.ContextWithDataLayer(b.Context(), datalayer.NewDataLayer(ds))

	// Build and warm an advised iterator once. Both QP variants reuse this
	// iterator (via Clone) so the comparison reflects the executor cost on the
	// same plan.
	iterator := compileAdvisedCheckIterator(b, ds, rev.Revision, schemaHash, fullSchema, check)

	return &executorHandle{
		ds:         ds,
		revision:   rev.Revision,
		schemaHash: schemaHash,
		schema:     fullSchema,
		check:      check,
		dispatcher: dispatcher,
		dlCtx:      dlCtx,
		iterator:   iterator,
	}
}

// compileAdvisedCheckIterator builds the canonical outline for the requested
// (resource, permission), applies the standard optimizers, runs a warm-up pass
// against a CountObserver, and returns the advised iterator. Mirrors the warm
// path in pkg/query/benchmarks/check_benchmark_test.go.
func compileAdvisedCheckIterator(
	b *testing.B,
	ds datastore.Datastore,
	revision datastore.Revision,
	schemaHash datalayer.SchemaHash,
	fullSchema *schemav2.Schema,
	check bm.CheckQuery,
) query.Iterator {
	b.Helper()

	co, err := query.BuildOutlineFromSchema(fullSchema, check.ResourceType, check.Permission)
	require.NoError(b, err)

	params := queryopt.RequestParams{
		Operation:       query.OperationCheck,
		SubjectType:     check.SubjectType,
		SubjectRelation: check.SubjectRelation,
	}
	optimized, err := queryopt.ApplyOptimizations(co, queryopt.OptimizersForRequest(params), params)
	require.NoError(b, err)

	// Match the production pipeline (see graph.DispatchQueryPlan and
	// permissions_queryplan): every dispatch-eligible alias gets a
	// DispatchIterator wrap. Without this the queryplan_dispatch variant
	// would never dispatch — the DispatchExecutor pivots on the wrap, not
	// on bare aliases — and the benchmark would silently measure the
	// local path under a dispatch label.
	optimized, err = dispatch.ApplyDispatchWrap(optimized, params)
	require.NoError(b, err)

	warmIt, err := optimized.Compile()
	require.NoError(b, err)

	reader := query.NewQueryDatastoreReader(datalayer.NewDataLayer(ds).SnapshotReader(revision, schemaHash))
	obs := query.NewCountObserver()
	warmCtx := query.NewLocalContext(
		b.Context(),
		query.WithReader(reader),
		query.WithObserver(obs),
		query.WithCaveatRunner(caveats.NewCaveatRunner(caveattypes.Default.TypeSet)),
	)

	resource := query.NewObject(check.ResourceType, check.ResourceID)
	subject := query.NewObject(check.SubjectType, check.SubjectID).WithEllipses()

	_, err = warmCtx.Check(warmIt, resource, subject)
	require.NoError(b, err)

	advisor := query.NewCountAdvisor(obs.GetStats())
	advisedCO, err := query.ApplyAdvisor(optimized, advisor)
	require.NoError(b, err)

	advised, err := advisedCO.Compile()
	require.NoError(b, err)
	return advised
}

func runExecutorCheck(b *testing.B, h *executorHandle) {
	b.Helper()

	resourceRR := &core.RelationReference{
		Namespace: h.check.ResourceType,
		Relation:  h.check.Permission,
	}
	subjectONR := &core.ObjectAndRelation{
		Namespace: h.check.SubjectType,
		ObjectId:  h.check.SubjectID,
		Relation:  h.check.SubjectRelation,
	}

	resourceObj := query.Object{ObjectType: h.check.ResourceType, ObjectID: h.check.ResourceID}
	subjectObj := query.ObjectAndRelation{
		ObjectType: h.check.SubjectType,
		ObjectID:   h.check.SubjectID,
		Relation:   h.check.SubjectRelation,
	}

	bloom, err := dispatchv1.NewTraversalBloomFilter(50)
	require.NoError(b, err)

	dispatchReq := func() *dispatchv1.DispatchCheckRequest {
		return &dispatchv1.DispatchCheckRequest{
			ResourceRelation: resourceRR,
			ResourceIds:      []string{h.check.ResourceID},
			ResultsSetting:   dispatchv1.DispatchCheckRequest_ALLOW_SINGLE_RESULT,
			Subject:          subjectONR,
			Metadata: &dispatchv1.ResolverMeta{
				AtRevision:     h.revision.String(),
				DepthRemaining: classicDispatchDepthRemaining,
				SchemaHash:     []byte(h.schemaHash),
				TraversalBloom: append([]byte(nil), bloom...),
			},
		}
	}

	// Warm-up each path once so any first-call cost (schema load, namespace
	// lookup, connection pool prime) is out of the timed loop.
	{
		resp, err := h.dispatcher.DispatchCheck(h.dlCtx, dispatchReq())
		require.NoError(b, err)
		requireDispatchMember(b, resp, h.check.ResourceID)
	}
	{
		qctx := newLocalQueryContext(b, h)
		path, err := qctx.Check(h.iterator.Clone(), resourceObj, subjectObj)
		require.NoError(b, err)
		require.NotNil(b, path)
	}
	{
		qctx := newDispatchQueryContext(b, h)
		path, err := qctx.Check(h.iterator.Clone(), resourceObj, subjectObj)
		require.NoError(b, err)
		require.NotNil(b, path)
	}

	b.StartTimer()

	b.Run("dispatch", func(b *testing.B) {
		for b.Loop() {
			resp, err := h.dispatcher.DispatchCheck(h.dlCtx, dispatchReq())
			require.NoError(b, err)
			requireDispatchMember(b, resp, h.check.ResourceID)
		}
	})

	b.Run("queryplan_local", func(b *testing.B) {
		for b.Loop() {
			qctx := newLocalQueryContext(b, h)
			path, err := qctx.Check(h.iterator.Clone(), resourceObj, subjectObj)
			require.NoError(b, err)
			require.NotNil(b, path)
		}
	})

	b.Run("queryplan_dispatch", func(b *testing.B) {
		for b.Loop() {
			qctx := newDispatchQueryContext(b, h)
			path, err := qctx.Check(h.iterator.Clone(), resourceObj, subjectObj)
			require.NoError(b, err)
			require.NotNil(b, path)
		}
	})
}

func newLocalQueryContext(b *testing.B, h *executorHandle) *query.Context {
	b.Helper()
	return query.NewLocalContext(
		b.Context(),
		query.WithReader(query.NewQueryDatastoreReader(
			datalayer.NewDataLayer(h.ds).SnapshotReader(h.revision, h.schemaHash),
		)),
		query.WithCaveatRunner(caveats.NewCaveatRunner(caveattypes.Default.TypeSet)),
	)
}

func newDispatchQueryContext(b *testing.B, h *executorHandle) *query.Context {
	b.Helper()
	planCtx := dispatch.NewPlanContext(h.revision.String(), h.schemaHash, nil, classicDispatchDepthRemaining, 0)
	executor := dispatch.NewDispatchExecutor(h.dispatcher, planCtx, 100)
	return &query.Context{
		Context:      h.dlCtx,
		Executor:     executor,
		Reader:       query.NewQueryDatastoreReader(datalayer.NewDataLayer(h.ds).SnapshotReader(h.revision, h.schemaHash)),
		CaveatRunner: caveats.NewCaveatRunner(caveattypes.Default.TypeSet),
	}
}

func requireDispatchMember(b *testing.B, resp *dispatchv1.DispatchCheckResponse, resourceID string) {
	b.Helper()
	found, ok := resp.ResultsByResourceId[resourceID]
	require.True(b, ok, "no result for resource %q", resourceID)
	require.Equal(b, dispatchv1.ResourceCheckResult_MEMBER, found.Membership)
}
