//go:build integration

package integrationtesting_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch"
	"github.com/authzed/spicedb/internal/dispatch/caching"
	"github.com/authzed/spicedb/internal/dispatch/keys"
	"github.com/authzed/spicedb/pkg/cache"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const benchTimedelta = 1 * time.Second

// dispatchQueryPlanHandle holds the precompiled state needed to run dispatch query plan benchmarks.
type dispatchQueryPlanHandle struct {
	ds         datastore.Datastore
	revision   datastore.Revision
	schemaHash datalayer.SchemaHash
	schema     *schema.Schema
}

func newDispatchQueryPlanHandle(b *testing.B, fileName string) *dispatchQueryPlanHandle {
	b.Helper()
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(b, 0, benchTimedelta, memdb.DisableGC)
	require.NoError(b, err)

	contents, err := testFiles.ReadFile(fileName)
	require.NoError(b, err)

	dl := datalayer.NewDataLayer(ds)

	populated, rev, err := validationfile.PopulateFromFilesContents(
		b.Context(), dl, caveattypes.Default.TypeSet,
		map[string][]byte{"testfile": contents},
	)
	require.NoError(b, err)

	_, schemaHash, err := dl.HeadRevision(b.Context())
	require.NoError(b, err)

	fullSchema, err := schema.BuildSchemaFromDefinitions(populated.NamespaceDefinitions, populated.CaveatDefinitions)
	require.NoError(b, err)

	return &dispatchQueryPlanHandle{ds: ds, revision: rev, schema: fullSchema, schemaHash: schemaHash}
}

// compileIterator builds, warms up, and compiles an iterator for the given
// resource type and permission. It runs a warm-up pass with a CountObserver to
// collect statistics, then applies a CountAdvisor to incorporate data-driven
// hints (e.g., arrow direction reversal) before returning the final iterator.
// The operation parameter determines which warm-up operation is used so the
// CountAdvisor sees realistic fan-out ratios for the actual query type.
// The resourceID and subjectID are used for the warm-up pass to exercise the
// same data paths as the actual benchmark queries.
func (h *dispatchQueryPlanHandle) compileIterator(b *testing.B, operation query.Operation, resourceType, resourceID, permission, subjectType, subjectID, subjectRelation string) query.Iterator {
	b.Helper()
	co, err := query.BuildOutlineFromSchema(h.schema, resourceType, permission)
	require.NoError(b, err)

	// Apply structural optimizations (reachability pruning, caveat pushdown, alias collapse).
	queryParams := queryopt.RequestParams{
		Operation:       operation,
		SubjectType:     subjectType,
		SubjectRelation: subjectRelation,
	}
	optimized, err := queryopt.ApplyOptimizations(co, queryopt.OptimizersForRequest(queryParams), queryParams)
	require.NoError(b, err)

	// Warm-up pass: run the iterator once with a CountObserver to
	// collect per-node statistics for the CountAdvisor.
	warmIt, err := optimized.Compile()
	require.NoError(b, err)

	obs := query.NewCountObserver()
	warmCtx := query.NewLocalContext(b.Context(),
		query.WithReader(query.NewQueryDatastoreReader(
			datalayer.NewDataLayer(h.ds).SnapshotReader(h.revision, h.schemaHash),
		)),
		query.WithObserver(obs),
		query.WithCaveatRunner(caveats.NewCaveatRunner(caveattypes.Default.TypeSet)),
	)

	resource := query.NewObject(resourceType, resourceID)
	subject := query.NewObject(subjectType, subjectID).WithEllipses()

	switch operation {
	case query.OperationCheck:
		_, err = warmCtx.Check(warmIt, resource, subject)
		require.NoError(b, err)
	case query.OperationIterResources:
		seq, werr := warmCtx.IterResources(warmIt, subject, query.NoObjectFilter())
		require.NoError(b, werr)
		_, werr = query.CollectAll(seq)
		require.NoError(b, werr)
	case query.OperationIterSubjects:
		seq, werr := warmCtx.IterSubjects(warmIt, resource, query.NoObjectFilter())
		require.NoError(b, werr)
		_, werr = query.CollectAll(seq)
		require.NoError(b, werr)
	}

	// Apply the CountAdvisor to incorporate data-driven hints (e.g., arrow
	// direction reversal) on top of the optimized canonical outline.
	advisor := query.NewCountAdvisor(obs.GetStats())
	advisedCO, err := query.ApplyAdvisor(optimized, advisor)
	require.NoError(b, err)

	it, err := advisedCO.Compile()
	require.NoError(b, err)
	return it
}

// newLocalContext creates a query context using LocalExecutor for baseline comparison.
func (h *dispatchQueryPlanHandle) newLocalContext(ctx context.Context) *query.Context {
	return query.NewLocalContext(ctx,
		query.WithRevisionedReader(datalayer.NewDataLayer(h.ds).SnapshotReader(h.revision, h.schemaHash)),
		query.WithCaveatRunner(caveats.NewCaveatRunner(caveattypes.Default.TypeSet)),
	)
}

// newDispatchContext creates a query context using a DispatchExecutor backed by a
// localQueryPlanDispatcher that handles DispatchQueryPlan by compiling and executing the plan locally.
func (h *dispatchQueryPlanHandle) newDispatchContext(ctx context.Context) *query.Context {
	planCtx := dispatch.NewPlanContext(h.revision.String(), h.schemaHash, nil, 0, 0)
	lpd := &localQueryPlanDispatcher{handle: h}
	executor := dispatch.NewDispatchExecutor(lpd, planCtx, 100)

	qctx := &query.Context{
		Context:       ctx,
		Executor:      executor,
		Reader:        query.NewQueryDatastoreReader(datalayer.NewDataLayer(h.ds).SnapshotReader(h.revision, h.schemaHash)),
		CaveatRunner:  caveats.NewCaveatRunner(caveattypes.Default.TypeSet),
		CaveatContext: nil,
	}
	return qctx
}

// newCachedDispatchContext creates a query context using a DispatchExecutor backed by
// a caching layer wrapping the localQueryPlanDispatcher. This exercises the Check cache path.
func (h *dispatchQueryPlanHandle) newCachedDispatchContext(b *testing.B, cachingDispatcher *caching.Dispatcher) *query.Context {
	planCtx := dispatch.NewPlanContext(h.revision.String(), h.schemaHash, nil, 0, 0)
	executor := dispatch.NewDispatchExecutor(cachingDispatcher, planCtx, 100)

	qctx := &query.Context{
		Context:       b.Context(),
		Executor:      executor,
		Reader:        query.NewQueryDatastoreReader(datalayer.NewDataLayer(h.ds).SnapshotReader(h.revision, h.schemaHash)),
		CaveatRunner:  caveats.NewCaveatRunner(caveattypes.Default.TypeSet),
		CaveatContext: nil,
	}
	return qctx
}

// newCachingDispatcher creates a caching dispatcher wrapping a localQueryPlanDispatcher.
func (h *dispatchQueryPlanHandle) newCachingDispatcher(b *testing.B) *caching.Dispatcher {
	b.Helper()
	cacheConfig := &cache.Config{
		NumCounters: 1e4,
		MaxCost:     1 << 20,
	}
	c, err := cache.NewStandardCache[keys.DispatchCacheKey, any](cacheConfig)
	require.NoError(b, err)

	cd, err := caching.NewCachingDispatcher(c, false, "bench", &keys.DirectKeyHandler{})
	require.NoError(b, err)

	lpd := &localQueryPlanDispatcher{handle: h}
	cd.SetDelegate(lpd)
	return cd
}

// localQueryPlanDispatcher is a minimal dispatcher that handles DispatchQueryPlan by compiling
// the plan from schema and executing it locally.
type localQueryPlanDispatcher struct {
	handle *dispatchQueryPlanHandle
}

func (d *localQueryPlanDispatcher) DispatchCheck(_ context.Context, _ *v1.DispatchCheckRequest) (*v1.DispatchCheckResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (d *localQueryPlanDispatcher) DispatchExpand(_ context.Context, _ *v1.DispatchExpandRequest) (*v1.DispatchExpandResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (d *localQueryPlanDispatcher) DispatchLookupResources2(_ *v1.DispatchLookupResources2Request, _ dispatch.LookupResources2Stream) error {
	return fmt.Errorf("not implemented")
}

func (d *localQueryPlanDispatcher) DispatchLookupResources3(_ *v1.DispatchLookupResources3Request, _ dispatch.LookupResources3Stream) error {
	return fmt.Errorf("not implemented")
}

func (d *localQueryPlanDispatcher) DispatchLookupSubjects(_ *v1.DispatchLookupSubjectsRequest, _ dispatch.LookupSubjectsStream) error {
	return fmt.Errorf("not implemented")
}
func (d *localQueryPlanDispatcher) Close() error { return nil }
func (d *localQueryPlanDispatcher) ReadyState() dispatch.ReadyState {
	return dispatch.ReadyState{IsReady: true}
}

func (d *localQueryPlanDispatcher) DispatchQueryPlan(req *v1.DispatchQueryPlanRequest, stream dispatch.PlanStream) error {
	ctx := stream.Context()

	// Find the iterator for this canonical key by compiling the full plan.
	// In the real implementation, this would be cached.
	it, err := d.findIterator(req)
	if err != nil {
		return err
	}

	// Build execution context with DispatchExecutor so nested aliases in the subtree
	// can re-dispatch to other nodes (multi-hop chains).
	//
	// We call it.CheckImpl / it.IterSubjectsImpl / it.IterResourcesImpl directly
	// (not qctx.Check) because the dispatch boundary has already been crossed.
	// Calling qctx.Check would route back through the executor, which would see the
	// alias and dispatch again — infinite loop. By entering the iterator's Impl method
	// directly, we execute the alias's own logic (relation rewriting, self-edge checks)
	// while letting anything underneath go through the executor normally.
	planCtx := req.PlanContext
	lpd := &localQueryPlanDispatcher{handle: d.handle}
	executor := dispatch.NewDispatchExecutor(lpd, planCtx, 100)

	qctx := &query.Context{
		Context:       ctx,
		Executor:      executor,
		Reader:        query.NewQueryDatastoreReader(datalayer.NewDataLayer(d.handle.ds).SnapshotReader(d.handle.revision, datalayer.NoSchemaHashForTesting)),
		CaveatRunner:  caveats.NewCaveatRunner(caveattypes.Default.TypeSet),
		CaveatContext: dispatch.CaveatContextFromPlanContext(planCtx),
	}

	resource := query.Object{ObjectType: req.Resource.Namespace, ObjectID: req.Resource.ObjectId}
	subject := query.ObjectAndRelation{
		ObjectType: req.Subject.Namespace,
		ObjectID:   req.Subject.ObjectId,
		Relation:   req.Subject.Relation,
	}

	switch req.Operation {
	case v1.PlanOperation_PLAN_OPERATION_CHECK:
		path, err := it.CheckImpl(qctx, resource, subject)
		if err != nil {
			return err
		}
		if path != nil {
			return stream.Publish(&v1.DispatchQueryPlanResponse{
				Paths: []*v1.ResultPath{dispatch.QueryPathToResultPath(path)},
			})
		}
		return nil

	case v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES:
		pathSeq, err := it.IterResourcesImpl(qctx, subject, query.NoObjectFilter())
		if err != nil {
			return err
		}
		for path, err := range pathSeq {
			if err != nil {
				return err
			}
			if err := stream.Publish(&v1.DispatchQueryPlanResponse{
				Paths: []*v1.ResultPath{dispatch.QueryPathToResultPath(path)},
			}); err != nil {
				return err
			}
		}
		return nil

	case v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS:
		pathSeq, err := it.IterSubjectsImpl(qctx, resource, query.NoObjectFilter())
		if err != nil {
			return err
		}
		for path, err := range pathSeq {
			if err != nil {
				return err
			}
			if err := stream.Publish(&v1.DispatchQueryPlanResponse{
				Paths: []*v1.ResultPath{dispatch.QueryPathToResultPath(path)},
			}); err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("unknown plan operation: %v", req.Operation)
	}
}

// findIterator compiles the full plan from schema and walks the iterator tree
// to find the subtree matching the requested canonical key.
func (d *localQueryPlanDispatcher) findIterator(req *v1.DispatchQueryPlanRequest) (query.Iterator, error) {
	targetKey := query.CanonicalKey(req.CanonicalKey)

	// Map PlanOperation to query.Operation for optimizer selection.
	var operation query.Operation
	switch req.Operation {
	case v1.PlanOperation_PLAN_OPERATION_CHECK:
		operation = query.OperationCheck
	case v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES:
		operation = query.OperationIterResources
	case v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS:
		operation = query.OperationIterSubjects
	}

	queryParams := queryopt.RequestParams{
		Operation:       operation,
		SubjectType:     req.Subject.Namespace,
		SubjectRelation: req.Subject.Relation,
	}

	for nsName, def := range d.handle.schema.Definitions() {
		for permName := range def.Permissions() {
			co, err := query.BuildOutlineFromSchema(d.handle.schema, nsName, permName)
			if err != nil {
				continue
			}

			optimized, err := queryopt.ApplyOptimizations(co, queryopt.OptimizersForRequest(queryParams), queryParams)
			if err != nil {
				continue
			}

			it, err := optimized.Compile()
			if err != nil {
				continue
			}
			if found := findByCanonicalKey(it, targetKey); found != nil {
				return found, nil
			}
		}
	}
	return nil, fmt.Errorf("no iterator found for canonical key %q", req.CanonicalKey)
}

// findByCanonicalKey recursively searches an iterator tree for a node with the given key.
func findByCanonicalKey(it query.Iterator, key query.CanonicalKey) query.Iterator {
	if it.CanonicalKey() == key {
		return it
	}
	for _, sub := range it.Subiterators() {
		if found := findByCanonicalKey(sub, key); found != nil {
			return found
		}
	}
	return nil
}

// --- Benchmarks ---

func BenchmarkDispatchQueryPlanCheck(b *testing.B) {
	tests := []struct {
		name                                    string
		file                                    string
		resourceType, resourceID, permission    string
		subjectType, subjectID, subjectRelation string
	}{
		{
			name:            "basic_rbac",
			file:            "testconfigs/basicrbac.yaml",
			resourceType:    "example/document",
			resourceID:      "firstdoc",
			permission:      "view",
			subjectType:     "example/user",
			subjectID:       "tom",
			subjectRelation: tuple.Ellipsis,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			handle := newDispatchQueryPlanHandle(b, tt.file)
			it := handle.compileIterator(b, query.OperationCheck, tt.resourceType, tt.resourceID, tt.permission, tt.subjectType, tt.subjectID, tt.subjectRelation)

			resource := query.Object{ObjectType: tt.resourceType, ObjectID: tt.resourceID}
			subject := query.ObjectAndRelation{ObjectType: tt.subjectType, ObjectID: tt.subjectID, Relation: tt.subjectRelation}

			b.Run("local_executor", func(b *testing.B) {
				for b.Loop() {
					qctx := handle.newLocalContext(b.Context())
					path, err := qctx.Check(it.Clone(), resource, subject)
					require.NoError(b, err)
					require.NotNil(b, path)
				}
			})

			b.Run("dispatch_executor", func(b *testing.B) {
				for b.Loop() {
					qctx := handle.newDispatchContext(b.Context())
					path, err := qctx.Check(it.Clone(), resource, subject)
					require.NoError(b, err)
					require.NotNil(b, path)
				}
			})

			b.Run("cached_dispatch_executor", func(b *testing.B) {
				cd := handle.newCachingDispatcher(b)
				for b.Loop() {
					qctx := handle.newCachedDispatchContext(b, cd)
					path, err := qctx.Check(it.Clone(), resource, subject)
					require.NoError(b, err)
					require.NotNil(b, path)
				}
			})
		})
	}
}

func BenchmarkDispatchQueryPlanLookupResources(b *testing.B) {
	tests := []struct {
		name                                    string
		file                                    string
		resourceType, permission                string
		subjectType, subjectID, subjectRelation string
		expectedResults                         int
	}{
		{
			name:            "basic_rbac",
			file:            "testconfigs/basicrbac.yaml",
			resourceType:    "example/document",
			permission:      "view",
			subjectType:     "example/user",
			subjectID:       "tom",
			subjectRelation: tuple.Ellipsis,
			// tom is writer on firstdoc and reader on seconddoc → 2 documents with view.
			expectedResults: 2,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			handle := newDispatchQueryPlanHandle(b, tt.file)
			it := handle.compileIterator(b, query.OperationIterResources, tt.resourceType, "", tt.permission, tt.subjectType, tt.subjectID, tt.subjectRelation)

			subject := query.ObjectAndRelation{ObjectType: tt.subjectType, ObjectID: tt.subjectID, Relation: tt.subjectRelation}

			b.Run("local_executor", func(b *testing.B) {
				for b.Loop() {
					qctx := handle.newLocalContext(b.Context())
					pathSeq, err := qctx.IterResources(it.Clone(), subject, query.NoObjectFilter())
					require.NoError(b, err)

					paths, err := query.CollectAll(pathSeq)
					require.NoError(b, err)
					require.Len(b, paths, tt.expectedResults)
				}
			})

			b.Run("dispatch_executor", func(b *testing.B) {
				for b.Loop() {
					qctx := handle.newDispatchContext(b.Context())
					pathSeq, err := qctx.IterResources(it.Clone(), subject, query.NoObjectFilter())
					require.NoError(b, err)

					paths, err := query.CollectAll(pathSeq)
					require.NoError(b, err)
					require.Len(b, paths, tt.expectedResults)
				}
			})
		})
	}
}

func BenchmarkDispatchQueryPlanLookupSubjects(b *testing.B) {
	tests := []struct {
		name                                 string
		file                                 string
		resourceType, resourceID, permission string
		subjectType, subjectRelation         string
		expectedResults                      int
	}{
		{
			name:            "basic_rbac",
			file:            "testconfigs/basicrbac.yaml",
			resourceType:    "example/document",
			resourceID:      "firstdoc",
			permission:      "view",
			subjectType:     "example/user",
			subjectRelation: tuple.Ellipsis,
			// tom is writer on firstdoc, fred is reader on firstdoc → 2 subjects with view.
			expectedResults: 2,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			handle := newDispatchQueryPlanHandle(b, tt.file)
			it := handle.compileIterator(b, query.OperationIterSubjects, tt.resourceType, tt.resourceID, tt.permission, tt.subjectType, "", tt.subjectRelation)

			resource := query.Object{ObjectType: tt.resourceType, ObjectID: tt.resourceID}

			b.Run("local_executor", func(b *testing.B) {
				for b.Loop() {
					qctx := handle.newLocalContext(b.Context())
					pathSeq, err := qctx.IterSubjects(it.Clone(), resource, query.NoObjectFilter())
					require.NoError(b, err)

					paths, err := query.CollectAll(pathSeq)
					require.NoError(b, err)
					require.Len(b, paths, tt.expectedResults)
				}
			})

			b.Run("dispatch_executor", func(b *testing.B) {
				for b.Loop() {
					qctx := handle.newDispatchContext(b.Context())
					pathSeq, err := qctx.IterSubjects(it.Clone(), resource, query.NoObjectFilter())
					require.NoError(b, err)

					paths, err := query.CollectAll(pathSeq)
					require.NoError(b, err)
					require.Len(b, paths, tt.expectedResults)
				}
			})
		})
	}
}
