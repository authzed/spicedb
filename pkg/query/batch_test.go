package query

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
	schema "github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

type batchRecordingReader struct {
	datalayer.RevisionedReader
	filters []datastore.RelationshipsFilter
	shapes  []queryshape.Shape
}

func (r *batchRecordingReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	r.filters = append(r.filters, filter)
	r.shapes = append(r.shapes, options.NewQueryOptionsWithOptions(opts...).QueryShape)
	return r.RevisionedReader.QueryRelationships(ctx, filter, opts...)
}

func batchTestReader(t interface {
	require.TestingT
	Helper()
	Context() context.Context
	Cleanup(func())
}, text string, relationships []tuple.Relationship,
) (*schema.Schema, *batchRecordingReader) {
	t.Helper()
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ds.Close()) })
	_, err = datalayer.WriteStoredSchemaForTest(t.Context(), ds, text)
	require.NoError(t, err)
	rev, err := common.WriteRelationships(t.Context(), ds, tuple.UpdateOperationCreate, relationships...)
	require.NoError(t, err)
	compiled, err := compiler.Compile(
		compiler.InputSchema{Source: input.Source("batch-test"), SchemaString: text},
		compiler.AllowUnprefixedObjectType(),
	)
	require.NoError(t, err)
	converted, err := schema.BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	return converted, &batchRecordingReader{
		RevisionedReader: datalayer.NewDataLayer(ds).SnapshotReader(rev, datalayer.NoSchemaHashForTesting),
	}
}

func TestBatchChecksGenerateEquivalentResourcesAndSubjects(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate sparse membership and userset edges, then check unions,
		// intersections and recursive arrows through the same shared batch APIs.
		text := `definition user {}
definition group {
 relation member: user | group#member
}
definition document {
 relation reader: user | user:*
 relation writer: user
 relation group: group
 permission view = reader + group->member
 permission edit = reader & writer
}`
		relationships := []tuple.Relationship{}

		for resource := range 4 {
			for subject := range 3 {
				for _, relation := range []string{"reader", "writer"} {
					if rapid.Bool().Draw(rt, fmt.Sprintf("%s-%d-%d", relation, resource, subject)) {
						relationships = append(
							relationships,
							tuple.MustParse(fmt.Sprintf("document:d%d#%s@user:u%d", resource, relation, subject)),
						)
					}
				}

				if rapid.Bool().Draw(rt, fmt.Sprintf("member-%d-%d", resource, subject)) {
					relationships = append(relationships, tuple.MustParse(fmt.Sprintf("group:g%d#member@user:u%d", resource, subject)))
				}
			}

			relationships = append(relationships, tuple.MustParse(fmt.Sprintf("document:d%d#group@group:g%d", resource, resource)))
		}

		if rapid.Bool().Draw(rt, "wildcard") {
			relationships = append(relationships, tuple.MustParse("document:d0#reader@user:*"))
		}

		if rapid.Bool().Draw(rt, "recursive") {
			relationships = append(relationships, tuple.MustParse("group:g0#member@group:g1#member"))
		}

		converted, reader := batchTestReader(rt, text, relationships)
		resources := []Object{
			NewObject("document", "d0"),
			NewObject("document", "d1"),
			NewObject("document", "d2"),
			NewObject("document", "d3"),
			NewObject("document", "missing"),
			NewObject("document", "d0"),
		}
		subjects := []ObjectAndRelation{
			NewObject("user", "u0").WithEllipses(),
			NewObject("user", "u1").WithEllipses(),
			NewObject("user", "u2").WithEllipses(),
			NewObject("user", "missing").WithEllipses(),
			NewObject("group", "g0").WithRelation("member"),
			NewObject("user", "u0").WithEllipses(),
		}

		for _, permission := range []string{"reader", "view", "edit"} {
			outline, err := BuildOutlineFromSchema(converted, "document", permission, WithSchemaExecutionOrder(), WithDeferredCaveats())
			require.NoError(rt, err)
			it, err := outline.Compile()
			require.NoError(rt, err)
			reference := NewLocalContext(
				rt.Context(),
				WithRevisionedReader(reader),
				WithBatchedArrows(false),
				WithCheckExecution(
					CheckExecutionOptions{
						TargetedRecursion:       true,
						StrictSubjectMatching:   true,
						CoalesceDirectWildcards: true,
						BroadSingleUsersetReads: true,
					},
				),
			)
			batched := NewLocalContext(
				rt.Context(),
				WithRevisionedReader(reader),
				WithBatchedArrows(true),
				WithCheckExecution(
					CheckExecutionOptions{
						BatchChecks:             true,
						BatchSize:               2,
						TargetedRecursion:       true,
						StrictSubjectMatching:   true,
						CoalesceDirectWildcards: true,
						BroadSingleUsersetReads: true,
					},
				),
			)

			for _, subject := range subjects {
				actual, err := batched.CheckManyResources(it, resources, subject)
				require.NoError(rt, err)
				partial, err := batched.CheckManyResources(it, resources, subject, AllowSingleResult)
				require.NoError(rt, err)
				assertAnyBatchEquivalent(rt, actual, partial)

				for i, resource := range resources {
					expected, err := reference.Check(it, resource, subject)
					require.NoError(rt, err)
					require.Equal(rt, expected, actual[i], "%s resource=%v subject=%v", permission, resource, subject)
				}
			}

			for _, resource := range resources {
				actual, err := batched.CheckManySubjects(it, resource, subjects)
				require.NoError(rt, err)
				partial, err := batched.CheckManySubjects(it, resource, subjects, AllowSingleResult)
				require.NoError(rt, err)
				assertAnyBatchEquivalent(rt, actual, partial)

				for i, subject := range subjects {
					expected, err := reference.Check(it, resource, subject)
					require.NoError(rt, err)
					require.Equal(rt, expected, actual[i], "%s resource=%v subject=%v", permission, resource, subject)
				}
			}
		}
	})
}

func TestBatchDirectReadsChunkAndKeepDuplicatePathsIndependent(t *testing.T) {
	converted, reader := batchTestReader(t, `definition user {}
definition document {
 relation viewer: user
}`, []tuple.Relationship{
		tuple.MustParse("document:d0#viewer@user:u0"),
		tuple.MustParse("document:d1#viewer@user:u0"),
	})
	outline, err := BuildOutlineFromSchema(converted, "document", "viewer")
	require.NoError(t, err)
	it, err := outline.Compile()
	require.NoError(t, err)
	ctx := NewLocalContext(
		t.Context(),
		WithRevisionedReader(reader),
		WithCheckExecution(CheckExecutionOptions{BatchChecks: true, BatchSize: 2}),
	)
	paths, err := ctx.CheckManyResources(
		it,
		[]Object{NewObject("document", "d0"), NewObject("document", "d0"), NewObject("document", "d1"), NewObject("document", "missing")},
		NewObject("user", "u0").WithEllipses(),
	)
	require.NoError(t, err)
	require.Len(t, reader.filters, 2)
	require.Len(t, reader.filters[0].OptionalResourceIds, 1)
	require.Nil(t, paths[3])
	require.NotSame(t, paths[0], paths[1])
	paths[0].Relation = "modified"
	require.Equal(t, "viewer", paths[1].Relation)
	require.Equal(t, "d1", paths[2].Resource.ObjectID)
}

func TestBatchExclusionKeepsVectorAndChecksEveryBaseCandidate(t *testing.T) {
	converted, reader := batchTestReader(t, `definition user {}
definition document {
 relation viewer: user
 relation banned: user
 permission view = viewer - banned
}`, []tuple.Relationship{
		tuple.MustParse("document:d0#viewer@user:u"),
		tuple.MustParse("document:d0#banned@user:u"),
		tuple.MustParse("document:d1#viewer@user:u"),
	})
	outline, err := BuildOutlineFromSchema(converted, "document", "view", WithSchemaExecutionOrder(), WithDeferredCaveats())
	require.NoError(t, err)
	it, err := outline.Compile()
	require.NoError(t, err)
	ctx := NewLocalContext(t.Context(), WithRevisionedReader(reader), WithCheckExecution(CheckExecutionOptions{
		BatchChecks: true, BaseFirstExclusion: true,
	}))
	paths, err := ctx.CheckManyResources(
		it,
		[]Object{NewObject("document", "d0"), NewObject("document", "d1")},
		NewObject("user", "u").WithEllipses(),
		AllowSingleResult,
	)
	require.NoError(t, err)
	require.Nil(t, paths[0])
	require.NotNil(t, paths[1], "the first base grant is excluded; the second candidate must remain")
	require.Len(t, reader.filters, 2, "each operand must retain the complete target vector in one read")
}

func TestBatchWildcardPreservesScalarSubjectValidation(t *testing.T) {
	converted, reader := batchTestReader(t, `definition user {}
definition document {
 relation viewer: user:*
}`, []tuple.Relationship{tuple.MustParse("document:d#viewer@user:*")})
	outline, err := BuildOutlineFromSchema(converted, "document", "viewer")
	require.NoError(t, err)
	it, err := outline.Compile()
	require.NoError(t, err)
	ctx := NewLocalContext(t.Context(), WithRevisionedReader(reader), WithCheckExecution(CheckExecutionOptions{BatchChecks: true}))
	require.Panics(t, func() {
		_, _ = ctx.Check(it, NewObject("document", "d"), NewObject("user", "u").WithRelation("member"))
	})
	require.Panics(t, func() {
		_, _ = ctx.CheckManySubjects(it, NewObject("document", "d"), []ObjectAndRelation{NewObject("user", "u").WithRelation("member")})
	}, "wildcard checks require an ellipsis subject, including in batches")
}

func TestBatchArrowRetainsIncomingCaveatProvenance(t *testing.T) {
	converted, reader := batchTestReader(t, `caveat enabled(ok bool) { ok }
definition user {}
definition group {
 relation member: user
}
definition document {
 relation parent: group with enabled
 permission view = parent->member
}`, []tuple.Relationship{
		tuple.MustParse("document:d0#parent@group:g[enabled:{\"ok\":true}]"),
		tuple.MustParse("document:d1#parent@group:g[enabled:{\"ok\":false}]"),
		tuple.MustParse("group:g#member@user:u"),
	})
	outline, err := BuildOutlineFromSchema(converted, "document", "view", WithSchemaExecutionOrder(), WithDeferredCaveats())
	require.NoError(t, err)
	it, err := outline.Compile()
	require.NoError(t, err)
	resources := []Object{NewObject("document", "d0"), NewObject("document", "d1"), NewObject("document", "d0")}
	subject := NewObject("user", "u").WithEllipses()
	ctx := NewLocalContext(
		t.Context(),
		WithRevisionedReader(reader),
		WithBatchedArrows(true),
		WithCheckExecution(CheckExecutionOptions{BatchChecks: true, StrictSubjectMatching: true}),
	)
	paths, err := ctx.CheckManyResources(it, resources, subject)
	require.NoError(t, err)
	require.Len(t, reader.filters, 2, "shared intermediate membership is checked once")
	require.NotSame(t, paths[0], paths[2])
	require.NotSame(t, paths[0].Caveat, paths[2].Caveat)
	require.True(t, paths[0].Caveat.GetCaveat().Context.Fields["ok"].GetBoolValue())
	require.False(t, paths[1].Caveat.GetCaveat().Context.Fields["ok"].GetBoolValue())

	// Returning one shared intermediate must not overwrite a different incoming
	// edge's caveat, or share mutable expressions between duplicate input slots.
	paths[0].Caveat.GetCaveat().CaveatName = "changed"
	require.Equal(t, "enabled", paths[2].Caveat.GetCaveat().CaveatName)
}

func TestBatchUnionRetainsConditionalGrantWhenLaterChildDenies(t *testing.T) {
	converted, reader := batchTestReader(t, `caveat enabled(ok bool) { ok }
definition user {}
definition document {
 relation viewer: user with enabled
 relation editor: user
 permission view = viewer + editor
}`, []tuple.Relationship{tuple.MustParse("document:d0#viewer@user:u0[enabled]")})
	outline, err := BuildOutlineFromSchema(converted, "document", "view", WithSchemaExecutionOrder(), WithDeferredCaveats())
	require.NoError(t, err)
	it, err := outline.Compile()
	require.NoError(t, err)
	ctx := NewLocalContext(
		t.Context(),
		WithRevisionedReader(reader),
		WithCheckExecution(CheckExecutionOptions{BatchChecks: true, StrictSubjectMatching: true}),
	)
	resource := NewObject("document", "d0")
	subject := NewObject("user", "u0").WithEllipses()

	resources, err := ctx.CheckManyResources(it, []Object{resource, NewObject("document", "missing")}, subject)
	require.NoError(t, err)
	require.NotNil(t, resources[0])
	require.NotNil(t, resources[0].Caveat)
	require.Nil(t, resources[1])

	subjects, err := ctx.CheckManySubjects(it, resource, []ObjectAndRelation{subject, NewObject("user", "missing").WithEllipses()})
	require.NoError(t, err)
	require.NotNil(t, subjects[0])
	require.NotNil(t, subjects[0].Caveat)
	require.Nil(t, subjects[1])
}

func TestBatchArrowFilteredTuplesetUsesCompatibleQueryShape(t *testing.T) {
	converted, reader := batchTestReader(t, `definition user {}
definition group {
 relation member: user
}
definition document {
 relation parent: group
 permission view = parent->member
}`, []tuple.Relationship{tuple.MustParse("document:d#parent@group:g"), tuple.MustParse("group:g#member@user:u")})
	outline, err := BuildOutlineFromSchema(converted, "document", "view")
	require.NoError(t, err)
	it, err := outline.Compile()
	require.NoError(t, err)
	ctx := NewLocalContext(
		t.Context(),
		WithRevisionedReader(reader),
		WithCheckExecution(CheckExecutionOptions{BatchChecks: true, StrictSubjectMatching: true}),
	)
	paths, err := ctx.CheckManyResources(it, []Object{NewObject("document", "d")}, NewObject("user", "u").WithEllipses())
	require.NoError(t, err)
	require.NotNil(t, paths[0])
	require.Equal(t, "group", reader.filters[0].OptionalSubjectsSelectors[0].OptionalSubjectType)
	require.Equal(t, queryshape.Varying, reader.shapes[0], "filtered tuplesets must not force the complete-primary-key shape")
}

// assertAnyBatchEquivalent checks that partial grants are valid and that a
// batch without an unconditional grant still returns the complete population.
func assertAnyBatchEquivalent(t require.TestingT, complete, partial []*Path) {
	require.Len(t, partial, len(complete))
	require.Equal(t, hasUnconditionalGrant(complete), hasUnconditionalGrant(partial))

	if !hasUnconditionalGrant(complete) {
		require.Equal(t, complete, partial)
		return
	}

	for i, path := range partial {
		if path != nil && path.Caveat == nil {
			require.NotNil(t, complete[i])
			require.Nil(t, complete[i].Caveat)
		}
	}
}

func TestBatchAnyRetainsIntersectionCandidates(t *testing.T) {
	converted, reader := batchTestReader(t, `definition user {}
definition document {
 relation reader: user
 relation writer: user
 permission edit = reader & writer
}`, []tuple.Relationship{
		tuple.MustParse("document:d0#reader@user:u0"),
		tuple.MustParse("document:d1#reader@user:u0"),
		tuple.MustParse("document:d1#writer@user:u0"),
		tuple.MustParse("document:d0#reader@user:u1"),
		tuple.MustParse("document:d0#writer@user:u1"),
	})
	outline, err := BuildOutlineFromSchema(converted, "document", "edit", WithSchemaExecutionOrder())
	require.NoError(t, err)
	it, err := outline.Compile()
	require.NoError(t, err)
	ctx := NewLocalContext(t.Context(), WithRevisionedReader(reader), WithCheckExecution(CheckExecutionOptions{BatchChecks: true}))
	paths, err := ctx.CheckManyResources(
		it,
		[]Object{NewObject("document", "d0"), NewObject("document", "d1")},
		NewObject("user", "u0").WithEllipses(),
		AllowSingleResult,
	)
	require.NoError(t, err)
	require.Nil(t, paths[0])
	require.NotNil(t, paths[1], "the reader grant on d0 must not hide the reader candidate on d1")
	paths, err = ctx.CheckManySubjects(
		it,
		NewObject("document", "d0"),
		[]ObjectAndRelation{NewObject("user", "u0").WithEllipses(), NewObject("user", "u1").WithEllipses()},
		AllowSingleResult,
	)
	require.NoError(t, err)
	require.Nil(t, paths[0])
	require.NotNil(t, paths[1])
}

func TestBatchAnyObservesSkippedChunksAsIncomplete(t *testing.T) {
	recorder := &executionRecorder{}
	ctx := NewLocalContext(
		t.Context(),
		WithExecutionObserver(recorder),
		WithCheckExecution(CheckExecutionOptions{BatchChecks: true, BatchSize: 1}),
	)
	it := NewFixedIterator(*MustPathFromString("document:d0#viewer@user:u"))
	paths, err := ctx.CheckManyResources(
		it,
		[]Object{NewObject("document", "d0"), NewObject("document", "d1")},
		NewObject("user", "u").WithEllipses(),
		AllowSingleResult,
	)
	require.NoError(t, err)
	require.NotNil(t, paths[0])
	require.Nil(t, paths[1])
	require.Len(t, recorder.events, 1)
	event := recorder.events[0]
	require.Equal(t, signal.ConsumerStopped, event.Status)
	require.Equal(t, "many-resources/any", event.Key.Mode)
	require.EqualValues(t, 2, event.BatchSize)
	require.EqualValues(t, 1, event.Granted)
	require.Zero(t, event.Denied, "unvisited targets must not train denial frequencies")
}

func TestBatchAnyPreservesCompleteLegacyObserverCallbacks(t *testing.T) {
	recorder := &executionRecorder{}
	ctx := NewLocalContext(
		t.Context(),
		WithObserver(NewAnalyzeObserver()),
		WithExecutionObserver(recorder),
		WithCheckExecution(CheckExecutionOptions{BatchChecks: true, BatchSize: 1}),
	)
	it := NewFixedIterator(*MustPathFromString("document:d0#viewer@user:u"))
	_, err := ctx.CheckManyResources(
		it,
		[]Object{NewObject("document", "d0"), NewObject("document", "d1")},
		NewObject("user", "u").WithEllipses(),
		AllowSingleResult,
	)
	require.NoError(t, err)
	require.Len(t, recorder.events, 1)
	require.Equal(t, signal.Complete, recorder.events[0].Status)
	require.Equal(t, "many-resources", recorder.events[0].Key.Mode)
	require.EqualValues(t, 1, recorder.events[0].Denied)
}

func TestBatchAnyArrowRetainsEveryCaveatedConnectingEdge(t *testing.T) {
	converted, reader := batchTestReader(t, `caveat enabled(ok bool) { ok }
definition user {}
definition group {
 relation member: user
}
definition document {
 relation parent: group with enabled
 permission view = parent->member
}`, []tuple.Relationship{
		tuple.MustParse("document:d#parent@group:g0[enabled:{\"ok\":false}]"),
		tuple.MustParse("document:d#parent@group:g1[enabled:{\"ok\":true}]"),
		tuple.MustParse("group:g0#member@user:u"),
		tuple.MustParse("group:g1#member@user:u"),
	})
	outline, err := BuildOutlineFromSchema(converted, "document", "view", WithSchemaExecutionOrder(), WithDeferredCaveats())
	require.NoError(t, err)
	it, err := outline.Compile()
	require.NoError(t, err)
	resource := NewObject("document", "d")
	subject := NewObject("user", "u").WithEllipses()
	options := CheckExecutionOptions{BatchChecks: true, BatchSize: 1}
	batched := NewLocalContext(t.Context(), WithRevisionedReader(reader), WithBatchedArrows(true), WithCheckExecution(options))
	reference := NewLocalContext(t.Context(), WithRevisionedReader(reader), WithCheckExecution(options))
	actual, err := batched.Check(it, resource, subject)
	require.NoError(t, err)
	expected, err := reference.Check(it, resource, subject)
	require.NoError(t, err)
	require.Equal(t, expected, actual, "a grant through the false edge must not hide the true connecting edge")
	require.Len(t, actual.Caveat.GetOperation().Children, 2)
}
