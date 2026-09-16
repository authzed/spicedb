package datastore_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// scriptedCall records the delete options one batch was invoked with.
type scriptedCall struct {
	limit    uint64
	after    options.Cursor
	cursored bool
}

// scriptedDatastore replays a fixed sequence of delete results, recording the
// options each batch was called with. Only the methods BulkDeleteRelationships
// actually uses are implemented; the rest panic via the embedded nil interface.
type scriptedDatastore struct {
	datastore.Datastore

	cursored bool
	results  []datastore.DeleteRelationshipsResult
	calls    []scriptedCall

	// rwtOpts records the resolved read-write options each batch's transaction
	// was opened with.
	rwtOpts []options.RWTOptions

	// afterEachBatch, if set, runs after each batch; used to cancel a context
	// mid-loop.
	afterEachBatch func(batch int)
}

func (s *scriptedDatastore) SupportsCursoredDelete() bool { return s.cursored }

func (s *scriptedDatastore) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	s.rwtOpts = append(s.rwtOpts, *options.NewRWTOptionsWithOptions(opts...))

	if err := f(ctx, &scriptedRWT{ds: s}); err != nil {
		return nil, err
	}

	if s.afterEachBatch != nil {
		s.afterEachBatch(len(s.calls))
	}

	return nil, nil
}

type scriptedRWT struct {
	datastore.ReadWriteTransaction
	ds *scriptedDatastore
}

func (r *scriptedRWT) DeleteRelationships(
	_ context.Context,
	_ *v1.RelationshipFilter,
	opts ...options.DeleteOptionsOption,
) (datastore.DeleteRelationshipsResult, error) {
	delOpts := options.NewDeleteOptionsWithOptions(opts...)

	call := scriptedCall{cursored: delOpts.CursoredDelete, after: delOpts.DeleteAfter}
	if delOpts.DeleteLimit != nil {
		call.limit = *delOpts.DeleteLimit
	}
	r.ds.calls = append(r.ds.calls, call)

	if len(r.ds.results) == 0 {
		return datastore.DeleteRelationshipsResult{}, nil
	}

	next := r.ds.results[0]
	r.ds.results = r.ds.results[1:]
	return next, nil
}

func cursorFor(t *testing.T, s string) options.Cursor {
	t.Helper()
	return options.ToCursor(tuple.MustParse(s))
}

func TestBulkDeleteRelationshipsCursoredAdvancesCursor(t *testing.T) {
	c1 := cursorFor(t, "document:doc10#viewer@user:alice")
	c2 := cursorFor(t, "document:doc20#viewer@user:alice")

	ds := &scriptedDatastore{
		cursored: true,
		results: []datastore.DeleteRelationshipsResult{
			{NumDeleted: 10, LimitReached: true, Cursor: c1},
			{NumDeleted: 10, LimitReached: true, Cursor: c2},
			{}, // pass 1 drains
			{}, // pass 2 (the second sweep) finds nothing
		},
	}

	progress, err := datastore.BulkDeleteRelationships(
		t.Context(), ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 10})
	require.NoError(t, err)

	require.Equal(t, uint64(20), progress.TotalDeleted)
	require.Equal(t, uint64(2), progress.Batches)
	require.True(t, progress.Cursored)

	require.Len(t, ds.calls, 4)
	require.Nil(t, ds.calls[0].after, "the first batch starts at the beginning")
	require.Equal(t, c1, ds.calls[1].after)
	require.Equal(t, c2, ds.calls[2].after)
	require.Nil(t, ds.calls[3].after, "the second sweep restarts at the beginning")

	for i, call := range ds.calls {
		require.True(t, call.cursored, "call %d did not request a cursored delete", i)
		require.Equal(t, uint64(10), call.limit, "call %d used the wrong limit", i)
	}
}

func TestBulkDeleteRelationshipsSkipsCommitRevision(t *testing.T) {
	// The command discards the per-batch commit revision, so each batch must
	// open its transaction with SkipCommitRevision set: on CockroachDB that
	// elides a SHOW COMMIT TIMESTAMP round trip on every batch.
	ds := &scriptedDatastore{
		cursored: true,
		results: []datastore.DeleteRelationshipsResult{
			{NumDeleted: 10, LimitReached: true, Cursor: cursorFor(t, "document:doc10#viewer@user:alice")},
			{}, // pass 1 drains
			{}, // second sweep finds nothing
		},
	}

	_, err := datastore.BulkDeleteRelationships(
		t.Context(), ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 10})
	require.NoError(t, err)

	require.NotEmpty(t, ds.rwtOpts)
	for i, opt := range ds.rwtOpts {
		require.True(t, opt.SkipCommitRevision, "batch %d did not skip the commit revision", i)
	}
}

func TestBulkDeleteRelationshipsFallbackWhenUnsupported(t *testing.T) {
	ds := &scriptedDatastore{
		cursored: false,
		results: []datastore.DeleteRelationshipsResult{
			{NumDeleted: 10, LimitReached: true},
			{NumDeleted: 3, LimitReached: false},
		},
	}

	progress, err := datastore.BulkDeleteRelationships(
		t.Context(), ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 10})
	require.NoError(t, err)

	require.Equal(t, uint64(13), progress.TotalDeleted)
	require.False(t, progress.Cursored)
	require.Equal(t, uint64(1), progress.Pass, "the fallback path needs no second sweep")

	require.Len(t, ds.calls, 2)
	for i, call := range ds.calls {
		require.False(t, call.cursored, "call %d requested a cursored delete", i)
		require.Nil(t, call.after, "call %d supplied a cursor", i)
	}
}

func TestBulkDeleteRelationshipsSecondSweepCatchesLateInserts(t *testing.T) {
	c1 := cursorFor(t, "document:doc10#viewer@user:alice")
	c2 := cursorFor(t, "document:doc02#viewer@user:alice")

	ds := &scriptedDatastore{
		cursored: true,
		results: []datastore.DeleteRelationshipsResult{
			{NumDeleted: 10, LimitReached: true, Cursor: c1},
			{}, // pass 1 drains
			// Two rows were inserted behind the cursor during pass 1.
			{NumDeleted: 2, LimitReached: false, Cursor: c2},
			{}, // pass 2 drains
			{}, // pass 3 finds nothing, so the loop ends
		},
	}

	progress, err := datastore.BulkDeleteRelationships(
		t.Context(), ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 10})
	require.NoError(t, err)

	require.Equal(t, uint64(12), progress.TotalDeleted)
	require.Equal(t, uint64(3), progress.Pass)

	// The first call of pass 2 and of pass 3 must restart at the beginning.
	require.Nil(t, ds.calls[2].after)
	require.Nil(t, ds.calls[4].after)
}

func TestBulkDeleteRelationshipsResumeCursor(t *testing.T) {
	resume := cursorFor(t, "document:doc99#viewer@user:alice")

	ds := &scriptedDatastore{
		cursored: true,
		results:  []datastore.DeleteRelationshipsResult{{}},
	}

	_, err := datastore.BulkDeleteRelationships(
		t.Context(), ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 10, ResumeCursor: resume})
	require.NoError(t, err)

	require.NotEmpty(t, ds.calls)
	require.Equal(t, resume, ds.calls[0].after)
}

func TestBulkDeleteRelationshipsHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	c1 := cursorFor(t, "document:doc10#viewer@user:alice")
	ds := &scriptedDatastore{
		cursored: true,
		results: []datastore.DeleteRelationshipsResult{
			{NumDeleted: 10, LimitReached: true, Cursor: c1},
			{NumDeleted: 10, LimitReached: true, Cursor: c1},
		},
	}
	ds.afterEachBatch = func(batch int) {
		if batch == 1 {
			cancel()
		}
	}

	progress, err := datastore.BulkDeleteRelationships(
		ctx, ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 10})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, uint64(10), progress.TotalDeleted)
}

func TestBulkDeleteRelationshipsRejectsZeroBatchSize(t *testing.T) {
	ds := &scriptedDatastore{cursored: true}

	_, err := datastore.BulkDeleteRelationships(
		t.Context(), ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 0})
	require.ErrorContains(t, err, "batch size")
	require.Empty(t, ds.calls, "the datastore must not be touched")
}

func TestBulkDeleteRelationshipsSleepsBetweenBatches(t *testing.T) {
	c1 := cursorFor(t, "document:doc10#viewer@user:alice")
	ds := &scriptedDatastore{
		cursored: true,
		results: []datastore.DeleteRelationshipsResult{
			{NumDeleted: 10, LimitReached: true, Cursor: c1},
			{},
			{},
		},
	}

	start := time.Now()
	_, err := datastore.BulkDeleteRelationships(
		t.Context(), ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 10, SleepBetweenBatches: 50 * time.Millisecond})
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}

// TestBulkDeleteRelationshipsResumeCursorForcesSweepEvenWhenFirstPassDeletesNothing
// guards against a resumed run reporting "complete" without ever sweeping
// from the beginning. A run resumed with --resume-cursor positioned near the
// end of the range can find nothing on its first pass (everything after the
// cursor is already gone) while rows inserted earlier in the range, before
// the original interrupted run reached them, are still sitting unvisited
// before that cursor. Pass 1 deleting zero must not be treated the same as
// "nothing left at all": at least one more pass starting from nil is
// required.
func TestBulkDeleteRelationshipsResumeCursorForcesSweepEvenWhenFirstPassDeletesNothing(t *testing.T) {
	resume := cursorFor(t, "document:doc99#viewer@user:alice")
	c := cursorFor(t, "document:doc05#viewer@user:alice")

	ds := &scriptedDatastore{
		cursored: true,
		results: []datastore.DeleteRelationshipsResult{
			{}, // pass 1: nothing left after the resume position
			{NumDeleted: 5, LimitReached: false, Cursor: c}, // pass 2 (forced): a row from earlier in the range
			{}, // pass 2 drains
			{}, // pass 3 finds nothing, so the loop ends
		},
	}

	progress, err := datastore.BulkDeleteRelationships(
		t.Context(), ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 10, ResumeCursor: resume})
	require.NoError(t, err)

	require.Equal(t, uint64(5), progress.TotalDeleted)
	require.Equal(t, uint64(3), progress.Pass,
		"a resumed run whose first pass deletes nothing must still sweep from the beginning")

	require.Len(t, ds.calls, 4)
	require.Equal(t, resume, ds.calls[0].after, "pass 1 starts at the resume cursor")
	require.Nil(t, ds.calls[1].after, "the forced sweep restarts at the beginning")
	require.Nil(t, ds.calls[3].after, "pass 3 restarts at the beginning")
}

// TestBulkDeleteRelationshipsWarnsWhenResumeCursorIgnoredByFallback guards
// against an operator resuming a run against a datastore that does not
// support cursored deletion (e.g. Postgres) and silently getting a full
// re-delete: only the generic "falling back to the slower path" warning
// fired, with nothing calling out that --resume-cursor specifically was
// dropped.
func TestBulkDeleteRelationshipsWarnsWhenResumeCursorIgnoredByFallback(t *testing.T) {
	resume := cursorFor(t, "document:doc99#viewer@user:alice")

	ds := &scriptedDatastore{
		cursored: false,
		results: []datastore.DeleteRelationshipsResult{
			{NumDeleted: 3, LimitReached: false},
		},
	}

	var buf bytes.Buffer
	ctx := zerolog.New(&buf).WithContext(t.Context())

	_, err := datastore.BulkDeleteRelationships(
		ctx, ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 10, ResumeCursor: resume})
	require.NoError(t, err)

	require.Contains(t, buf.String(), "resume-cursor",
		"an operator resuming against a non-cursored datastore must be warned explicitly that the cursor was dropped")
}

// TestBulkDeleteRelationshipsErrorsOnCursoredResultWithNilCursor guards the
// only thing preventing an infinite delete loop against a misbehaving
// datastore: if the cursored path reports rows deleted but returns a nil
// cursor, BulkDeleteRelationships must error rather than loop (a nil cursor
// on the next batch would re-run the same unbounded query forever).
// Mutation testing found this guard has no coverage of its own; deleting it
// leaves every other test green.
func TestBulkDeleteRelationshipsErrorsOnCursoredResultWithNilCursor(t *testing.T) {
	ds := &scriptedDatastore{
		cursored: true,
		results: []datastore.DeleteRelationshipsResult{
			{NumDeleted: 5, LimitReached: true, Cursor: nil},
		},
	}

	progress, err := datastore.BulkDeleteRelationships(
		t.Context(), ds, &v1.RelationshipFilter{ResourceType: "document"},
		datastore.BulkDeleteOptions{BatchSize: 10})
	require.ErrorContains(t, err, "without a cursor")
	require.Equal(t, uint64(5), progress.TotalDeleted,
		"the batch that triggered the guard still counted toward progress before the error was returned")

	require.Len(t, ds.calls, 1, "the guard must stop the loop after the first offending batch, not continue looping")
}
