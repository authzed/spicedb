package query

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

type retryTypedReader struct {
	datalayer.RevisionedReader
	calls int
	fail  bool
}

// QueryRelationships can fail after a yielded row to test incomplete scans.
func (r *retryTypedReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	r.calls++
	seq, err := r.RevisionedReader.QueryRelationships(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	return func(yield func(tuple.Relationship, error) bool) {
		for rel, err := range seq {
			if !yield(rel, err) {
				return
			}

			if r.fail {
				yield(tuple.Relationship{}, errors.New("partial scan failed"))
				return
			}
		}
	}, nil
}

func TestSharedTypedScansRetryFailuresAndOwnResults(t *testing.T) {
	converted, recording := batchTestReader(t, `definition user {}
definition group {
 relation member: user
}
definition team {
 relation member: user
}
definition document {
 relation viewer: group#member | team#member
}`, []tuple.Relationship{
		tuple.MustParse("document:d#viewer@group:g#member"),
		tuple.MustParse("document:d#viewer@team:t#member"),
	})
	group, err := converted.ResolveBaseRelation("document", "viewer", "group", "member", "", false, false)
	require.NoError(t, err)
	team, err := converted.ResolveBaseRelation("document", "viewer", "team", "member", "", false, false)
	require.NoError(t, err)
	reader := &retryTypedReader{RevisionedReader: recording.RevisionedReader, fail: true}
	ctx := NewLocalContext(
		t.Context(),
		WithRevisionedReader(reader),
		WithCheckExecution(CheckExecutionOptions{BroadSingleUsersetReads: true}),
	)
	resources := []Object{NewObject("document", "d"), NewObject("document", "d")}
	_, handled, err := sharedTypedSubjects(ctx, NewDatastoreIterator(group), resources)
	require.True(t, handled)
	require.ErrorContains(t, err, "partial scan failed")
	require.Empty(t, ctx.typedScans, "partial rows cannot establish a completed source scan")
	reader.fail = false

	paths, handled, err := sharedTypedSubjects(ctx, NewDatastoreIterator(group), resources)
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, 2, reader.calls, "a failed scan must be retried")
	require.Len(t, paths[0], 1)
	require.Len(t, paths[1], 1)
	paths[0][0].Subject.ObjectID = "mutated"
	require.Equal(t, "g", paths[1][0].Subject.ObjectID, "duplicate result slots own their paths")

	branch := ctx.forkForCheck(t.Context())
	paths, handled, err = sharedTypedSubjects(branch, NewDatastoreIterator(team), resources)
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, 2, reader.calls, "another typed group reuses the immutable scan")
	require.Equal(t, "t", paths[0][0].Subject.ObjectID)
	paths[0][0].Subject.ObjectID = "changed-by-branch"

	paths, handled, err = sharedTypedSubjects(ctx, NewDatastoreIterator(group), resources)
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, "g", paths[0][0].Subject.ObjectID)

	// Changing the fixed reader/revision must not reuse another snapshot's rows,
	// even if a standalone test reassigns Reader on an existing query Context.
	another := &retryTypedReader{RevisionedReader: recording.RevisionedReader}
	WithRevisionedReader(another)(ctx)
	_, handled, err = sharedTypedSubjects(ctx, NewDatastoreIterator(group), resources)
	require.True(t, handled)
	require.NoError(t, err)
	require.Equal(t, 1, another.calls)
}
