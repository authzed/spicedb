//go:build datastore && (postgres || crdb || mysql)

package embedded_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/embedded"
	"github.com/authzed/spicedb/pkg/tuple"
)

type stageRelationships func(context.Context, func(context.Context, *embedded.RelationshipTransaction) error) (*embedded.PendingTransaction, error)

// An independent normal datastore observes writes made by the borrowed facade.
func adoptedHistoryAndSchemaContract(t *testing.T, p *embedded.Permissions, observer datastore.Datastore, stage stageRelationships) {
	ctx := t.Context()
	schema := `caveat tagged(value int) { value == 3 }
definition user {}
definition document {
 relation viewer: user with tagged
 permission view = viewer
}`
	_, err := p.WriteSchema(ctx, schema)
	require.NoError(t, err)
	rel := func(id string, value int) tuple.Relationship {
		return tuple.MustWithCaveat(tuple.MustParse("document:"+id+"#viewer@user:alice"), "tagged", map[string]any{"value": value})
	}
	initial, err := p.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel("historic", 1))})
	require.NoError(t, err)
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	changes, errs := observer.Watch(watchCtx, initial.Revision, datastore.WatchJustRelationships())
	pending, err := stage(ctx, func(ctx context.Context, r *embedded.RelationshipTransaction) error {
		for _, update := range []tuple.RelationshipUpdate{
			tuple.Touch(rel("historic", 2)), tuple.Touch(rel("historic", 3)),
			tuple.Create(rel("new", 1)), tuple.Delete(rel("new", 1)), tuple.Create(rel("new", 3)),
		} {
			if _, err := r.WriteRelationships(ctx, []tuple.RelationshipUpdate{update}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	defer func() {
		if err := pending.Rollback(context.Background()); err != nil && !errors.Is(err, embedded.ErrTransactionClosed) {
			t.Errorf("unable to roll back pending transaction during cleanup: %v", err)
		}
	}()
	committed, err := pending.Commit(ctx)
	require.NoError(t, err)
	rows, err := p.SnapshotReader(initial.Revision).ReadRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "document"})
	require.NoError(t, err)
	count := 0
	for relationship, err := range rows.Relationships {
		require.NoError(t, err)
		require.Equal(t, "historic", relationship.Resource.ObjectID)
		require.InDelta(t, float64(1), relationship.OptionalCaveat.Context.AsMap()["value"], 0.000001)
		count++
	}
	require.Equal(t, 1, count)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case change, ok := <-changes:
			require.True(t, ok, "watch closed")
			for _, update := range change.RelationshipChanges {
				require.Equal(t, tuple.UpdateOperationTouch, update.Operation)
				require.InDelta(t, float64(3), update.Relationship.OptionalCaveat.Context.AsMap()["value"], 0.000001)
				require.False(t, seen[update.Relationship.Resource.ObjectID], "intermediate mutation escaped")
				seen[update.Relationship.Resource.ObjectID] = true
			}
		case err := <-errs:
			require.NoError(t, err)
			t.Fatal("watch terminated")
		case <-timeout.C:
			t.Fatal("timed out awaiting final relationship changes")
		}
	}
	cancel()
	result, err := p.SnapshotReader(committed.Revision).Check(ctx, embedded.CheckRequest{ResourceType: "document", ResourceID: "historic", Permission: "view", SubjectType: "user", SubjectID: "alice"})
	require.NoError(t, err)
	require.True(t, result.HasPermission)

	// Warm the configured shared schema cache, then race removal of a relation
	// with a transaction validating and writing that relation. Neither engine
	// may commit both the incompatible schema and the relationship.
	_, err = p.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Delete(rel("historic", 3)), tuple.Delete(rel("new", 3))})
	require.NoError(t, err)
	_, err = p.WriteSchema(ctx, `definition user {}
definition race {
 relation viewer: user
 permission view = viewer
}`)
	require.NoError(t, err)
	_, err = p.Check(ctx, embedded.CheckRequest{ResourceType: "race", ResourceID: "doc", Permission: "view", SubjectType: "user", SubjectID: "alice"})
	require.NoError(t, err)
	pending, err = stage(ctx, func(ctx context.Context, r *embedded.RelationshipTransaction) error {
		_, err := r.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Create(tuple.MustParse("race:doc#viewer@user:alice"))})
		return err
	})
	require.NoError(t, err)
	defer func() {
		if err := pending.Rollback(context.Background()); err != nil && !errors.Is(err, embedded.ErrTransactionClosed) {
			t.Errorf("unable to roll back pending transaction during cleanup: %v", err)
		}
	}()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		_, err := p.WriteSchema(ctx, "definition user {}\ndefinition race {}")
		done <- err
	}()
	<-started
	// Give the concurrent schema operation a chance to encounter the open txn.
	select {
	case schemaErr := <-done:
		done <- schemaErr
	case <-time.After(100 * time.Millisecond):
	}
	_, commitErr := pending.Commit(ctx)
	if commitErr != nil {
		_ = pending.Rollback(context.Background())
	}
	select {
	case schemaErr := <-done:
		require.True(t, commitErr != nil || schemaErr != nil, "incompatible schema and relationship both committed")
	case <-time.After(20 * time.Second):
		t.Fatal("schema writer did not finish")
	}
}
