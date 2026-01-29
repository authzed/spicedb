package crdb

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func setupTestDiskBuffer(t *testing.T) (*diskBufferedChangeProvider, *pebble.DB) {
	tempDir := t.TempDir()

	db, err := pebble.Open(tempDir, &pebble.Options{})
	require.NoError(t, err)

	sendChange := func(change datastore.RevisionChanges) error {
		return nil
	}

	sendError := func(err error) {}

	provider, err := newDiskBufferedChangeProvider(
		db,
		datastore.WatchRelationships|datastore.WatchSchema,
		sendChange,
		sendError,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	return provider, db
}

func TestDiskBufferAddRelationshipChanges(t *testing.T) {
	provider, _ := setupTestDiskBuffer(t)
	ctx := context.Background()

	rev1, err := revisions.NewForHLC(decimal.NewFromInt(1000))
	require.NoError(t, err)

	rel := tuple.MustParse("document:doc1#viewer@user:alice")

	err = provider.AddRelationshipChange(ctx, rev1, rel, tuple.UpdateOperationTouch)
	require.NoError(t, err)
}

func TestDiskBufferCheckpointEmission(t *testing.T) {
	provider, _ := setupTestDiskBuffer(t)
	ctx := context.Background()

	rev1, err := revisions.NewForHLC(decimal.NewFromInt(1000))
	require.NoError(t, err)
	rev2, err := revisions.NewForHLC(decimal.NewFromInt(2000))
	require.NoError(t, err)

	rel1 := tuple.MustParse("document:doc1#viewer@user:alice")
	rel2 := tuple.MustParse("document:doc2#editor@user:bob")

	// Add changes
	err = provider.AddRelationshipChange(ctx, rev1, rel1, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	err = provider.AddRelationshipChange(ctx, rev2, rel2, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	// First call sets the checkpoint
	changes, err := provider.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev2)
	require.NoError(t, err)
	require.Nil(t, changes)

	// Second call retrieves and emits accumulated changes
	changes, err = provider.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev2)
	require.NoError(t, err)
	require.Len(t, changes, 2)
	require.Len(t, changes[0].RelationshipChanges, 1)
	require.Len(t, changes[1].RelationshipChanges, 1)
}

func TestDiskBufferSchemaChanges(t *testing.T) {
	provider, _ := setupTestDiskBuffer(t)
	ctx := context.Background()

	rev1, err := revisions.NewForHLC(decimal.NewFromInt(1000))
	require.NoError(t, err)

	nsDef := &core.NamespaceDefinition{Name: "document"}

	err = provider.AddChangedDefinition(ctx, rev1, nsDef)
	require.NoError(t, err)

	// First call sets checkpoint
	changes, err := provider.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev1)
	require.NoError(t, err)
	require.Nil(t, changes)

	// Second call retrieves changes
	changes, err = provider.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev1)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Len(t, changes[0].ChangedDefinitions, 1)
	require.Equal(t, "document", changes[0].ChangedDefinitions[0].GetName())
}

func TestDiskBufferDeletedNamespace(t *testing.T) {
	provider, _ := setupTestDiskBuffer(t)
	ctx := context.Background()

	rev1, err := revisions.NewForHLC(decimal.NewFromInt(1000))
	require.NoError(t, err)

	err = provider.AddDeletedNamespace(ctx, rev1, "old_namespace")
	require.NoError(t, err)

	// First call sets checkpoint
	changes, err := provider.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev1)
	require.NoError(t, err)
	require.Nil(t, changes)

	// Second call retrieves changes
	changes, err = provider.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev1)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Contains(t, changes[0].DeletedNamespaces, "old_namespace")
}

func TestDiskBufferDeletedCaveat(t *testing.T) {
	provider, _ := setupTestDiskBuffer(t)
	ctx := context.Background()

	rev1, err := revisions.NewForHLC(decimal.NewFromInt(1000))
	require.NoError(t, err)

	err = provider.AddDeletedCaveat(ctx, rev1, "old_caveat")
	require.NoError(t, err)

	// First call sets checkpoint
	changes, err := provider.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev1)
	require.NoError(t, err)
	require.Nil(t, changes)

	// Second call retrieves changes
	changes, err = provider.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev1)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Contains(t, changes[0].DeletedCaveats, "old_caveat")
}

func TestDiskBufferRevisionMetadata(t *testing.T) {
	provider, _ := setupTestDiskBuffer(t)
	ctx := context.Background()

	rev1, err := revisions.NewForHLC(decimal.NewFromInt(1000))
	require.NoError(t, err)
	rev2, err := revisions.NewForHLC(decimal.NewFromInt(2000))
	require.NoError(t, err)

	// Add metadata
	metadata := map[string]any{
		"user_id": "test-user",
		"action":  "create",
	}
	err = provider.AddRevisionMetadata(ctx, rev1, metadata)
	require.NoError(t, err)

	// First call sets the checkpoint
	changes, err := provider.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev2)
	require.NoError(t, err)
	require.Nil(t, changes)

	// Second call retrieves and emits accumulated changes with metadata
	changes, err = provider.FilterAndRemoveRevisionChanges(revisions.HLCKeyLessThanFunc, rev2)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Len(t, changes[0].Metadatas, 1)
	require.Equal(t, "test-user", changes[0].Metadatas[0].Fields["user_id"].GetStringValue())
	require.Equal(t, "create", changes[0].Metadatas[0].Fields["action"].GetStringValue())
}
