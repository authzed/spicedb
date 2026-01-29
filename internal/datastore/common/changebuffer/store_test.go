package changebuffer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// mockRevision implements datastore.Revision for testing.
// This is a simple string-based revision for testing the changebuffer package
// without depending on specific datastore revision implementations.
type mockRevision struct {
	value string
}

func (m mockRevision) Equal(other datastore.Revision) bool {
	if otherMock, ok := other.(mockRevision); ok {
		return m.value == otherMock.value
	}
	return false
}

func (m mockRevision) GreaterThan(other datastore.Revision) bool {
	if otherMock, ok := other.(mockRevision); ok {
		return m.value > otherMock.value
	}
	return false
}

func (m mockRevision) LessThan(other datastore.Revision) bool {
	if otherMock, ok := other.(mockRevision); ok {
		return m.value < otherMock.value
	}
	return false
}

func (m mockRevision) String() string {
	return m.value
}

func (m mockRevision) ByteSortable() bool {
	return true
}

func mockParsingFunc(revisionStr string) (datastore.Revision, error) {
	return mockRevision{value: revisionStr}, nil
}

func setupTestStore(t *testing.T) (*Store, *pebble.DB, string) {
	// TODO(calude): t.TempDir
	tempDir := filepath.Join(os.TempDir(), "spicedb-test-"+uuid.New().String())

	db, err := pebble.Open(tempDir, &pebble.Options{})
	require.NoError(t, err)

	store, err := New(mockParsingFunc, db)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, db.Close())
		_ = os.RemoveAll(tempDir)
	})

	return store, db, tempDir
}

func TestStoreAddAndIterateRelationshipChanges(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev1 := mockRevision{value: "rev1"}
	rev2 := mockRevision{value: "rev2"}

	// Add relationship changes
	rel1 := tuple.MustParse("document:doc1#viewer@user:alice")
	rel2 := tuple.MustParse("document:doc2#editor@user:bob")

	err := store.AddRelationshipChange(ctx, rev1, rel1, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	err = store.AddRelationshipChange(ctx, rev2, rel2, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	// Iterate up to rev2
	//nolint:prealloc // size unknown when iterating over sequence
	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev2) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Len(t, changes, 2)
	require.True(t, changes[0].Revision.Equal(rev1))
	require.True(t, changes[1].Revision.Equal(rev2))
	require.Len(t, changes[0].RelationshipChanges, 1)
	require.Len(t, changes[1].RelationshipChanges, 1)
	require.Equal(t, tuple.UpdateOperationTouch, changes[0].RelationshipChanges[0].Operation)
}

func TestStoreRelationshipDeduplication(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev1 := mockRevision{value: "rev1"}
	rel := tuple.MustParse("document:doc1#viewer@user:alice")

	// Add TOUCH, then DELETE - TOUCH should win (following SpiceDB semantics)
	err := store.AddRelationshipChange(ctx, rev1, rel, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	err = store.AddRelationshipChange(ctx, rev1, rel, tuple.UpdateOperationDelete)
	require.NoError(t, err)

	// Iterate and verify only TOUCH is present (TOUCH takes precedence)
	//nolint:prealloc // size unknown when iterating over sequence
	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev1) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Len(t, changes, 1)
	require.Len(t, changes[0].RelationshipChanges, 1)
	require.Equal(t, tuple.UpdateOperationTouch, changes[0].RelationshipChanges[0].Operation)
}

func TestStoreAddAndIterateNamespaceChanges(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev1 := mockRevision{value: "rev1"}
	rev2 := mockRevision{value: "rev2"}

	// Add namespace changes
	nsDef1 := &core.NamespaceDefinition{Name: "document"}
	nsDef2 := &core.NamespaceDefinition{Name: "user"}

	err := store.AddChangedDefinition(ctx, rev1, nsDef1)
	require.NoError(t, err)

	err = store.AddChangedDefinition(ctx, rev2, nsDef2)
	require.NoError(t, err)

	// Iterate up to rev2
	//nolint:prealloc // size unknown when iterating over sequence
	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev2) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Len(t, changes, 2)
	require.Len(t, changes[0].ChangedDefinitions, 1)
	require.Equal(t, "document", changes[0].ChangedDefinitions[0].GetName())
	require.Len(t, changes[1].ChangedDefinitions, 1)
	require.Equal(t, "user", changes[1].ChangedDefinitions[0].GetName())
}

func TestStoreAddAndIterateCaveatChanges(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev1 := mockRevision{value: "rev1"}

	// Add caveat change
	caveatDef := &core.CaveatDefinition{Name: "is_admin"}

	err := store.AddChangedDefinition(ctx, rev1, caveatDef)
	require.NoError(t, err)

	// Iterate and verify
	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev1) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Len(t, changes, 1)
	require.Len(t, changes[0].ChangedDefinitions, 1)
	require.Equal(t, "is_admin", changes[0].ChangedDefinitions[0].GetName())
}

func TestStoreDeletedNamespace(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev1 := mockRevision{value: "rev1"}

	err := store.AddDeletedNamespace(ctx, rev1, "old_namespace")
	require.NoError(t, err)

	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev1) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Len(t, changes, 1)
	require.Contains(t, changes[0].DeletedNamespaces, "old_namespace")
}

func TestStoreDeletedCaveat(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev1 := mockRevision{value: "rev1"}

	err := store.AddDeletedCaveat(ctx, rev1, "old_caveat")
	require.NoError(t, err)

	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev1) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Len(t, changes, 1)
	require.Contains(t, changes[0].DeletedCaveats, "old_caveat")
}

func TestStoreDeleteRevisionsRange(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev1 := mockRevision{value: "rev1"}
	rev2 := mockRevision{value: "rev2"}
	rev3 := mockRevision{value: "rev3"}

	// Add changes for all three revisions
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	err := store.AddRelationshipChange(ctx, rev1, rel, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	err = store.AddRelationshipChange(ctx, rev2, rel, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	err = store.AddRelationshipChange(ctx, rev3, rel, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	// Delete rev1 and rev2
	err = store.DeleteRevisionsRange(ctx, rev1, rev2)
	require.NoError(t, err)

	// Verify only rev3 remains
	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev3) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Len(t, changes, 1)
	require.True(t, changes[0].Revision.Equal(rev3))
}

func TestStoreMultipleChangeTypes(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev := mockRevision{value: "rev1"}

	// Add all types of changes in the same revision
	rel := tuple.MustParse("document:doc1#viewer@user:alice")
	err := store.AddRelationshipChange(ctx, rev, rel, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	nsDef := &core.NamespaceDefinition{Name: "document"}
	err = store.AddChangedDefinition(ctx, rev, nsDef)
	require.NoError(t, err)

	caveatDef := &core.CaveatDefinition{Name: "is_admin"}
	err = store.AddChangedDefinition(ctx, rev, caveatDef)
	require.NoError(t, err)

	// Iterate and verify all changes are present
	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Len(t, changes, 1)
	require.Len(t, changes[0].RelationshipChanges, 1)
	require.Len(t, changes[0].ChangedDefinitions, 2)
}

func TestStoreEmptyIteration(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev := mockRevision{value: "rev1"}

	// Iterate with no changes
	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Empty(t, changes)
}

func TestStorePersistence(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "spicedb-test-"+uuid.New().String())

	rev1 := mockRevision{value: "rev1"}
	rel := tuple.MustParse("document:doc1#viewer@user:alice")

	// Scoping ensures database is properly closed before reopening to test persistence

	// Create store and add data
	{
		db, err := pebble.Open(tempDir, &pebble.Options{})
		require.NoError(t, err)

		store, err := New(mockParsingFunc, db)
		require.NoError(t, err)

		ctx := context.Background()
		err = store.AddRelationshipChange(ctx, rev1, rel, tuple.UpdateOperationTouch)
		require.NoError(t, err)

		require.NoError(t, db.Close())
	}

	// Reopen and verify data persisted
	{
		db, err := pebble.Open(tempDir, &pebble.Options{})
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db.Close())
			_ = os.RemoveAll(tempDir)
		}()

		store, err := New(mockParsingFunc, db)
		require.NoError(t, err)

		ctx := context.Background()
		var changes []RevisionChanges
		for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev1) {
			require.NoError(t, err)
			changes = append(changes, ch)
		}

		require.Len(t, changes, 1)
		require.Len(t, changes[0].RelationshipChanges, 1)
	}
}

func TestStoreIterationOrder(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	// Add revisions out of order
	rev1 := mockRevision{value: "a"}
	rev2 := mockRevision{value: "c"}
	rev3 := mockRevision{value: "b"}

	rel := tuple.MustParse("document:doc1#viewer@user:alice")

	var err error
	err = store.AddRelationshipChange(ctx, rev2, rel, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	err = store.AddRelationshipChange(ctx, rev1, rel, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	err = store.AddRelationshipChange(ctx, rev3, rel, tuple.UpdateOperationTouch)
	require.NoError(t, err)

	// Iterate up to rev2 (which is "c") and verify all revisions are returned in order
	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev2) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	// Should get all 3 revisions in lexicographic order: a, b, c
	require.Len(t, changes, 3)
	require.Equal(t, "a", changes[0].Revision.String())
	require.Equal(t, "b", changes[1].Revision.String())
	require.Equal(t, "c", changes[2].Revision.String())
}

func TestStoreNamespaceDeduplication(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev := mockRevision{value: "rev1"}

	// Add namespace change then deletion - change should win
	nsDef := &core.NamespaceDefinition{Name: "document"}
	var err error
	err = store.AddChangedDefinition(ctx, rev, nsDef)
	require.NoError(t, err)

	err = store.AddDeletedNamespace(ctx, rev, "document")
	require.NoError(t, err)

	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Len(t, changes, 1)
	require.Len(t, changes[0].ChangedDefinitions, 1)
	require.Equal(t, "document", changes[0].ChangedDefinitions[0].GetName())
	require.Empty(t, changes[0].DeletedNamespaces)
}

func TestStoreCaveatDeduplication(t *testing.T) {
	store, _, _ := setupTestStore(t)
	ctx := context.Background()

	rev := mockRevision{value: "rev1"}

	// Add caveat change then deletion - change should win
	caveatDef := &core.CaveatDefinition{Name: "is_admin"}
	var err error
	err = store.AddChangedDefinition(ctx, rev, caveatDef)
	require.NoError(t, err)

	err = store.AddDeletedCaveat(ctx, rev, "is_admin")
	require.NoError(t, err)

	var changes []RevisionChanges
	for ch, err := range store.IterateRevisionChangesUpToBoundary(ctx, rev) {
		require.NoError(t, err)
		changes = append(changes, ch)
	}

	require.Len(t, changes, 1)
	require.Len(t, changes[0].ChangedDefinitions, 1)
	require.Equal(t, "is_admin", changes[0].ChangedDefinitions[0].GetName())
	require.Empty(t, changes[0].DeletedCaveats)
}
