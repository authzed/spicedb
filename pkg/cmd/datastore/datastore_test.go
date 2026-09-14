package datastore

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestDefaults(t *testing.T) {
	f := pflag.FlagSet{}
	expected := NewConfigWithOptionsAndDefaults()
	err := RegisterDatastoreFlagsWithPrefix(&f, "", expected)
	require.NoError(t, err)
	received := DefaultDatastoreConfig()
	require.Equal(t, expected, received)
}

func TestLoadDatastoreFromFileContents(t *testing.T) {
	ctx := t.Context()
	ds, err := NewDatastore(ctx,
		SetBootstrapFileContents(map[string][]byte{"test": []byte("schema: definition user{}")}),
		WithEngine(MemoryEngine))
	require.NoError(t, err)
	t.Cleanup(func() {
		ds.Close()
	})

	revisionResult, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revisionResult.Revision).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Len(t, namespaces, 1)
	require.Equal(t, "user", namespaces[0].Definition.Name)
}

func TestLoadDatastoreFromFile(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "")
	require.NoError(t, err)
	_, err = file.Write([]byte("schema: definition user{}"))
	require.NoError(t, err)

	ctx := t.Context()
	ds, err := NewDatastore(ctx,
		SetBootstrapFiles([]string{file.Name()}),
		WithEngine(MemoryEngine))
	require.NoError(t, err)
	t.Cleanup(func() {
		ds.Close()
	})

	revisionResult, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revisionResult.Revision).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Len(t, namespaces, 1)
	require.Equal(t, "user", namespaces[0].Definition.Name)
}

// NOTE: this test captured a segfault in https://github.com/authzed/spicedb/issues/2783
func TestLoadDatastoreFromFileWithCaveats(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "")
	require.NoError(t, err)
	_, err = file.Write([]byte(`
schema: |-
  
  definition user {}
  
  caveat mfa_match_multi(acceptable_amr list<string>, provided_amr list<string>) {
     size(acceptable_amr) == 0 || (size(provided_amr) > 0 && acceptable_amr.exists(x, x in provided_amr))
  }

  definition organization {
    relation mfa_guard: organization with mfa_match_multi
    relation check: user:*
      
    permission secured_access = mfa_guard->check
  }
  
relationships: |-
  organization:orga#mfa_guard@organization:orga[mfa_match_multi:{"acceptable_amr": ["mfa"]}]`))
	require.NoError(t, err)

	ctx := t.Context()
	ds, err := NewDatastore(ctx,
		SetBootstrapFiles([]string{file.Name()}),
		WithEngine(MemoryEngine))
	require.NoError(t, err)
	t.Cleanup(func() {
		ds.Close()
	})

	revisionResult, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revisionResult.Revision).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Len(t, namespaces, 2)
	require.Equal(t, "organization", namespaces[0].Definition.Name)
}

func TestLoadDatastoreFromFileAndContents(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "")
	require.NoError(t, err)
	_, err = file.Write([]byte("schema: definition repository{}"))
	require.NoError(t, err)

	ctx := t.Context()
	ds, err := NewDatastore(ctx,
		SetBootstrapFiles([]string{file.Name()}),
		SetBootstrapFileContents(map[string][]byte{"test": []byte("schema: definition user{}")}),
		WithEngine(MemoryEngine))
	require.NoError(t, err)

	revisionResult, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	namespaces, err := ds.SnapshotReader(revisionResult.Revision).LegacyListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Len(t, namespaces, 2)
	namespaceNames := []string{namespaces[0].Definition.Name, namespaces[1].Definition.Name}
	require.Contains(t, namespaceNames, "user")
	require.Contains(t, namespaceNames, "repository")
}

// hangingOptimizedRevisionDatastore is a minimal datastore whose OptimizedRevision blocks until its context is cancelled.
type hangingOptimizedRevisionDatastore struct {
	datastore.Datastore
	sawDeadline atomic.Bool
}

func (h *hangingOptimizedRevisionDatastore) OptimizedRevision(ctx context.Context) (datastore.RevisionWithSchemaHashAndValidity, error) {
	if _, ok := ctx.Deadline(); ok {
		h.sawDeadline.Store(true)
	}
	<-ctx.Done()
	return datastore.RevisionWithSchemaHashAndValidity{}, ctx.Err()
}

// TestOptimizedRevisionTimeoutReachesSQLDatastore drives the real NewDatastore
// proxy stack against a fake engine wired the way the SQL engines are: the
// constructor wraps the concrete datastore in
// datastore.NewSeparatingContextDatastoreProxy. That proxy severs deadlines for
// most methods; if it did so for OptimizedRevision, the shared and fallback
// bounds imposed by the optimized-revision proxy above it would never reach
// the query and a hung datastore call would block every waiting caller
// forever.
//
// Using NewDatastore rather than stacking the proxies by hand keeps the test
// from diverging from production wiring as further layers are added.
func TestOptimizedRevisionTimeoutReachesSQLDatastore(t *testing.T) {
	const engineName = "test-hanging-optimized-revision"

	fake := &hangingOptimizedRevisionDatastore{}
	RegisterEngine(engineName, func(_ context.Context, _ Config) (datastore.Datastore, error) {
		// Mirror the SQL constructors (see NewCRDBDatastore, NewPostgresDatastore,
		// NewMySQLDatastore), which all return the concrete datastore wrapped in
		// the separating-context proxy.
		return datastore.NewSeparatingContextDatastoreProxy(fake), nil
	})
	t.Cleanup(func() { delete(BuilderForEngine, engineName) })

	synctest.Test(t, func(t *testing.T) {
		ds, err := NewDatastore(t.Context(), WithEngine(engineName))
		require.NoError(t, err)

		// The caller has no deadline of its own, so only the optimized-revision
		// proxy's default bounds can unblock the call. synctest's fake clock lets
		// them fire without waiting. If a layer strips them, every goroutine in
		// the bubble is durably blocked and synctest fails the test.
		_, err = ds.OptimizedRevision(t.Context())
		require.Error(t, err, "hung revision call must return an error rather than block forever")
		require.True(t, fake.sawDeadline.Load(), "the optimized-revision proxy's deadline must reach the concrete datastore")
	})
}
