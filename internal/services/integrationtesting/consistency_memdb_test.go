//go:build integration

package integrationtesting_test

import (
	"testing"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	consistencytest "github.com/authzed/spicedb/pkg/consistency/test"
	"github.com/authzed/spicedb/pkg/datastore"
	dstest "github.com/authzed/spicedb/pkg/datastore/test"
)

// TestConsistencyMemDB runs the consistency suite against memdb.
// It needs no Docker, so it runs in the normal integration test job.
func TestConsistencyMemDB(t *testing.T) {
	consistencytest.AllConsistency(t, dstest.DatastoreTesterFunc(
		func(tb testing.TB, _ dstest.RevisionParameters, watchBufferLength uint16) (datastore.Datastore, error) {
			return dsfortesting.NewMemDBDatastoreForTesting(tb, watchBufferLength, testTimedelta, memdb.DisableGC)
		}))
}
