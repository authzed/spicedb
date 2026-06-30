//go:build datastoreconsistency

package integrationtesting_test

import (
	"testing"

	consistencytest "github.com/authzed/spicedb/pkg/consistency/test"
	"github.com/authzed/spicedb/pkg/datastore"
)

// TestConsistencyPerDatastore is a system-wide consistency test suite that reads in
// the various validation files in the testconfigs directory and executes the full set
// of APIs against the data within, ensuring that all results of the various APIs are
// consistent with one another. It runs the suite against every supported datastore
// engine, the local and caching dispatchers, and multiple dispatch chunk sizes.
//
// This test suite acts as essentially a full integration test for the API,
// dispatching, caching, computation and datastore layers. It should reflect
// both real-world schemas, as well as the full set of hand-constructed corner
// cases so that the system can be fully exercised.
func TestConsistencyPerDatastore(t *testing.T) {
	// TODO inmemory?

	for _, engineID := range datastore.Engines {
		t.Run(engineID, func(t *testing.T) {
			consistencytest.ConsistencyForEngine(t, engineID, nil)
		})
	}
}
