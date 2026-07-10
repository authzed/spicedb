package datastore

import (
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"

	crdbversion "github.com/authzed/spicedb/internal/datastore/crdb/version"
	pgversion "github.com/authzed/spicedb/internal/datastore/postgres/version"
	"github.com/authzed/spicedb/pkg/datastore"
)

// InitFunc initializes a datastore instance from a uri that has been
// generated from a TestDatastoreBuilder
type InitFunc func(engine, uri string) datastore.Datastore

// RunningEngineForTest represents an instance of a datastore engine running with its backing
// database/service, expressly for testing.
type RunningEngineForTest interface {
	// NewDatabase returns the connection string to a new initialized logical database,
	// but one that is *not migrated*.
	NewDatabase(t testing.TB) string

	// NewDatastore returns a new logical datastore initialized with the
	// initFunc. For example, the sql based stores will create a new logical
	// database in the database instance, migrate it and provide the URI for it
	// to initFunc
	NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore
}

// RunDatastoreEngine starts the backing database or service necessary for the given engine
// for testing and sets the defaults for that database or service. Note that this does *NOT*
// create the logical database nor call migrate; callers can do so via NewDatabase and NewDatastore
// respectively. Note also that the backing database or service will be shutdown automatically via
// the Cleanup of the testing object.
func RunDatastoreEngine(t testing.TB, engine string, opts ...testcontainers.ContainerCustomizer) RunningEngineForTest {
	switch engine {
	case "memory":
		return RunMemoryForTesting(t)
	case "cockroachdb":
		ver := os.Getenv("CRDB_TEST_VERSION")
		if ver == "" {
			ver = crdbversion.LatestTestedCockroachDBVersion
		}
		return RunCRDBForTesting(t, ver, opts...)
	case "postgres":
		ver := os.Getenv("POSTGRES_TEST_VERSION")
		if ver == "" {
			ver = pgversion.LatestTestedPostgresVersion
		}
		return RunPostgresForTesting(t, ver, false, opts...)
	case "mysql":
		return RunMySQLForTesting(t, opts...)
	case "spanner":
		return RunSpannerForTesting(t, opts...)
	default:
		t.Fatalf("found missing engine for RunDatastoreEngine: %s", engine)
		return nil
	}
}
