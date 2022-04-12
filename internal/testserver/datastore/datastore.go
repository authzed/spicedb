package datastore

import (
	"testing"

	"github.com/authzed/spicedb/internal/datastore"
)

// InitFunc initialize a datastore instance from a uri that has been
// generated from a TestDatastoreBuilder
type InitFunc func(engine, uri string) datastore.Datastore

// TestDatastoreBuilder stamps out new datastores from a backing service (like
// a container running a database).
type TestDatastoreBuilder interface {
	// NewDatastore returns a new logical datastore initialized with the
	// initFunc. For example, the sql based stores will create a new logical
	// database in the database instance and provide the URI for it to initFunc
	NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore
}

// TestDatastoreBuilderFunc allows a function to implement TestDatastoreBuilder
type TestDatastoreBuilderFunc func(t testing.TB, initFunc InitFunc) datastore.Datastore

// NewDatastore implements TestDatastoreBuilder
func (f TestDatastoreBuilderFunc) NewDatastore(t testing.TB, initFunc InitFunc) datastore.Datastore {
	return f(t, initFunc)
}

// NewTestDatastoreBuilder returns a TestDatastoreBuilder for a given engine
// this is primarily useful for tests against all datastores; most other tests
// can directly build the datastore they want
func NewTestDatastoreBuilder(t testing.TB, engine string) TestDatastoreBuilder {
	switch engine {
	case "memory":
		return TestDatastoreBuilderFunc(func(t testing.TB, initFunc InitFunc) datastore.Datastore {
			return initFunc("memory", "")
		})
	case "cockroachdb":
		return NewCRDBBuilder(t)
	case "postgres":
		return NewPostgresBuilder(t)
	case "spanner":
		return NewSpannerBuilder(t)
	}
	return nil
}
