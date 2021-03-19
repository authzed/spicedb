package test

import "github.com/authzed/spicedb/internal/datastore"

type DatastoreTester interface {
	// Creates a new datastore instance for a single test
	New() (datastore.Datastore, error)
}
