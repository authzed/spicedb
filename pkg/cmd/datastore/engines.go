package datastore

// Every datastore engine defined in this repository is linked here so that
// NewDatastore can build any of them by name without callers having to import
// anything: importing this package is enough, exactly as it was before the
// engines began registering themselves.
//
// The builders themselves live in the engine packages and register into
// pkg/cmd/datastore/dsconfig from an init function, so an engine defined
// outside this repository becomes available the same way, by being linked into
// the binary. Only in-repo engines need a line here.
import (
	_ "github.com/authzed/spicedb/internal/datastore/crdb"
	_ "github.com/authzed/spicedb/internal/datastore/memdb"
	_ "github.com/authzed/spicedb/internal/datastore/mysql"
	_ "github.com/authzed/spicedb/internal/datastore/postgres"
	_ "github.com/authzed/spicedb/internal/datastore/spanner"
)
