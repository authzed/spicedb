// Package engines registers, via side effect of import, every datastore
// engine defined in this repository with pkg/cmd/datastore's engine registry.
// Blank-import it from any binary or test that constructs datastores by engine
// name through pkg/cmd/datastore.NewDatastore.
package engines

import (
	_ "github.com/authzed/spicedb/internal/datastore/crdb"
	_ "github.com/authzed/spicedb/internal/datastore/memdb"
	_ "github.com/authzed/spicedb/internal/datastore/mysql"
	_ "github.com/authzed/spicedb/internal/datastore/postgres"
	_ "github.com/authzed/spicedb/internal/datastore/spanner"
)
