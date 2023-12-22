package revisionparsing

import (
	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/mysql"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/datastore/spanner"
)

// ParseRevisionStringByDatastoreEngineID defines a map from datastore engine ID to its associated
// revision parsing function.
var ParseRevisionStringByDatastoreEngineID = map[string]revisions.ParsingFunc{
	memdb.Engine:    memdb.ParseRevisionString,
	crdb.Engine:     crdb.ParseRevisionString,
	postgres.Engine: postgres.ParseRevisionString,
	mysql.Engine:    mysql.ParseRevisionString,
	spanner.Engine:  spanner.ParseRevisionString,
}
