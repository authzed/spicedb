package revisionparsing

import (
	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/mysql"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/spanner"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

// ParsingFunc defines a type representing parsing a revision string into a revision.
type ParsingFunc func(revisionStr string) (rev datastore.Revision, err error)

// ParseRevisionStringByDatastoreEngineID defines a map from datastore engine ID to its associated
// revision parsing function.
var ParseRevisionStringByDatastoreEngineID = map[string]ParsingFunc{
	memdb.Engine:    revision.ParseRevisionString,
	crdb.Engine:     revision.ParseRevisionString,
	postgres.Engine: postgres.ParseRevisionString,
	mysql.Engine:    revision.ParseRevisionString,
	spanner.Engine:  revision.ParseRevisionString,
}
