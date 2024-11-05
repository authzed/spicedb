package dsfortesting

import (
	"time"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datastore"
)

func NewMemDBDatastoreForTesting(
	watchBufferLength uint16,
	revisionQuantization,
	gcWindow time.Duration,
) (datastore.Datastore, error) {
	return memdb.NewMemdbDatastore(watchBufferLength, revisionQuantization, gcWindow)
}
