package crdb

import (
	"context"

	"github.com/authzed/spicedb/internal/datastore"
)

func (cds *crdbDatastore) Watch(ctx context.Context, afterRevision uint64) (<-chan *datastore.RevisionChanges, <-chan error) {
	return nil, nil
}
