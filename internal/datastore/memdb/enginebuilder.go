package memdb

import (
	"context"
	"errors"

	log "github.com/authzed/spicedb/internal/logging"
	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
	"github.com/authzed/spicedb/pkg/datastore"
)

func init() {
	datastorecfg.RegisterEngine(Engine, newDatastoreFromConfig)
}

func newDatastoreFromConfig(_ context.Context, opts datastorecfg.Config) (datastore.Datastore, error) {
	if len(opts.ReadReplicaURIs) > 0 {
		return nil, errors.New("read replicas are not supported for the in-memory datastore engine")
	}

	log.Warn().Msg("in-memory datastore is not persistent and not feasible to run in a high availability fashion")
	return NewMemdbDatastore(opts.WatchBufferLength, opts.RevisionQuantization, opts.GCWindow)
}
