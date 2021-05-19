package crdb

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"

	"github.com/authzed/spicedb/internal/datastore"
)

const (
	errUnableToInstantiate = "unable to instantiate datastore: %w"
)

func NewPostgresDatastore(url string) (datastore.Datastore, error) {
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		return nil, fmt.Errorf(errUnableToInstantiate, err)
	}

	return &crdbDatastore{conn: conn}, nil
}

type crdbDatastore struct {
	conn *pgx.Conn
}

func (cds *crdbDatastore) Revision(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (cds *crdbDatastore) SyncRevision(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (cds *crdbDatastore) CheckRevision(ctx context.Context, revision uint64) error {
	return nil
}
