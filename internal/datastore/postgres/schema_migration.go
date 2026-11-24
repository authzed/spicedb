package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

func (pgd *pgDatastore) ReadSchemaMigrationBoundary(ctx context.Context) (datastore.SchemaMigrationBoundary, error) {
	var source []byte
	var xid xid8
	err := pgd.readPool.QueryRow(ctx, `SELECT hash, created_xid FROM schema_revision
  WHERE name = $1 AND deleted_xid = '9223372036854775807'::xid8`, common.SchemaMigrationMarkerName).Scan(&source, &xid)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if string(source) == common.SchemaMigrationSourceUnified {
		return datastore.SchemaMigrationBoundaryFunc(func(datastore.Revision) bool { return true }), nil
	}
	if string(source) != common.SchemaMigrationSourceLegacy {
		return nil, fmt.Errorf("invalid schema migration source %q", source)
	}
	return datastore.SchemaMigrationBoundaryFunc(func(rev datastore.Revision) bool {
		r, ok := rev.(postgresRevision)
		return ok && r.snapshot.txVisible(xid.Uint64)
	}), nil
}
