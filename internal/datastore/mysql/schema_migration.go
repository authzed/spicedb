package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

func (mds *mysqlDatastore) ReadSchemaMigrationBoundary(ctx context.Context) (datastore.SchemaMigrationBoundary, error) {
	var source []byte
	var id uint64
	//nolint:gosec // The table name comes from validated internal schema configuration.
	query := fmt.Sprintf(`SELECT hash, created_transaction FROM %s WHERE name = ? AND deleted_transaction = 9223372036854775807`, mds.driver.SchemaRevision())
	err := mds.db.QueryRowContext(ctx, query, common.SchemaMigrationMarkerName).Scan(&source, &id)
	if errors.Is(err, sql.ErrNoRows) {
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
		r, ok := rev.(revisions.TransactionIDRevision)
		return ok && r.TransactionID() >= id
	}), nil
}
