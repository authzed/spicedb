package migrations

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore/common"
	mysqlCommon "github.com/authzed/spicedb/internal/datastore/mysql/common"
	"github.com/authzed/spicedb/pkg/migrate"
)

func init() {
	mustRegisterMigration("refresh_schema_tables", "identifiers_case_sensitive", noNonatomicMigration, func(ctx context.Context, tx TxWrapper) error {
		source := common.SchemaMigrationSourceUnified
		if migrate.ShouldRefreshSchema(ctx) {
			if err := populateSchemaTables(ctx, tx, true); err != nil {
				return err
			}
			source = common.SchemaMigrationSourceLegacy
		}
		id, err := mysqlCommon.InsertNewTransaction(ctx, tx.tx, tx.tables.RelationTupleTransaction(), nil)
		if err != nil {
			return err
		}
		//nolint:gosec // The table name comes from validated internal schema configuration.
		query := fmt.Sprintf(`INSERT INTO %s (name, hash, created_transaction) VALUES (?, ?, ?)`, tx.tables.SchemaRevision())
		_, err = tx.tx.ExecContext(ctx, query, common.SchemaMigrationMarkerName, []byte(source), id)
		return err
	})
}
