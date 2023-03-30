package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const createUniqueLivingNamespaceConstraint = `
	ALTER TABLE namespace_config
	ADD CONSTRAINT uq_namespace_living UNIQUE (namespace, deleted_transaction);
`

const deleteAllButNewestNamespace = `
	DELETE FROM namespace_config WHERE namespace IN (
		SELECT namespace FROM namespace_config WHERE deleted_transaction = 9223372036854775807 GROUP BY namespace HAVING COUNT(created_transaction) > 1
	) AND (namespace, created_transaction) NOT IN (
		SELECT namespace, max(created_transaction) from namespace_config where deleted_transaction = 9223372036854775807 GROUP BY namespace HAVING COUNT(created_transaction) > 1);`

func init() {
	if err := DatabaseMigrations.Register("add-unique-living-ns", "add-reverse-index", noNonatomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, deleteAllButNewestNamespace); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, createUniqueLivingNamespaceConstraint); err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
