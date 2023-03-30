package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const (
	getNSConfigPkeyName = `
	SELECT constraint_name FROM information_schema.table_constraints
		WHERE table_schema = current_schema()
      	AND table_name = 'namespace_config'
      	AND constraint_type = 'PRIMARY KEY';`

	getNSConfigPkeyNameAlternate = `
	SELECT relname from pg_index, pg_class
		WHERE pg_index.indisprimary
		AND pg_index.indrelid = 'namespace_config'::regclass
		AND pg_class.oid = pg_index.indexrelid;`

	defaultNSConfigPkeyName = "namespace_config_pkey"

	dropNSConfigIDPkey = "ALTER TABLE namespace_config DROP CONSTRAINT %s;"
)

var addXIDConstraints = []string{
	`ALTER TABLE relation_tuple_transaction ALTER COLUMN snapshot SET NOT NULL;`,
	`ALTER TABLE relation_tuple ALTER COLUMN created_xid SET NOT NULL;`,
	`ALTER TABLE namespace_config ALTER COLUMN created_xid SET NOT NULL;`,
	`ALTER TABLE caveat ALTER COLUMN created_xid SET NOT NULL;`,
	`ALTER TABLE relation_tuple_transaction
		DROP CONSTRAINT pk_rttx,
		ADD CONSTRAINT pk_rttx PRIMARY KEY USING INDEX ix_rttx_pk;`,
	`ALTER TABLE namespace_config
		ADD CONSTRAINT pk_namespace_config PRIMARY KEY USING INDEX ix_namespace_config_pk,
		ADD CONSTRAINT uq_namespace_living_xid UNIQUE USING INDEX ix_namespace_config_living;`,
	`ALTER TABLE relation_tuple
		DROP CONSTRAINT pk_relation_tuple,
		ADD CONSTRAINT pk_relation_tuple PRIMARY KEY USING INDEX ix_relation_tuple_pk,
		ADD CONSTRAINT uq_relation_tuple_living_xid UNIQUE USING INDEX ix_relation_tuple_living;`,
	`ALTER TABLE caveat
		DROP CONSTRAINT pk_caveat_v1,
		ADD CONSTRAINT pk_caveat_v2 PRIMARY KEY USING INDEX ix_caveat_living,
		ADD CONSTRAINT uq_caveat_v2 UNIQUE USING INDEX ix_caveat_unique;`,
}

func init() {
	if err := DatabaseMigrations.Register("add-xid-constraints", "backfill-xid-add-indices",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			var constraintName string
			if err := tx.QueryRow(ctx, getNSConfigPkeyName).Scan(&constraintName); err != nil {
				// Try the backup query
				if err := tx.QueryRow(ctx, getNSConfigPkeyNameAlternate).Scan(&constraintName); err != nil {
					// Just use the default, if it's wrong we will fail to drop it below
					constraintName = defaultNSConfigPkeyName
				}
			}

			if _, err := tx.Exec(ctx, fmt.Sprintf(dropNSConfigIDPkey, constraintName)); err != nil {
				return err
			}

			for _, stmt := range addXIDConstraints {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
