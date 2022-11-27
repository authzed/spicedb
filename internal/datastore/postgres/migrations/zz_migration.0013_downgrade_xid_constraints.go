package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

var downgradeStatements = []string{
	`ALTER TABLE relation_tuple_transaction
		DROP CONSTRAINT pk_rttx,
		ADD CONSTRAINT pk_rttx PRIMARY KEY USING INDEX ix_rttx_oldpk;`,
	`ALTER TABLE namespace_config
		DROP CONSTRAINT pk_namespace_config,
		DROP CONSTRAINT uq_namespace_living_xid,
		ADD CONSTRAINT namespace_config_pkey PRIMARY KEY USING INDEX ix_namespace_config_oldpk;`,
	`ALTER TABLE relation_tuple
		DROP CONSTRAINT pk_relation_tuple,
		DROP CONSTRAINT uq_relation_tuple_living_xid,
		ADD CONSTRAINT pk_relation_tuple PRIMARY KEY (id);`,
	`ALTER TABLE caveat
		DROP CONSTRAINT pk_caveat_v2,
		DROP CONSTRAINT uq_caveat_v2,
		ADD CONSTRAINT pk_caveat_v1 PRIMARY KEY (name, deleted_transaction);`,
	`ALTER TABLE relation_tuple_transaction ALTER COLUMN snapshot DROP NOT NULL;`,
	`ALTER TABLE relation_tuple ALTER COLUMN created_xid DROP NOT NULL;`,
	`ALTER TABLE namespace_config ALTER COLUMN created_xid DROP NOT NULL;`,
	`ALTER TABLE caveat ALTER COLUMN created_xid DROP NOT NULL;`,
}

func init() {
	if err := DatabaseMigrations.Register("add-xid-columns", "add-xid-constraints",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			for _, stmt := range downgradeStatements {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
