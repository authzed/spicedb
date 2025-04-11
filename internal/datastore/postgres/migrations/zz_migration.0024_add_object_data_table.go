package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const createObjectDataTable = `
CREATE TABLE object_data (
    od_type VARCHAR NOT NULL,
    od_id VARCHAR NOT NULL,
    od_data JSONB NOT NULL,
    od_created_xid xid8 DEFAULT pg_current_xact_id() NOT NULL,
    od_deleted_xid xid8 DEFAULT '9223372036854775807'::xid8 NOT NULL,
    CONSTRAINT pk_object_data PRIMARY KEY (od_type, od_id, od_created_xid, od_deleted_xid),
    CONSTRAINT uq_object_data_living UNIQUE (od_type, od_id, od_deleted_xid)
);

-- Index for looking up live objects (most common case)
CREATE INDEX ix_object_data_lookup ON object_data (od_type, od_id) 
    INCLUDE (od_data)
    WHERE od_deleted_xid = '9223372036854775807'::xid8;

-- Index for garbage collection of deleted records
CREATE INDEX ix_gc_object_data ON object_data USING btree (od_deleted_xid DESC) 
    WHERE od_deleted_xid < '9223372036854775807'::xid8;

-- Index for watching changes
CREATE INDEX ix_watch_object_data ON object_data USING btree (od_created_xid);

-- GIN index for JSONB querying
CREATE INDEX ix_object_data_jsonb ON object_data USING gin (od_data);`

func init() {
	if err := DatabaseMigrations.Register(
		"add-object-data-table",
		"add-index-for-transaction-gc", // previous migration name
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, createObjectDataTable); err != nil {
				return err
			}
			return nil
		},
	); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
