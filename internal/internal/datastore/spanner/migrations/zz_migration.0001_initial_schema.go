package migrations

import (
	"context"

	"cloud.google.com/go/spanner"
	"google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	createNamespaceConfig = `CREATE TABLE namespace_config (
		namespace STRING(1024),
		serialized_config BYTES(MAX) NOT NULL,
		timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
	) PRIMARY KEY (namespace)`

	createRelationTuple = `CREATE TABLE relation_tuple (
		namespace STRING(1024) NOT NULL,
		object_id STRING(1024) NOT NULL,
		relation STRING(1024) NOT NULL,
		userset_namespace STRING(1024) NOT NULL,
		userset_object_id STRING(1024) NOT NULL,
		userset_relation STRING(1024) NOT NULL,
		timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
	) PRIMARY KEY (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)`

	createSchemaVersion = `CREATE TABLE schema_version (
		version_num STRING(1024) NOT NULL
	) PRIMARY KEY (version_num)`

	// TODO see if we can make the operation smaller
	createChangelog = `CREATE TABLE changelog (
		timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
		uuid STRING(36) NOT NULL,
		operation INT64,
		namespace STRING(1024) NOT NULL,
		object_id STRING(1024) NOT NULL,
		relation STRING(1024) NOT NULL,
		userset_namespace STRING(1024) NOT NULL,
		userset_object_id STRING(1024) NOT NULL,
		userset_relation STRING(1024) NOT NULL,		
	) PRIMARY KEY (timestamp, uuid, operation, namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)`

	insertEmptyVersion = `INSERT INTO schema_version (version_num) VALUES ('')`

	createReverseQueryIndex = `CREATE INDEX ix_relation_tuple_by_subject ON relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation)`
	createReverseCheckIndex = `CREATE INDEX ix_relation_tuple_by_subject_relation ON relation_tuple (userset_namespace, userset_relation, namespace, relation)`
)

func init() {
	if err := SpannerMigrations.Register("initial", "", func(smd SpannerMigrationDriver) error {
		ctx := context.Background()

		updateOp, err := smd.adminClient.UpdateDatabaseDdl(ctx, &database.UpdateDatabaseDdlRequest{
			Database: smd.client.DatabaseName(),
			Statements: []string{
				createNamespaceConfig,
				createRelationTuple,
				createSchemaVersion,
				createChangelog,
				createReverseQueryIndex,
				createReverseCheckIndex,
			},
		})
		if err != nil {
			return err
		}

		if err := updateOp.Wait(ctx); err != nil {
			return err
		}

		_, err = smd.client.ReadWriteTransaction(ctx, func(c context.Context, rwt *spanner.ReadWriteTransaction) error {
			_, err := rwt.Update(c, spanner.NewStatement(insertEmptyVersion))
			return err
		})

		return err
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
