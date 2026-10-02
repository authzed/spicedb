//go:build datastore && mysql

package embedded_test

import (
	"context"
	"database/sql"
	"testing"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/embedded"
	"github.com/authzed/spicedb/pkg/tuple"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestExampleMySQLTransaction(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "mysql")
	ctx := t.Context()
	db, err := sql.Open("mysql", engine.NewDatabase(t))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, err = db.ExecContext(ctx, "CREATE TABLE application_documents (id varchar(100) PRIMARY KEY)")
	require.NoError(t, err)

	cfg := embedded.MySQLConfig{}
	require.NoError(t, cfg.MigrateIfNeeded(ctx, db))

	perms, err := embedded.NewMySQLPermissions(ctx, db, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, perms.Close()) })

	_, err = perms.WriteSchema(ctx, `definition user {}
definition document {
 relation reader: user
 permission view = reader
}`)
	require.NoError(t, err)

	tx, err := perms.BeginTransaction(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "INSERT INTO application_documents (id) VALUES (?)", "doc1")
	require.NoError(t, err)
	pending, err := perms.WithMySQLTransaction(ctx, tx, func(ctx context.Context, relationships *embedded.RelationshipTransaction) error {
		_, err := relationships.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:doc1#reader@user:alice")),
		})
		return err
	})
	require.NoError(t, err)

	committed, err := pending.Commit(ctx)
	require.NoError(t, err)
	require.NotEqual(t, datastore.NoRevision, committed.Revision)

	var documentID string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT id FROM application_documents WHERE id = ?", "doc1").Scan(&documentID))
	require.Equal(t, "doc1", documentID)

	reader := perms.SnapshotReader(committed.Revision)
	checked, err := reader.Check(ctx, embedded.CheckRequest{
		ResourceType: "document", ResourceID: "doc1", Permission: "view",
		SubjectType: "user", SubjectID: "alice",
	})
	require.NoError(t, err)
	require.True(t, checked.HasPermission)

	schema, err := reader.ReadSchema(ctx)
	require.NoError(t, err)
	require.Contains(t, schema.SchemaText, "permission view = reader")

	read, err := reader.ReadRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "document"})
	require.NoError(t, err)
	var found bool
	for relationship, err := range read.Relationships {
		require.NoError(t, err)
		found = found || relationship.Resource.ObjectID == "doc1"
	}
	require.True(t, found)
}
