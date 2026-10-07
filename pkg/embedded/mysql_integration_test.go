//go:build datastore && mysql

package embedded_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	backend "github.com/authzed/spicedb/internal/datastore/mysql"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/embedded"
	"github.com/authzed/spicedb/pkg/tuple"
	_ "github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/require"
)

func TestEmbeddedMySQLCustomTable(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "mysql")
	for _, mode := range []datalayer.SchemaMode{datalayer.SchemaModeReadLegacyWriteLegacy, datalayer.SchemaModeReadNewWriteNew} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			engine.NewDatastore(t, func(_, uri string) datastore.Datastore {
				ctx := t.Context()
				pool, err := sql.Open("mysql", uri)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, pool.Close()) })
				pool.SetMaxOpenConns(1)
				_, err = pool.ExecContext(ctx, "CREATE TABLE application_documents (id varchar(100) PRIMARY KEY)")
				require.NoError(t, err)
				p, err := embedded.NewMySQLPermissions(ctx, pool, embedded.MySQLConfig{Permissions: embedded.Config{SchemaMode: mode}})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, p.Close()) })
				second, err := embedded.NewMySQLPermissions(ctx, pool, embedded.MySQLConfig{Permissions: embedded.Config{SchemaMode: mode}})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, second.Close()) })
				_, err = p.WriteSchema(ctx, `definition user {}
definition document {
 relation viewer: user
 permission view = viewer
}`)
				require.NoError(t, err)
				original, err := p.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:historic#viewer@user:alice"))})
				require.NoError(t, err)
				historicRequest := embedded.CheckRequest{ResourceType: "document", ResourceID: "historic", Permission: "view", SubjectType: "user", SubjectID: "alice"}
				for _, commit := range []bool{false, true} {
					tx, err := p.BeginTransaction(ctx)
					require.NoError(t, err)

					defer tx.Rollback()
					_, err = tx.ExecContext(ctx, "INSERT INTO application_documents VALUES (?)", "doc")
					require.NoError(t, err)
					var escaped *embedded.RelationshipTransaction
					var escapedRows embedded.RelationshipIterator
					pending, err := p.WithMySQLTransaction(ctx, tx, func(ctx context.Context, rels *embedded.RelationshipTransaction) error {
						escaped = rels
						rel := tuple.MustParse("document:doc#viewer@user:alice")
						result, err := rels.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Create(rel)})
						if err != nil {
							return err
						}
						for _, update := range []tuple.RelationshipUpdate{tuple.Delete(rel), tuple.Create(rel), tuple.Delete(rel), tuple.Delete(rel), tuple.Touch(rel)} {
							if _, err := rels.WriteRelationships(ctx, []tuple.RelationshipUpdate{update}); err != nil {
								return err
							}
						}
						historic := tuple.MustParse("document:historic#viewer@user:alice")
						for _, update := range []tuple.RelationshipUpdate{tuple.Delete(historic), tuple.Touch(historic), tuple.Delete(historic), tuple.Touch(historic), tuple.Delete(historic)} {
							if _, err := rels.WriteRelationships(ctx, []tuple.RelationshipUpdate{update}); err != nil {
								return err
							}
						}
						read, err := rels.ReadRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "document"})
						if err != nil {
							return err
						}
						escapedRows = read.Relationships
						count := 0
						for _, err := range read.Relationships {
							if err != nil {
								return err
							}
							count++
						}
						require.Equal(t, 1, count)
						require.Equal(t, datastore.NoRevision, result.Revision)
						return err
					})
					require.NoError(t, err)
					_, err = escaped.ReadSchema(ctx)
					require.ErrorIs(t, err, embedded.ErrSessionClosed)
					for _, err := range escapedRows {
						require.ErrorIs(t, err, embedded.ErrSessionClosed)
					}
					_, err = p.WithMySQLTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
						t.Fatal("duplicate binding callback")
						return nil
					})
					require.Error(t, err)

					_, err = second.WithMySQLTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error { return nil })
					require.ErrorIs(t, err, embedded.ErrTransactionAlreadyBound)
					if commit {
						result, err := pending.Commit(ctx)
						require.NoError(t, err)
						require.NotEqual(t, datastore.NoRevision, result.Revision)
						checked, err := p.SnapshotReader(result.Revision).Check(ctx, embedded.CheckRequest{ResourceType: "document", ResourceID: "doc", Permission: "view", SubjectType: "user", SubjectID: "alice"})
						require.NoError(t, err)
						require.True(t, checked.HasPermission)
					} else {
						require.NoError(t, pending.Rollback(ctx))
					}
					historical, err := p.SnapshotReader(original.Revision).Check(ctx, historicRequest)
					require.NoError(t, err)
					require.True(t, historical.HasPermission)
					current, err := p.Check(ctx, historicRequest)
					require.NoError(t, err)
					require.Equal(t, !commit, current.HasPermission)
					var count int
					require.NoError(t, pool.QueryRowContext(ctx, "SELECT count(*) FROM application_documents").Scan(&count))
					if commit {
						require.Equal(t, 1, count)
					} else {
						require.Zero(t, count)
					}
				}
				sentinel := errors.New("application callback failed")
				tx, err := p.BeginTransaction(ctx)
				require.NoError(t, err)

				defer tx.Rollback()
				_, err = tx.ExecContext(ctx, "INSERT INTO application_documents VALUES ('failed')")
				require.NoError(t, err)
				pending, err := p.WithMySQLTransaction(ctx, tx, func(ctx context.Context, rels *embedded.RelationshipTransaction) error {
					_, err := rels.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:failed#viewer@user:alice"))})
					if err != nil {
						return err
					}
					return sentinel
				})
				require.ErrorIs(t, err, sentinel)
				require.Nil(t, pending)
				require.NoError(t, tx.Rollback())
				tx, err = p.BeginTransaction(ctx)
				require.NoError(t, err)

				defer tx.Rollback()
				pending, err = p.WithMySQLTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error { return nil })
				require.NoError(t, err)
				require.NoError(t, tx.Rollback())
				failed, err := pending.Commit(ctx)
				require.Error(t, err)
				require.Equal(t, datastore.NoRevision, failed.Revision)
				checked, err := p.Check(ctx, embedded.CheckRequest{ResourceType: "document", ResourceID: "failed", Permission: "view", SubjectType: "user", SubjectID: "alice"})
				require.NoError(t, err)
				require.False(t, checked.HasPermission)

				for _, isolation := range []sql.IsolationLevel{sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead} {
					tx, err := pool.BeginTx(ctx, &sql.TxOptions{Isolation: isolation})
					require.NoError(t, err)

					defer tx.Rollback()
					_, err = tx.ExecContext(ctx, "SELECT 1")
					require.NoError(t, err)
					_, err = p.WithMySQLTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
						t.Fatal("weak isolation callback invoked")
						return nil
					})
					require.ErrorIs(t, err, embedded.ErrTransactionIsolation)
					require.NoError(t, tx.Rollback())
				}
				tx, err = pool.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true})
				require.NoError(t, err)

				defer tx.Rollback()
				_, err = p.WithMySQLTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
					t.Fatal("read-only callback invoked")
					return nil
				})
				require.ErrorIs(t, err, embedded.ErrTransactionReadOnly)
				require.NoError(t, tx.Rollback())

				tx, err = p.BeginTransaction(ctx)
				require.NoError(t, err)

				defer tx.Rollback()
				_, err = tx.ExecContext(ctx, "UPDATE mysql_metadata SET unique_id='00000000-0000-0000-0000-000000000000'")
				require.NoError(t, err)
				_, err = p.WithMySQLTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
					t.Fatal("datastore identity mismatch callback invoked")
					return nil
				})
				require.ErrorIs(t, err, embedded.ErrTransactionIdentity)
				require.NoError(t, tx.Rollback())

				_, err = pool.ExecContext(ctx, "UPDATE performance_schema.setup_consumers SET ENABLED='NO' WHERE NAME='events_transactions_current'")
				require.NoError(t, err)
				defer pool.ExecContext(context.Background(), "UPDATE performance_schema.setup_consumers SET ENABLED='YES' WHERE NAME='events_transactions_current'")
				tx, err = p.BeginTransaction(ctx)
				require.NoError(t, err)

				defer tx.Rollback()
				_, err = p.WithMySQLTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
					t.Fatal("missing transaction instrumentation callback invoked")
					return nil
				})
				require.Error(t, err)
				require.NoError(t, tx.Rollback())

				return nil
			})
		})
	}
}

func TestEmbeddedMySQLMigrateIfNeeded(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "mysql")
	dsn := engine.NewDatabase(t)
	ctx := t.Context()
	pool, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer pool.Close()
	pool.SetMaxOpenConns(1)

	cfg := embedded.MySQLConfig{TablePrefix: "embed_"}
	require.NoError(t, cfg.MigrateIfNeeded(ctx, pool))
	require.NoError(t, cfg.MigrateIfNeeded(ctx, pool))

	perms, err := embedded.NewMySQLPermissions(ctx, pool, cfg)
	require.NoError(t, err)
	require.NoError(t, perms.Close())
	require.NoError(t, pool.PingContext(ctx))
}

func TestEmbeddedMySQLHistoryAndSchemaRaces(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "mysql")
	for _, mode := range []datalayer.SchemaMode{datalayer.SchemaModeReadLegacyWriteLegacy, datalayer.SchemaModeReadNewWriteNew} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			engine.NewDatastore(t, func(_, uri string) datastore.Datastore {
				ctx := t.Context()
				pool, err := sql.Open("mysql", uri)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, pool.Close()) })
				p, err := embedded.NewMySQLPermissions(ctx, pool, embedded.MySQLConfig{Permissions: embedded.Config{SchemaMode: mode, SchemaCacheMaxCostBytes: 1 << 20}})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, p.Close()) })
				observer, err := backend.NewMySQLDatastore(ctx, uri)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, observer.Close()) })
				adoptedHistoryAndSchemaContract(t, p.Permissions, observer, func(ctx context.Context, fn func(context.Context, *embedded.RelationshipTransaction) error) (*embedded.PendingTransaction, error) {
					tx, err := p.BeginTransaction(ctx)
					if err != nil {
						return nil, err
					}
					pending, err := p.WithMySQLTransaction(ctx, tx, fn)
					if err != nil {
						_ = tx.Rollback()
					}
					return pending, err
				})
				return nil
			})
		})
	}
}
