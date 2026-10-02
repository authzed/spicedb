//go:build datastore && crdb

package embedded_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	backend "github.com/authzed/spicedb/internal/datastore/crdb"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/embedded"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedCRDBCustomTable(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "cockroachdb")
	for _, mode := range []datalayer.SchemaMode{datalayer.SchemaModeReadLegacyWriteLegacy, datalayer.SchemaModeReadNewWriteNew} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			engine.NewDatastore(t, func(_, uri string) datastore.Datastore {
				ctx := t.Context()
				poolConfig, err := pgxpool.ParseConfig(uri)
				require.NoError(t, err)
				poolConfig.MaxConns = 1
				pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
				require.NoError(t, err)
				t.Cleanup(pool.Close)
				_, err = pool.Exec(ctx, "CREATE TABLE application_documents (id text PRIMARY KEY)")
				require.NoError(t, err)
				p, err := embedded.NewCRDBPermissions(ctx, pool, embedded.CRDBConfig{Permissions: embedded.Config{SchemaMode: mode}})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, p.Close()) })
				second, err := embedded.NewCRDBPermissions(ctx, pool, embedded.CRDBConfig{Permissions: embedded.Config{SchemaMode: mode}})
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

					defer tx.Rollback(context.Background())
					_, err = tx.Exec(ctx, "INSERT INTO application_documents VALUES ($1)", "doc")
					require.NoError(t, err)
					var escaped *embedded.RelationshipTransaction
					var escapedRows embedded.RelationshipIterator
					pending, err := p.WithCRDBTransaction(ctx, tx, func(ctx context.Context, rels *embedded.RelationshipTransaction) error {
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
					_, err = p.WithCRDBTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
						t.Fatal("duplicate binding callback")
						return nil
					})
					require.Error(t, err)

					_, err = second.WithCRDBTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error { return nil })
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
					require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM application_documents").Scan(&count))
					if commit {
						require.Equal(t, 1, count)
					} else {
						require.Zero(t, count)
					}
				}
				sentinel := errors.New("application callback failed")
				tx, err := p.BeginTransaction(ctx)
				require.NoError(t, err)

				defer tx.Rollback(context.Background())
				_, err = tx.Exec(ctx, "INSERT INTO application_documents VALUES ('failed')")
				require.NoError(t, err)
				pending, err := p.WithCRDBTransaction(ctx, tx, func(ctx context.Context, rels *embedded.RelationshipTransaction) error {
					_, err := rels.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:failed#viewer@user:alice"))})
					if err != nil {
						return err
					}
					return sentinel
				})
				require.ErrorIs(t, err, sentinel)
				require.Nil(t, pending)
				require.NoError(t, tx.Rollback(ctx))
				tx, err = p.BeginTransaction(ctx)
				require.NoError(t, err)

				defer tx.Rollback(context.Background())
				pending, err = p.WithCRDBTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error { return nil })
				require.NoError(t, err)
				require.NoError(t, tx.Rollback(ctx))
				failed, err := pending.Commit(ctx)
				require.Error(t, err)
				require.Equal(t, datastore.NoRevision, failed.Revision)
				checked, err := p.Check(ctx, embedded.CheckRequest{ResourceType: "document", ResourceID: "failed", Permission: "view", SubjectType: "user", SubjectID: "alice"})
				require.NoError(t, err)
				require.False(t, checked.HasPermission)

				for _, isolation := range []pgx.TxIsoLevel{pgx.ReadCommitted} {
					tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
					require.NoError(t, err)

					defer tx.Rollback(ctx)
					_, err = tx.Exec(ctx, "SELECT 1")
					require.NoError(t, err)
					_, err = p.WithCRDBTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
						t.Fatal("weak isolation callback invoked")
						return nil
					})
					require.ErrorIs(t, err, embedded.ErrTransactionIsolation)
					require.NoError(t, tx.Rollback(ctx))
				}
				tx, err = pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
				require.NoError(t, err)

				defer tx.Rollback(ctx)
				_, err = p.WithCRDBTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
					t.Fatal("read-only callback invoked")
					return nil
				})
				require.ErrorIs(t, err, embedded.ErrTransactionReadOnly)
				require.NoError(t, tx.Rollback(ctx))

				tx, err = p.BeginTransaction(ctx)
				require.NoError(t, err)

				defer tx.Rollback(ctx)
				_, err = tx.Exec(ctx, "UPDATE metadata SET unique_id='00000000-0000-0000-0000-000000000000'")
				require.NoError(t, err)
				_, err = p.WithCRDBTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
					t.Fatal("datastore identity mismatch callback invoked")
					return nil
				})
				require.ErrorIs(t, err, embedded.ErrTransactionIdentity)
				require.NoError(t, tx.Rollback(ctx))

				return nil
			})
		})
	}
}

func TestEmbeddedCRDBMigrateIfNeeded(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "cockroachdb")
	uri := engine.NewDatabase(t)
	ctx := t.Context()
	poolConfig, err := pgxpool.ParseConfig(uri)
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	require.NoError(t, err)
	defer pool.Close()

	cfg := embedded.CRDBConfig{}
	require.NoError(t, cfg.MigrateIfNeeded(ctx, pool))
	require.NoError(t, cfg.MigrateIfNeeded(ctx, pool))

	perms, err := embedded.NewCRDBPermissions(ctx, pool, cfg)
	require.NoError(t, err)
	require.NoError(t, perms.Close())
	require.NoError(t, pool.Ping(ctx))
}

func TestEmbeddedCRDBHistoryAndSchemaRaces(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "cockroachdb")
	for _, mode := range []datalayer.SchemaMode{datalayer.SchemaModeReadLegacyWriteLegacy, datalayer.SchemaModeReadNewWriteNew} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			engine.NewDatastore(t, func(_, uri string) datastore.Datastore {
				ctx := t.Context()
				pool, err := pgxpool.New(ctx, uri)
				require.NoError(t, err)
				t.Cleanup(pool.Close)
				p, err := embedded.NewCRDBPermissions(ctx, pool, embedded.CRDBConfig{Permissions: embedded.Config{SchemaMode: mode, SchemaCacheMaxCostBytes: 1 << 20}})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, p.Close()) })
				observer, err := backend.NewCRDBDatastore(ctx, uri)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, observer.Close()) })
				adoptedHistoryAndSchemaContract(t, p.Permissions, observer, func(ctx context.Context, fn func(context.Context, *embedded.RelationshipTransaction) error) (*embedded.PendingTransaction, error) {
					tx, err := p.BeginTransaction(ctx)
					if err != nil {
						return nil, err
					}
					pending, err := p.WithCRDBTransaction(ctx, tx, fn)
					if err != nil {
						_ = tx.Rollback(context.Background())
					}
					return pending, err
				})
				return nil
			})
		})
	}
}
