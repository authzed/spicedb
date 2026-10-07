//go:build datastore && postgres

package embedded_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	backend "github.com/authzed/spicedb/internal/datastore/postgres"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/embedded"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestEmbeddedPostgresCustomTable(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "postgres")
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
				p, err := embedded.NewPostgresPermissions(ctx, pool, embedded.PostgresConfig{Permissions: embedded.Config{SchemaMode: mode}})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, p.Close()) })
				second, err := embedded.NewPostgresPermissions(ctx, pool, embedded.PostgresConfig{Permissions: embedded.Config{SchemaMode: mode}})
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
					pending, err := p.WithPostgresTransaction(ctx, tx, func(ctx context.Context, rels *embedded.RelationshipTransaction) error {
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
					_, err = p.WithPostgresTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
						t.Fatal("duplicate binding callback")
						return nil
					})
					require.Error(t, err)

					_, err = second.WithPostgresTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error { return nil })
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
				pending, err := p.WithPostgresTransaction(ctx, tx, func(ctx context.Context, rels *embedded.RelationshipTransaction) error {
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
				pending, err = p.WithPostgresTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error { return nil })
				require.NoError(t, err)
				require.NoError(t, tx.Rollback(ctx))
				failed, err := pending.Commit(ctx)
				require.Error(t, err)
				require.Equal(t, datastore.NoRevision, failed.Revision)
				checked, err := p.Check(ctx, embedded.CheckRequest{ResourceType: "document", ResourceID: "failed", Permission: "view", SubjectType: "user", SubjectID: "alice"})
				require.NoError(t, err)
				require.False(t, checked.HasPermission)

				for _, isolation := range []pgx.TxIsoLevel{pgx.ReadUncommitted, pgx.ReadCommitted, pgx.RepeatableRead} {
					tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: isolation})
					require.NoError(t, err)

					defer tx.Rollback(ctx)
					_, err = tx.Exec(ctx, "SELECT 1")
					require.NoError(t, err)
					_, err = p.WithPostgresTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
						t.Fatal("weak isolation callback invoked")
						return nil
					})
					require.ErrorIs(t, err, embedded.ErrTransactionIsolation)
					require.NoError(t, tx.Rollback(ctx))
				}
				tx, err = pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly})
				require.NoError(t, err)

				defer tx.Rollback(ctx)
				_, err = p.WithPostgresTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
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
				_, err = p.WithPostgresTransaction(ctx, tx, func(context.Context, *embedded.RelationshipTransaction) error {
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

func TestEmbeddedPostgresMigrateIfNeeded(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "postgres")
	uri := engine.NewDatabase(t)
	ctx := t.Context()
	poolConfig, err := pgxpool.ParseConfig(uri)
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	require.NoError(t, err)
	defer pool.Close()

	cfg := embedded.PostgresConfig{}
	require.NoError(t, cfg.MigrateIfNeeded(ctx, pool))
	require.NoError(t, cfg.MigrateIfNeeded(ctx, pool))

	perms, err := embedded.NewPostgresPermissions(ctx, pool, cfg)
	require.NoError(t, err)
	require.NoError(t, perms.Close())
	require.NoError(t, pool.Ping(ctx))
}

func TestEmbeddedPostgresCommittedVisibilityAndCommitFailure(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "postgres")
	engine.NewDatastore(t, func(_, uri string) datastore.Datastore {
		ctx := t.Context()
		pool, err := pgxpool.New(ctx, uri)
		require.NoError(t, err)
		t.Cleanup(pool.Close)
		_, err = pool.Exec(ctx, "CREATE TABLE application_unique (id text UNIQUE DEFERRABLE INITIALLY DEFERRED)")
		require.NoError(t, err)
		p, err := embedded.NewPostgresPermissions(ctx, pool, embedded.PostgresConfig{})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, p.Close()) })
		_, err = p.WriteSchema(ctx, `definition user {}
definition document {
 relation viewer: user
 permission view = viewer
}`)
		require.NoError(t, err)
		request := embedded.CheckRequest{ResourceType: "document", ResourceID: "pending", Permission: "view", SubjectType: "user", SubjectID: "alice"}
		for _, fail := range []bool{true, false} {
			tx, err := p.BeginTransaction(ctx)
			require.NoError(t, err)

			defer tx.Rollback(context.Background())
			_, err = tx.Exec(ctx, "INSERT INTO application_unique VALUES ('pending')")
			require.NoError(t, err)
			pending, err := p.WithPostgresTransaction(ctx, tx, func(ctx context.Context, r *embedded.RelationshipTransaction) error {
				_, err := r.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Create(tuple.MustParse("document:pending#viewer@user:alice"))})
				return err
			})
			require.NoError(t, err)
			before, err := p.Check(ctx, request)
			require.NoError(t, err)
			require.False(t, before.HasPermission)
			// Further application SQL is included in the same commit. A deferred
			// constraint lets the actual driver Commit fail after successful adoption.
			if fail {
				_, err = tx.Exec(ctx, "INSERT INTO application_unique VALUES ('pending')")
				require.NoError(t, err)
			}
			result, err := pending.Commit(ctx)
			if fail {
				require.Error(t, err)
				require.Equal(t, datastore.NoRevision, result.Revision)
			} else {
				require.NoError(t, err)
			}
			after, err := p.Check(ctx, request)
			require.NoError(t, err)
			require.Equal(t, !fail, after.HasPermission)
			var count int
			require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM application_unique").Scan(&count))
			if fail {
				require.Zero(t, count)
			} else {
				require.Equal(t, 1, count)
			}
		}
		return nil
	})
}

func TestEmbeddedPostgresCallerPoolCompatibility(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "postgres")
	for _, queryMode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec, pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol} {
		t.Run(queryMode.String(), func(t *testing.T) {
			engine.NewDatastore(t, func(_, uri string) datastore.Datastore {
				ctx := t.Context()
				cfg, err := pgxpool.ParseConfig(uri)
				require.NoError(t, err)
				cfg.MaxConns = 1
				cfg.ConnConfig.DefaultQueryExecMode = queryMode
				cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"
				cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
				var calls atomic.Int32
				cfg.AfterConnect = func(context.Context, *pgx.Conn) error { calls.Add(1); return nil }
				pool, err := pgxpool.NewWithConfig(ctx, cfg)
				require.NoError(t, err)
				defer pool.Close()
				p, err := embedded.NewPostgresPermissions(ctx, pool, embedded.PostgresConfig{})
				require.NoError(t, err)
				defer p.Close()
				_, err = p.WriteSchema(ctx, `definition user {}
definition document {
 relation viewer: user
 permission view = viewer
}`)
				require.NoError(t, err)
				tx, err := p.BeginTransaction(ctx)
				require.NoError(t, err)

				defer tx.Rollback(context.Background())
				var isolation, access string
				require.NoError(t, tx.QueryRow(ctx, "SHOW transaction_isolation").Scan(&isolation))
				require.NoError(t, tx.QueryRow(ctx, "SHOW transaction_read_only").Scan(&access))
				require.Equal(t, "serializable", isolation)
				require.Equal(t, "off", access)
				pending, err := p.WithPostgresTransaction(ctx, tx, func(ctx context.Context, r *embedded.RelationshipTransaction) error {
					_, err := r.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:doc#viewer@user:alice"))})
					return err
				})
				require.NoError(t, err)
				result, err := pending.Commit(ctx)
				require.NoError(t, err)
				checked, err := p.SnapshotReader(result.Revision).Check(ctx, embedded.CheckRequest{ResourceType: "document", ResourceID: "doc", Permission: "view", SubjectType: "user", SubjectID: "alice"})
				require.NoError(t, err)
				require.True(t, checked.HasPermission)
				require.Positive(t, calls.Load())
				require.NoError(t, p.Close())
				require.NoError(t, pool.Ping(ctx))
				return nil
			})
		})
	}
}

func TestEmbeddedPostgresHistoryAndSchemaRaces(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "postgres")
	for _, mode := range []datalayer.SchemaMode{datalayer.SchemaModeReadLegacyWriteLegacy, datalayer.SchemaModeReadNewWriteNew} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			engine.NewDatastore(t, func(_, uri string) datastore.Datastore {
				ctx := t.Context()
				pool, err := pgxpool.New(ctx, uri)
				require.NoError(t, err)
				t.Cleanup(pool.Close)
				p, err := embedded.NewPostgresPermissions(ctx, pool, embedded.PostgresConfig{Permissions: embedded.Config{SchemaMode: mode, SchemaCacheMaxCostBytes: 1 << 20}})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, p.Close()) })
				observer, err := backend.NewPostgresDatastore(ctx, uri)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, observer.Close()) })
				adoptedHistoryAndSchemaContract(t, p.Permissions, observer, func(ctx context.Context, fn func(context.Context, *embedded.RelationshipTransaction) error) (*embedded.PendingTransaction, error) {
					tx, err := p.BeginTransaction(ctx)
					if err != nil {
						return nil, err
					}
					pending, err := p.WithPostgresTransaction(ctx, tx, fn)
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
