//go:build datastore && postgres

package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

type testPGSchemaCache struct {
	mu    sync.Mutex
	items map[datalayer.SchemaCacheKey]*datastore.ReadOnlyStoredSchema
}

func newTestPGSchemaCache() *testPGSchemaCache {
	return &testPGSchemaCache{
		items: make(map[datalayer.SchemaCacheKey]*datastore.ReadOnlyStoredSchema),
	}
}

func (c *testPGSchemaCache) Get(key datalayer.SchemaCacheKey) (*datastore.ReadOnlyStoredSchema, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.items[key]
	return v, ok
}

func (c *testPGSchemaCache) Set(key datalayer.SchemaCacheKey, entry *datastore.ReadOnlyStoredSchema, _ int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = entry
	return true
}

func buildStoredSchemaForTest(t *testing.T, schemaText string) (*core.StoredSchema, string) {
	t.Helper()

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	defs := make([]compiler.SchemaDefinition, 0, len(compiled.ObjectDefinitions)+len(compiled.CaveatDefinitions))
	nsMap := make(map[string]*core.NamespaceDefinition, len(compiled.ObjectDefinitions))
	for _, od := range compiled.ObjectDefinitions {
		defs = append(defs, od)
		nsMap[od.Name] = od
	}
	cavMap := make(map[string]*core.CaveatDefinition, len(compiled.CaveatDefinitions))
	for _, cd := range compiled.CaveatDefinitions {
		defs = append(defs, cd)
		cavMap[cd.Name] = cd
	}

	hash, err := generator.ComputeSchemaHash(defs)
	require.NoError(t, err)

	return &core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText:           schemaText,
				SchemaHash:           hash,
				NamespaceDefinitions: nsMap,
				CaveatDefinitions:    cavMap,
			},
		},
	}, hash
}

func queryDBNowUTC(ctx context.Context, t *testing.T, conn *pgx.Conn) time.Time {
	t.Helper()
	var now time.Time
	require.NoError(t, conn.QueryRow(ctx, "SELECT (NOW() AT TIME ZONE 'utc')").Scan(&now))
	return now
}

func waitForNextBucket(ctx context.Context, t *testing.T, conn *pgx.Conn, quantization time.Duration, prevBucket int64) time.Time {
	t.Helper()
	quantNs := quantization.Nanoseconds()
	for {
		now := queryDBNowUTC(ctx, t, conn)
		if quantizationBucket(now, quantization, 0) > prevBucket {
			return now
		}
		rem := quantNs - (now.UnixNano() % quantNs)
		time.Sleep(time.Duration(rem) + quantization/10)
	}
}

// TestConcurrentSchemaWriteOptimizedRevisionSchemaHash reproduces
// https://github.com/authzed/spicedb/issues/3535: under concurrent transactions on a
// single Postgres primary, OptimizedRevision's schema hash lookup must use snapshot
// visibility (matching ReadStoredSchema's aliveFilter) rather than scalar xid8 comparisons.
func TestConcurrentSchemaWriteOptimizedRevisionSchemaHash(t *testing.T) {
	b := testdatastore.RunPostgresForTesting(t, postgresConfig.pgVersion, postgresConfig.pgbouncer)

	const (
		schemaOldText = `definition user {}`
		schemaNewText = `definition user {}

definition document {
	relation viewer: user
}`
	)
	storedOld, hashOld := buildStoredSchemaForTest(t, schemaOldText)
	storedNew, hashNew := buildStoredSchemaForTest(t, schemaNewText)
	require.NotEqual(t, hashOld, hashNew)

	newDS := func(t *testing.T, quantization time.Duration) (*pgDatastore, *pgx.Conn) {
		t.Helper()
		ctx := t.Context()
		var conn *pgx.Conn
		var pgDS *pgDatastore
		b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			var err error
			conn, err = pgx.Connect(ctx, uri)
			require.NoError(t, err)
			RegisterTypes(conn.TypeMap())
			t.Cleanup(func() { _ = conn.Close(context.Background()) })

			rawDS, err := newPostgresDatastore(
				ctx,
				uri,
				primaryInstanceID,
				RevisionQuantization(quantization),
				GCWindow(24*time.Hour),
				GCInterval(disableBackgroundGC),
				WatchBufferLength(1),
				WithRevisionHeartbeat(false),
			)
			require.NoError(t, err)
			pgDS = rawDS.(*pgDatastore)
			return rawDS
		})
		return pgDS, conn
	}

	// Forward case (#3535): T_schema (lower xid) writes S_new while uncommitted;
	// T_other (higher xid, in the next quantization bucket) captures a snapshot with
	// T_schema in xipList and commits; T_schema then commits.
	// OptimizedRevision selects T_other, at which ReadStoredSchema sees S_old, so
	// OptimizedRevision must return hashOld (not hashNew) and never poison schemaHashCache.
	t.Run("concurrent schema write invisible in selected snapshot", func(t *testing.T) {
		const (
			quantization = 2 * time.Second
			maxAttempts  = 4
		)

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			mustAssert := attempt == maxAttempts
			ctx := t.Context()
			ds, conn := newDS(t, quantization)

			// Write initial schema S_old.
			_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteStoredSchema(ctx, storedOld)
			})
			require.NoError(t, err)

			// Start T_schema in the current quantization bucket and hold it open before commit.
			schemaWritten := make(chan struct{})
			allowCommit := make(chan struct{})
			var releaseOnce sync.Once
			releaseSchema := func() { releaseOnce.Do(func() { close(allowCommit) }) }
			defer releaseSchema()
			type txResult struct {
				rev datastore.Revision
				err error
			}
			schemaDone := make(chan txResult, 1)
			go func() {
				rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					if err := rwt.WriteStoredSchema(ctx, storedNew); err != nil {
						return err
					}
					close(schemaWritten)
					<-allowCommit
					return nil
				}, options.WithDisableRetries(true))
				schemaDone <- txResult{rev: rev, err: err}
			}()

			select {
			case <-schemaWritten:
			case res := <-schemaDone:
				require.Failf(t, "T_schema exited before writing schema", "err=%v", res.err)
			}
			bucketAtSchema := quantizationBucket(queryDBNowUTC(ctx, t, conn), quantization, 0)

			// Cross into the next quantization bucket so T_other is the first row in the new bucket.
			startNewBucket := waitForNextBucket(ctx, t, conn, quantization, bucketAtSchema)
			bucketAtOther := quantizationBucket(startNewBucket, quantization, 0)

			// Run T_other (e.g., heartbeat or unrelated write) while T_schema is still in progress.
			otherRevRaw, err := ds.ReadWriteTx(ctx, func(_ context.Context, _ datastore.ReadWriteTransaction) error {
				return nil
			})
			require.NoError(t, err)
			otherRev := otherRevRaw.(postgresRevision)

			// Let T_schema commit now that T_other's snapshot captured T_schema in xipList.
			releaseSchema()
			schemaRes := <-schemaDone
			require.NoError(t, schemaRes.err)
			schemaRev := schemaRes.rev.(postgresRevision)

			// Query OptimizedRevision through DataLayer (which delegates directly to
			// ds.OptimizedRevision) while still inside bucketAtOther.
			cache := newTestPGSchemaCache()
			dl := datalayer.NewDataLayer(ds, datalayer.WithSchemaMode(datalayer.SchemaModeReadNewWriteNew), datalayer.WithSchemaCache(cache))

			dlOptRev, dlOptHash, err := dl.OptimizedRevision(ctx)
			require.NoError(t, err)
			endNow := queryDBNowUTC(ctx, t, conn)

			if quantizationBucket(endNow, quantization, 0) != bucketAtOther {
				if mustAssert {
					require.Failf(t, "quantization boundary crossed", "bucket moved from %d to %d", bucketAtOther, quantizationBucket(endNow, quantization, 0))
				}
				continue
			}

			optPG := dlOptRev.(postgresRevision)
			require.True(t, optPG.optionalTxID.Valid, "expected selected transaction xid")
			require.Equal(t, otherRev.optionalTxID.Uint64, optPG.optionalTxID.Uint64, "OptimizedRevision should select T_other")
			require.Less(t, schemaRev.optionalTxID.Uint64, optPG.optionalTxID.Uint64, "T_schema xid must be lower than T_other xid")
			require.False(t, optPG.snapshot.txVisible(schemaRev.optionalTxID.Uint64), "T_schema must be invisible in T_other's snapshot")

			storedAtOpt, err := ds.SnapshotReader(dlOptRev).ReadStoredSchema(ctx)
			require.NoError(t, err)
			require.Equal(t, hashOld, storedAtOpt.Get().GetV1().SchemaHash, "ReadStoredSchema at T_other must return S_old")
			require.Equal(t, datalayer.SchemaHash(hashOld), dlOptHash, "OptimizedRevision must return hash matching ReadStoredSchema (S_old), not S_new")

			// Verify end-to-end through DataLayer with schema cache using the already-verified
			// revision and hash: reading returns S_old and never poisons hashNew for HeadRevision.
			srOpt, err := dl.SnapshotReader(dlOptRev, dlOptHash).ReadSchema(ctx)
			require.NoError(t, err)
			_, hasDocAtOpt, err := srOpt.LookupTypeDefByName(ctx, "document")
			require.NoError(t, err)
			require.False(t, hasDocAtOpt, "document definition should not exist at T_other")

			dlHeadRev, dlHeadHash, err := dl.HeadRevision(ctx)
			require.NoError(t, err)
			require.Equal(t, datalayer.SchemaHash(hashNew), dlHeadHash)

			srHead, err := dl.SnapshotReader(dlHeadRev, dlHeadHash).ReadSchema(ctx)
			require.NoError(t, err)
			_, hasDocAtHead, err := srHead.LookupTypeDefByName(ctx, "document")
			require.NoError(t, err)
			require.True(t, hasDocAtHead, "HeadRevision read for hashNew must return S_new (cache must not be poisoned with S_old)")
			return
		}
	})

	// Same forward interleaving when T_schema is the very first schema write (no prior schema).
	// ReadStoredSchema returns ErrSchemaNotFound at T_other, so OptimizedRevision must return
	// an empty schema hash rather than hashNew.
	t.Run("concurrent first schema write invisible in selected snapshot", func(t *testing.T) {
		const (
			quantization = 2 * time.Second
			maxAttempts  = 4
		)

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			mustAssert := attempt == maxAttempts
			ctx := t.Context()
			ds, conn := newDS(t, quantization)

			schemaWritten := make(chan struct{})
			allowCommit := make(chan struct{})
			var releaseOnce sync.Once
			releaseSchema := func() { releaseOnce.Do(func() { close(allowCommit) }) }
			defer releaseSchema()
			type txResult struct {
				rev datastore.Revision
				err error
			}
			schemaDone := make(chan txResult, 1)
			go func() {
				rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					if err := rwt.WriteStoredSchema(ctx, storedNew); err != nil {
						return err
					}
					close(schemaWritten)
					<-allowCommit
					return nil
				}, options.WithDisableRetries(true))
				schemaDone <- txResult{rev: rev, err: err}
			}()

			select {
			case <-schemaWritten:
			case res := <-schemaDone:
				require.Failf(t, "T_schema exited before writing schema", "err=%v", res.err)
			}
			bucketAtSchema := quantizationBucket(queryDBNowUTC(ctx, t, conn), quantization, 0)
			startNewBucket := waitForNextBucket(ctx, t, conn, quantization, bucketAtSchema)
			bucketAtOther := quantizationBucket(startNewBucket, quantization, 0)

			otherRevRaw, err := ds.ReadWriteTx(ctx, func(_ context.Context, _ datastore.ReadWriteTransaction) error {
				return nil
			})
			require.NoError(t, err)
			otherRev := otherRevRaw.(postgresRevision)

			releaseSchema()
			schemaRes := <-schemaDone
			require.NoError(t, schemaRes.err)
			schemaRev := schemaRes.rev.(postgresRevision)

			opt, err := ds.OptimizedRevision(ctx)
			require.NoError(t, err)
			endNow := queryDBNowUTC(ctx, t, conn)

			if quantizationBucket(endNow, quantization, 0) != bucketAtOther {
				if mustAssert {
					require.Failf(t, "quantization boundary crossed", "bucket moved from %d to %d", bucketAtOther, quantizationBucket(endNow, quantization, 0))
				}
				continue
			}

			optPG := opt.Revision.(postgresRevision)
			require.Equal(t, otherRev.optionalTxID.Uint64, optPG.optionalTxID.Uint64)
			require.False(t, optPG.snapshot.txVisible(schemaRev.optionalTxID.Uint64))

			_, err = ds.SnapshotReader(opt.Revision).ReadStoredSchema(ctx)
			require.ErrorIs(t, err, datastore.ErrSchemaNotFound)
			require.Empty(t, opt.SchemaHash, "OptimizedRevision must return empty schema hash when no schema is visible in selected snapshot")
			return
		}
	})

	// Reverse interleaving: T_other acquires an earlier xid than T_schema, T_schema commits
	// S_new, then T_other captures pg_current_snapshot() (which sees T_schema as committed)
	// and inserts its relation_tuple_transaction row with its earlier transaction timestamp.
	// OptimizedRevision selects T_other; ReadStoredSchema sees S_new, so OptimizedRevision
	// must return hashNew (not hashOld).
	t.Run("reverse interleaving: earlier xid sees later committed schema in snapshot", func(t *testing.T) {
		const (
			quantization = 2 * time.Second
			maxAttempts  = 4
		)

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			mustAssert := attempt == maxAttempts
			ctx := t.Context()
			ds, conn := newDS(t, quantization)

			_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteStoredSchema(ctx, storedOld)
			})
			require.NoError(t, err)

			// Wait for a fresh quantization bucket after S_old.
			bucketAtOld := quantizationBucket(queryDBNowUTC(ctx, t, conn), quantization, 0)
			startNewBucket := waitForNextBucket(ctx, t, conn, quantization, bucketAtOld)
			bucketAtOther := quantizationBucket(startNewBucket, quantization, 0)

			// Begin T_other in READ COMMITTED and allocate its xid before T_schema starts.
			// Its transaction timestamp (NOW()) is captured at BEGIN, before T_schema.
			txOther, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			require.NoError(t, err)
			defer func() { _ = txOther.Rollback(context.Background()) }()
			var otherXID xid8
			require.NoError(t, txOther.QueryRow(ctx, "SELECT pg_current_xact_id()").Scan(&otherXID))
			require.True(t, otherXID.Valid)

			// Ensure T_schema gets a strictly later transaction timestamp than T_other.
			time.Sleep(5 * time.Millisecond)

			// Run and commit T_schema (gets xid > otherXID).
			schemaRevRaw, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteStoredSchema(ctx, storedNew)
			})
			require.NoError(t, err)
			schemaRev := schemaRevRaw.(postgresRevision)
			require.Greater(t, schemaRev.optionalTxID.Uint64, otherXID.Uint64)

			// Now T_other captures pg_current_snapshot() (in READ COMMITTED, a fresh statement
			// snapshot that sees T_schema as committed) and writes its transaction row.
			_, err = txOther.Exec(ctx, "INSERT INTO relation_tuple_transaction (xid, snapshot) VALUES (pg_current_xact_id(), pg_current_snapshot())")
			require.NoError(t, err)
			require.NoError(t, txOther.Commit(ctx))

			opt, err := ds.OptimizedRevision(ctx)
			require.NoError(t, err)
			endNow := queryDBNowUTC(ctx, t, conn)

			if quantizationBucket(endNow, quantization, 0) != bucketAtOther {
				if mustAssert {
					require.Failf(t, "quantization boundary crossed", "bucket moved from %d to %d", bucketAtOther, quantizationBucket(endNow, quantization, 0))
				}
				continue
			}

			optPG := opt.Revision.(postgresRevision)
			require.True(t, optPG.optionalTxID.Valid)
			require.Equal(t, otherXID.Uint64, optPG.optionalTxID.Uint64, "OptimizedRevision should select T_other (earlier timestamp)")
			require.True(t, optPG.snapshot.txVisible(schemaRev.optionalTxID.Uint64), "T_schema must be visible in T_other's snapshot")

			storedAtOpt, err := ds.SnapshotReader(opt.Revision).ReadStoredSchema(ctx)
			require.NoError(t, err)
			require.Equal(t, hashNew, storedAtOpt.Get().GetV1().SchemaHash, "ReadStoredSchema at T_other must return S_new")
			require.Equal(t, hashNew, opt.SchemaHash, "OptimizedRevision must return hash matching ReadStoredSchema (S_new), not S_old")
			return
		}
	})

	// When T_schema itself is the selected transaction, selected.snapshot does not yet mark
	// selected.xid complete until OptimizedRevision calls snapshot.markComplete(revision).
	// The SQL lookup must treat selected.xid as complete and return hashNew.
	t.Run("selected transaction is the schema write itself", func(t *testing.T) {
		const (
			quantization = 2 * time.Second
			maxAttempts  = 4
		)

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			mustAssert := attempt == maxAttempts
			ctx := t.Context()
			ds, conn := newDS(t, quantization)

			_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteStoredSchema(ctx, storedOld)
			})
			require.NoError(t, err)

			bucketAtOld := quantizationBucket(queryDBNowUTC(ctx, t, conn), quantization, 0)
			startNewBucket := waitForNextBucket(ctx, t, conn, quantization, bucketAtOld)
			bucketAtNew := quantizationBucket(startNewBucket, quantization, 0)

			schemaRevRaw, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteStoredSchema(ctx, storedNew)
			})
			require.NoError(t, err)
			schemaRev := schemaRevRaw.(postgresRevision)

			opt, err := ds.OptimizedRevision(ctx)
			require.NoError(t, err)
			endNow := queryDBNowUTC(ctx, t, conn)

			if quantizationBucket(endNow, quantization, 0) != bucketAtNew {
				if mustAssert {
					require.Failf(t, "quantization boundary crossed", "bucket moved from %d to %d", bucketAtNew, quantizationBucket(endNow, quantization, 0))
				}
				continue
			}

			optPG := opt.Revision.(postgresRevision)
			require.True(t, optPG.optionalTxID.Valid)
			require.Equal(t, schemaRev.optionalTxID.Uint64, optPG.optionalTxID.Uint64)

			storedAtOpt, err := ds.SnapshotReader(opt.Revision).ReadStoredSchema(ctx)
			require.NoError(t, err)
			require.Equal(t, hashNew, storedAtOpt.Get().GetV1().SchemaHash)
			require.Equal(t, hashNew, opt.SchemaHash)
			return
		}
	})

	// When no transaction exists in the quantization window (selected.xid IS NULL) and for
	// HeadRevision, visibility must use pg_current_snapshot() consistently while a schema
	// write is in flight and after it commits.
	t.Run("selected xid null and HeadRevision with in-flight schema write", func(t *testing.T) {
		ctx := t.Context()
		// 1ns quantization forces selected.xid to be NULL (falls back to pg_current_snapshot()).
		ds, _ := newDS(t, 1*time.Nanosecond)

		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteStoredSchema(ctx, storedOld)
		})
		require.NoError(t, err)

		schemaWritten := make(chan struct{})
		allowCommit := make(chan struct{})
		var releaseOnce sync.Once
		releaseSchema := func() { releaseOnce.Do(func() { close(allowCommit) }) }
		defer releaseSchema()
		schemaErr := make(chan error, 1)
		go func() {
			_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				if err := rwt.WriteStoredSchema(ctx, storedNew); err != nil {
					return err
				}
				close(schemaWritten)
				<-allowCommit
				return nil
			}, options.WithDisableRetries(true))
			schemaErr <- err
		}()

		select {
		case <-schemaWritten:
		case err := <-schemaErr:
			require.Failf(t, "T_schema exited before writing schema", "err=%v", err)
		}

		// While T_schema is uncommitted, both OptimizedRevision (selected.xid IS NULL) and
		// HeadRevision must return hashOld and agree with ReadStoredSchema.
		optDuring, err := ds.OptimizedRevision(ctx)
		require.NoError(t, err)
		require.False(t, optDuring.Revision.(postgresRevision).optionalTxID.Valid, "expected selected.xid IS NULL")
		require.Equal(t, hashOld, optDuring.SchemaHash)
		storedOptDuring, err := ds.SnapshotReader(optDuring.Revision).ReadStoredSchema(ctx)
		require.NoError(t, err)
		require.Equal(t, hashOld, storedOptDuring.Get().GetV1().SchemaHash)

		headDuring, err := ds.HeadRevision(ctx)
		require.NoError(t, err)
		require.Equal(t, hashOld, headDuring.SchemaHash)
		storedHeadDuring, err := ds.SnapshotReader(headDuring.Revision).ReadStoredSchema(ctx)
		require.NoError(t, err)
		require.Equal(t, hashOld, storedHeadDuring.Get().GetV1().SchemaHash)

		// Commit T_schema and verify both now return hashNew while earlier snapshots still read S_old.
		releaseSchema()
		require.NoError(t, <-schemaErr)

		optAfter, err := ds.OptimizedRevision(ctx)
		require.NoError(t, err)
		require.False(t, optAfter.Revision.(postgresRevision).optionalTxID.Valid, "expected selected.xid IS NULL")
		require.Equal(t, hashNew, optAfter.SchemaHash)
		storedOptAfter, err := ds.SnapshotReader(optAfter.Revision).ReadStoredSchema(ctx)
		require.NoError(t, err)
		require.Equal(t, hashNew, storedOptAfter.Get().GetV1().SchemaHash)

		headAfter, err := ds.HeadRevision(ctx)
		require.NoError(t, err)
		require.Equal(t, hashNew, headAfter.SchemaHash)
		storedHeadAfter, err := ds.SnapshotReader(headAfter.Revision).ReadStoredSchema(ctx)
		require.NoError(t, err)
		require.Equal(t, hashNew, storedHeadAfter.Get().GetV1().SchemaHash)

		// Earlier snapshot captured while T_schema was in flight still reads S_old after commit.
		storedHeadDuringAfterCommit, err := ds.SnapshotReader(headDuring.Revision).ReadStoredSchema(ctx)
		require.NoError(t, err)
		require.Equal(t, hashOld, storedHeadDuringAfterCommit.Get().GetV1().SchemaHash)
	})
}
