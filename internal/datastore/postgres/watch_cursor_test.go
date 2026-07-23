//go:build datastore && postgres

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/structpb"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/test"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const cursorWatchTestTimeout = 30 * time.Second

// newDatastoreFunc constructs a datastore against the shared test container,
// with the cursor watch enabled or disabled.
type newDatastoreFunc func(t testing.TB, revisionQuantization, gcWindow time.Duration, watchBufferLength uint16, cursorWatch bool) datastore.Datastore

// TestPostgresCursorWatch runs the datastore Watch conformance suite plus
// targeted cursor-watch tests against a Postgres with wal_level=logical, with
// the cursor watch (and therefore the commit LSN ledger) enabled.
func TestPostgresCursorWatch(t *testing.T) {
	b := testdatastore.RunPostgresForTestingWithLogicalReplication(t, postgresTestVersion())

	newDatastore := newDatastoreFunc(func(t testing.TB, revisionQuantization, gcWindow time.Duration, watchBufferLength uint16, cursorWatch bool) datastore.Datastore {
		ctx := t.Context()
		return b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := newPostgresDatastore(
				ctx, uri, primaryInstanceID,
				RevisionQuantization(revisionQuantization),
				GCWindow(gcWindow),
				GCInterval(veryLargeGCInterval),
				WatchBufferLength(watchBufferLength),
				DebugAnalyzeBeforeStatistics(),
				WithRevisionHeartbeat(false),
				WithLogicalWatch(cursorWatch),
			)
			require.NoError(t, err)
			return ds
		})
	})

	cursorTester := test.DatastoreTesterFunc(func(t testing.TB, revisionQuantization, _, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		return newDatastore(t, revisionQuantization, gcWindow, watchBufferLength, true), nil
	})

	conformanceTests := map[string]func(*testing.T, test.DatastoreTester){
		"TestWatchBasic":                                test.WatchTest,
		"TestWatchCancel":                               test.WatchCancelTest,
		"TestCaveatedRelationshipWatch":                 test.CaveatedRelationshipWatchTest,
		"TestWatchWithTouch":                            test.WatchWithTouchTest,
		"TestWatchWithDelete":                           test.WatchWithDeleteTest,
		"TestWatchWithMetadata":                         test.WatchWithMetadataTest,
		"TestWatchWithExpiration":                       test.WatchWithExpirationTest,
		"TestWatchEmissionStrategy":                     test.WatchEmissionStrategyTest,
		"TestWatchSchema":                               test.WatchSchemaTest,
		"TestWatchRelationshipsAndSchemaChanges":        test.WatchRelationshipsAndSchemaChangesTest,
		"TestWatchObservesEveryReturnedRevision":        test.WatchObservesEveryReturnedRevisionTest,
		"TestWatchEmitsCheckpointAfterWriteWithChanges": test.WatchEmitsCheckpointAfterWriteWithChangesTest,
		"TestWatchRelationshipsAndSchemaAndCheckpoints": test.WatchRelationshipsAndSchemaAndCheckpointsTest,
	}

	for name, testFunc := range conformanceTests {
		t.Run(name, func(t *testing.T) {
			testFunc(t, cursorTester)
		})
	}

	// The invariant the whole design rests on.
	t.Run("FrontierBoundedDeliveryIsExactlyOnce", func(t *testing.T) {
		testFrontierBoundedDeliveryIsExactlyOnce(t, b)
	})

	t.Run("GCDoesNotEmitChanges", func(t *testing.T) {
		testCursorWatchIgnoresGC(t, newDatastore)
	})

	t.Run("ParityWithPollingWatch", func(t *testing.T) {
		testCursorWatchParity(t, b)
	})

	t.Run("OverlappingTransactions", func(t *testing.T) {
		testCursorWatchOverlappingTransactions(t, newDatastore)
	})

	t.Run("TokenReadRoundTrip", func(t *testing.T) {
		testCursorWatchTokenReadRoundTrip(t, newDatastore)
	})

	t.Run("CommitOrderLinearExtension", func(t *testing.T) {
		testCursorWatchCommitOrderLinearExtension(t, newDatastore)
	})

	t.Run("WatchArgumentValidation", func(t *testing.T) {
		testCursorWatchArgumentValidation(t, newDatastore)
	})

	t.Run("CrossEpochTokenStability", func(t *testing.T) {
		testCursorWatchCrossEpochTokenStability(t, newDatastore)
	})

	t.Run("SameTokenFromAnyCursor", func(t *testing.T) {
		testCursorWatchSameTokenFromAnyCursor(t, newDatastore)
	})

	t.Run("ReconnectWithPositionedToken", func(t *testing.T) {
		testCursorWatchReconnectWithPositionedToken(t, newDatastore)
	})

	t.Run("ConcurrentWatchers", func(t *testing.T) {
		testCursorWatchConcurrentWatchers(t, newDatastore)
	})

	t.Run("DisconnectDuringBackfillLosesNothing", func(t *testing.T) {
		testCursorWatchDisconnectDuringBackfillLosesNothing(t, newDatastore)
	})

	t.Run("LegacyTokenHandoff", func(t *testing.T) {
		testCursorWatchLegacyTokenHandoff(t, b)
	})

	t.Run("CheckpointsAreExactOnResume", func(t *testing.T) {
		testCursorWatchCheckpointsAreExactOnResume(t, b)
	})

	t.Run("BacklogDrainsWithoutSleeping", func(t *testing.T) {
		testCursorWatchBacklogDrainsWithoutSleeping(t, b)
	})

	t.Run("IdleCheckpointsFollowTheFrontier", func(t *testing.T) {
		testCursorWatchIdleCheckpointsFollowTheFrontier(t, b)
	})

	t.Run("StaleRevisionRejected", func(t *testing.T) {
		testCursorWatchStaleRevisionRejected(t, newDatastore)
	})

	t.Run("GapBelowCursorFailsLoudly", func(t *testing.T) {
		testCursorWatchGapBelowCursorFailsLoudly(t, b)
	})

	t.Run("GapRecordedOnOperatorDrop", func(t *testing.T) {
		testLedgerGapRecordedOnOperatorDrop(t, b)
	})

	t.Run("GapIsReplayedFromTheTables", func(t *testing.T) {
		testLedgerGapIsReplayedFromTheTables(t, b)
	})

	t.Run("PreLedgerPositionsAreBackfilled", func(t *testing.T) {
		testPreLedgerPositionsAreBackfilled(t, b)
	})

	t.Run("PreLedgerBackfillStopsAtGCWindow", func(t *testing.T) {
		testPreLedgerBackfillStopsAtGCWindow(t, b)
	})

	t.Run("LedgerWithoutWriterFailsWatch", func(t *testing.T) {
		testCursorWatchLedgerWithoutWriterFails(t, b)
	})

	t.Run("LedgerFrontierWaitTimeout", func(t *testing.T) {
		testLedgerFrontierWaitTimeout(t, b)
	})

	t.Run("LedgerRecordsWhatTheStreamReports", func(t *testing.T) {
		testLedgerRecordsWhatTheStreamReports(t, b)
	})

	t.Run("LedgerTakeover", func(t *testing.T) {
		testLedgerTakeover(t, b)
	})

	t.Run("PreLedgerHistoryIsDeliveredPositioned", func(t *testing.T) {
		testLedgerPreLedgerHistoryIsDeliveredPositioned(t, b)
	})

	t.Run("UnrecordedTransactionFailsLoudly", func(t *testing.T) {
		testLedgerUnrecordedTransactionFailsLoudly(t, b)
	})

	t.Run("LedgerWritesAreInvisibleToWatchers", func(t *testing.T) {
		testLedgerWritesAreInvisibleToWatchers(t, newDatastore)
	})

	t.Run("DisabledFeatureDetectsAbandonedSlot", func(t *testing.T) {
		testLedgerDisabledFeatureDetectsAbandonedSlot(t, b)
	})

	t.Run("LedgerStorageShape", func(t *testing.T) {
		testLedgerStorageShape(t, b)
	})

	t.Run("LedgerPositionsAreGarbageCollected", func(t *testing.T) {
		testLedgerPositionsAreGarbageCollected(t, b)
	})

	t.Run("LedgerOrphanPositionsAreIgnored", func(t *testing.T) {
		testLedgerOrphanPositionsAreIgnored(t, b)
	})
}

// newCursorWatchTestDatastore builds a datastore with the cursor watch enabled
// against a fresh database, returning it alongside that database's URI so a test
// can inspect or damage the ledger's state directly.
func newCursorWatchTestDatastore(t *testing.T, b testdatastore.RunningEngineForTest, options ...Option) (datastore.Datastore, string) {
	t.Helper()

	var dbURI string
	ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		dbURI = uri
		allOptions := append([]Option{
			RevisionQuantization(0),
			GCWindow(1000 * time.Second),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(512),
			WithRevisionHeartbeat(false),
			WithLogicalWatch(true),
		}, options...)

		ds, err := newPostgresDatastore(t.Context(), uri, primaryInstanceID, allOptions...)
		require.NoError(t, err)
		return ds
	})

	return ds, dbURI
}

// testFrontierBoundedDeliveryIsExactlyOnce is the test the design rests on.
//
// Concurrent writers commit a known set of transactions while a cursor loop
// reads (cursor, frontier] in commit-position order, exactly as the watch does,
// using the production queries against a raw connection. Every committed
// transaction must be delivered exactly once, in strictly ascending position
// order, with no position ever above the frontier that bounded its own batch.
func testFrontierBoundedDeliveryIsExactlyOnce(t *testing.T, b testdatastore.RunningEngineForTest) {
	testCases := []struct {
		name      string
		writers   int
		perWriter int
		batchSize int
	}{
		{name: "a single writer, one transaction per batch", writers: 1, perWriter: 12, batchSize: 1},
		{name: "concurrent writers, small batches", writers: 4, perWriter: 8, batchSize: 3},
		{name: "concurrent writers, one batch could hold everything", writers: 6, perWriter: 5, batchSize: 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			ds, dbURI := newCursorWatchTestDatastore(t, b)

			conn, err := pgx.Connect(ctx, dbURI)
			require.NoError(err)
			defer func() { _ = conn.Close(ctx) }()
			RegisterTypes(conn.TypeMap())

			pgds, ok := ds.(*pgDatastore)
			require.True(ok)
			slotName := pgds.ledgerSlotName

			// Delivery starts at the frontier as it stands now, so the ground
			// truth is exactly the transactions written below.
			cursor := readLedgerFrontier(t, ctx, conn, slotName)

			var mu sync.Mutex
			expected := make(map[uint64]struct{}, tc.writers*tc.perWriter)

			group, groupCtx := errgroup.WithContext(ctx)
			for writer := 0; writer < tc.writers; writer++ {
				group.Go(func() error {
					for iteration := 0; iteration < tc.perWriter; iteration++ {
						rel := tuple.MustParse(fmt.Sprintf("document:frontier#viewer@user:w%d_i%d", writer, iteration))
						revision, err := ds.ReadWriteTx(groupCtx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
							return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
						})
						if err != nil {
							return err
						}

						txid, ok := revision.(postgresRevision).OptionalTransactionID()
						if !ok {
							return errors.New("write revision missing its transaction ID")
						}
						mu.Lock()
						expected[txid.Uint64] = struct{}{}
						mu.Unlock()
					}
					return nil
				})
			}
			require.NoError(group.Wait())

			mu.Lock()
			groundTruth := make(map[uint64]struct{}, len(expected))
			for xid := range expected {
				groundTruth[xid] = struct{}{}
			}
			mu.Unlock()

			delivered := make(map[uint64]int, len(groundTruth))
			var lastPosition pglogrepl.LSN
			remaining := len(groundTruth)
			deadline := time.After(cursorWatchTestTimeout)

			for remaining > 0 {
				select {
				case <-deadline:
					require.Failf("timed out", "%d transactions still undelivered", remaining)
				default:
				}

				frontier := readLedgerFrontier(t, ctx, conn, slotName)
				if frontier <= cursor {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				rows, err := conn.Query(ctx, cursorWatchRevisionsQuery, cursor.String(), frontier.String(), tc.batchSize)
				require.NoError(err)

				type deliveredRow struct {
					xid      uint64
					position pglogrepl.LSN
				}
				batch := make([]deliveredRow, 0, tc.batchSize)
				for rows.Next() {
					revision, positionText, err := scanWatchRevisionRow(rows)
					require.NoError(err)
					require.NotNil(positionText, "the discovery query must never return an unrecorded transaction")

					position, err := pglogrepl.ParseLSN(*positionText)
					require.NoError(err)
					batch = append(batch, deliveredRow{xid: revision.optionalTxID.Uint64, position: position})
				}
				require.NoError(rows.Err())
				rows.Close()

				require.LessOrEqual(len(batch), tc.batchSize, "a batch must never exceed the requested size")

				for _, row := range batch {
					require.Greater(row.position, lastPosition, "positions must strictly ascend across batches")
					require.LessOrEqual(row.position, frontier, "no delivered position may exceed the frontier that bounded its batch")
					lastPosition = row.position

					delivered[row.xid]++
					if _, isGroundTruth := groundTruth[row.xid]; isGroundTruth && delivered[row.xid] == 1 {
						remaining--
					}
				}

				if len(batch) > 0 {
					cursor = batch[len(batch)-1].position
					continue
				}

				// Nothing in the window: the frontier covers only transactions
				// this cursor has already passed.
				cursor = frontier
			}

			for xid := range groundTruth {
				require.Equal(1, delivered[xid], "transaction %d was not delivered exactly once", xid)
			}
			for xid, count := range delivered {
				require.Equal(1, count, "transaction %d was delivered %d times", xid, count)
			}
		})
	}
}

// readLedgerFrontier reads the ledger slot's confirmed position, which is the
// bound the cursor watch delivers up to.
func readLedgerFrontier(t *testing.T, ctx context.Context, conn *pgx.Conn, slotName string) pglogrepl.LSN {
	t.Helper()

	var confirmedText *string
	var active bool
	var walStatus, database string
	require.NoError(t, conn.QueryRow(ctx, selectSlotStateQuery, slotName).Scan(&confirmedText, &active, &walStatus, &database))
	if confirmedText == nil {
		return 0
	}

	confirmed, err := pglogrepl.ParseLSN(*confirmedText)
	require.NoError(t, err)
	return confirmed
}

// recordedCommitLSNs reads the commit positions the ledger has recorded, keyed by
// transaction ID. A transaction with no recorded position is absent from the result.
func recordedCommitLSNs(t *testing.T, ctx context.Context, conn *pgx.Conn) map[uint64]string {
	t.Helper()

	rows, err := conn.Query(ctx, "SELECT xid::text::bigint, commit_lsn::text FROM ledger_xid_lsn;")
	require.NoError(t, err)
	defer rows.Close()

	recorded := make(map[uint64]string)
	for rows.Next() {
		var xid uint64
		var commitLSN string
		require.NoError(t, rows.Scan(&xid, &commitLSN))
		recorded[xid] = commitLSN
	}
	require.NoError(t, rows.Err())

	return recorded
}

// testCursorWatchIgnoresGC asserts that physical DELETEs performed by garbage
// collection never surface as relationship removals, and that a watch keeps
// running across a collection pass.
func testCursorWatchIgnoresGC(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// A tiny GC window makes everything soft-deleted immediately collectable.
	ds := newDatastore(t, 0, 1*time.Millisecond, 128, true)

	relCreated := tuple.MustParse("document:gcdoc#viewer@user:kept")
	relDeleted := tuple.MustParse("document:gcdoc#viewer@user:removed")

	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(relCreated), tuple.Touch(relDeleted)})
	})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Delete(relDeleted)})
	})
	require.NoError(err)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	// Prove the watch is delivering before running GC.
	marker := tuple.MustParse("document:gcdoc#viewer@user:marker")
	markerRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(marker)})
	})
	require.NoError(err)

	seen := collectChangesUntilRevision(t, changes, errchan, markerRevision)

	// Run garbage collection, physically deleting the soft-deleted row and old
	// transaction rows.
	pgds, ok := ds.(*pgDatastore)
	require.True(ok)

	gc, err := pgds.BuildGarbageCollector(ctx)
	require.NoError(err)
	defer gc.Close()

	now, err := gc.Now(ctx)
	require.NoError(err)

	collectBefore, err := gc.TxIDBefore(ctx, now)
	require.NoError(err)

	deleted, err := gc.DeleteBeforeTx(ctx, collectBefore)
	require.NoError(err)
	require.GreaterOrEqual(deleted.Relationships, int64(1), "expected garbage collection to delete at least the soft-deleted relationship row")

	// A final write proves the watch survived GC and stayed ordered. A running
	// watch is not failed by collection below its own position: it has already
	// delivered everything down there.
	relAfter := tuple.MustParse("document:gcdoc#viewer@user:after")
	afterRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(relAfter)})
	})
	require.NoError(err)

	seen = append(seen, collectChangesUntilRevision(t, changes, errchan, afterRevision)...)

	// The only relationship changes observed must be the two live writes: the
	// collected rows must not have produced phantom removals.
	var observed []string
	for _, change := range seen {
		for _, relChange := range change.RelationshipChanges {
			observed = append(observed, relChange.DebugString())
		}
	}

	require.ElementsMatch([]string{
		tuple.Touch(marker).DebugString(),
		tuple.Touch(relAfter).DebugString(),
	}, observed, "garbage collection must not emit any relationship changes")
}

// testCursorWatchParity runs the polling watch and the cursor watch side by side
// over the same database, drives an identical workload including concurrent
// writers committing out of transaction-ID order, and asserts that both produce
// observationally equivalent changes, transaction by transaction.
func testCursorWatchParity(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cursorDS, dbURI := newCursorWatchTestDatastore(t, b)

	pollingDS, err := newPostgresDatastore(
		ctx, dbURI, primaryInstanceID,
		RevisionQuantization(0),
		GCWindow(1000*time.Second),
		GCInterval(veryLargeGCInterval),
		WatchBufferLength(512),
		WithRevisionHeartbeat(false),
		WithLogicalWatch(false),
	)
	require.NoError(err)
	t.Cleanup(func() { _ = pollingDS.Close() })

	headRevision, err := cursorDS.HeadRevision(ctx)
	require.NoError(err)

	watchOptions := datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchSchema | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	}

	cursorChanges, cursorErrs := cursorDS.Watch(ctx, headRevision.Revision, watchOptions)
	require.Empty(cursorErrs)
	pollingChanges, pollingErrs := pollingDS.Watch(ctx, headRevision.Revision, watchOptions)
	require.Empty(pollingErrs)

	metadata, err := structpb.NewStruct(map[string]any{"reason": "parity"})
	require.NoError(err)

	plainRel := tuple.MustParse("paritydoc:doc1#viewer@parityuser:alice")
	caveatedRel := tuple.MustParse(`paritydoc:doc1#viewer@parityuser:bob[paritycaveat:{"tenant":"one"}]`)
	retouchedRel := tuple.MustParse(`paritydoc:doc1#viewer@parityuser:bob[paritycaveat:{"tenant":"two"}]`)

	workloadSteps := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{
			name: "write schema definitions",
			run: func(ctx context.Context) error {
				_, err := cursorDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					if err := rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{Name: "paritydoc"}, &core.NamespaceDefinition{Name: "parityuser"}); err != nil {
						return err
					}
					return rwt.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{{Name: "paritycaveat", SerializedExpression: []byte("parity-expression")}})
				})
				return err
			},
		},
		{
			name: "write caveated relationships with transaction metadata",
			run: func(ctx context.Context) error {
				_, err := cursorDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(plainRel), tuple.Touch(caveatedRel)})
				}, options.WithMetadata(metadata))
				return err
			},
		},
		{
			name: "touch changing the caveat context (delete-old + insert-new)",
			run: func(ctx context.Context) error {
				_, err := cursorDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(retouchedRel)})
				})
				return err
			},
		},
		{
			name: "delete a relationship",
			run: func(ctx context.Context) error {
				_, err := cursorDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Delete(plainRel)})
				})
				return err
			},
		},
		{
			name: "concurrent writers commit out of transaction-ID order",
			run: func(ctx context.Context) error {
				group, groupCtx := errgroup.WithContext(ctx)
				for writer := 0; writer < 5; writer++ {
					group.Go(func() error {
						for iteration := 0; iteration < 4; iteration++ {
							rel := tuple.MustParse(fmt.Sprintf("paritydoc:concurrent#viewer@parityuser:w%d_i%d", writer, iteration))
							if _, err := cursorDS.ReadWriteTx(groupCtx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
								return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
							}); err != nil {
								return err
							}
						}
						return nil
					})
				}
				return group.Wait()
			},
		},
		{
			name: "rewrite a namespace definition",
			run: func(ctx context.Context) error {
				_, err := cursorDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					return rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{Name: "paritydoc", Metadata: &core.Metadata{}})
				})
				return err
			},
		},
		{
			name: "delete a namespace",
			run: func(ctx context.Context) error {
				_, err := cursorDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					return rwt.LegacyDeleteNamespaces(ctx, []string{"paritydoc"}, datastore.DeleteNamespacesOnly)
				})
				return err
			},
		},
		{
			name: "delete a caveat",
			run: func(ctx context.Context) error {
				_, err := cursorDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
					return rwt.LegacyDeleteCaveats(ctx, []string{"paritycaveat"})
				})
				return err
			},
		},
	}

	for _, step := range workloadSteps {
		require.NoError(step.run(ctx), "workload step %q failed", step.name)
	}

	finalRevision, err := cursorDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("paritysentinel:final#viewer@parityuser:done"))})
	})
	require.NoError(err)

	cursorCollected := collectChangesUntilRevision(t, cursorChanges, cursorErrs, finalRevision)
	pollingCollected := collectChangesUntilRevision(t, pollingChanges, pollingErrs, finalRevision)

	cursorNormalized := normalizeChangesByTransaction(t, cursorCollected)
	pollingNormalized := normalizeChangesByTransaction(t, pollingCollected)

	require.Equal(pollingNormalized, cursorNormalized, "the cursor watch and the polling watch must produce observationally equivalent changes")
}

// runOverlappingTransactionPair deterministically produces a pair of
// overlapping (snapshot-concurrent) transactions. The holder transaction opens
// and writes holderRel, then blocks until the inner transaction has written
// innerRel and committed, and only then commits. The inner therefore always
// commits strictly inside the holder's open window: the two are genuinely
// concurrent (each snapshot has the other in flight) and the inner commits
// first. Retries are disabled so an unexpected conflict fails fast instead of
// silently rerunning the choreography. Both relationships must be distinct so
// the two transactions never conflict.
//
// It returns (innerRevision, holderRevision): the inner committed first (and
// therefore carries the smaller commit position), the holder last.
func runOverlappingTransactionPair(t *testing.T, ctx context.Context, ds datastore.Datastore, holderRel, innerRel tuple.Relationship) (postgresRevision, postgresRevision) {
	t.Helper()

	holderWritten := make(chan struct{})
	innerCommitted := make(chan struct{})
	holderDone := make(chan struct{})

	var rawHolderRev datastore.Revision
	var holderErr error
	go func() {
		defer close(holderDone)
		rawHolderRev, holderErr = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			if err := rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(holderRel)}); err != nil {
				return err
			}
			close(holderWritten)
			select {
			case <-innerCommitted:
				return nil
			case <-time.After(cursorWatchTestTimeout):
				return errors.New("timed out waiting for the inner transaction to commit")
			}
		}, options.WithDisableRetries(true))
	}()

	select {
	case <-holderWritten:
	case <-time.After(cursorWatchTestTimeout):
		require.FailNow(t, "timed out waiting for the holder transaction to open")
	}

	rawInnerRev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(innerRel)})
	}, options.WithDisableRetries(true))
	require.NoError(t, err)
	close(innerCommitted)

	<-holderDone
	require.NoError(t, holderErr)

	innerRev, ok := rawInnerRev.(postgresRevision)
	require.True(t, ok)
	holderRev, ok := rawHolderRev.(postgresRevision)
	require.True(t, ok)
	return innerRev, holderRev
}

// testCursorWatchOverlappingTransactions choreographs two deterministically
// overlapping transactions (B starts first, A starts and commits entirely within
// B's lifetime, B commits last) and asserts the full ordering for the pair: the
// snapshots abstain from ordering them, the commit positions order them by commit
// order, wherever the snapshots do speak the positions agree, and reads at the
// emitted tokens follow snapshot visibility.
func testCursorWatchOverlappingTransactions(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, 1000*time.Second, 512, true)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	relA := tuple.MustParse("document:overlap#viewer@user:committed_first")
	relB := tuple.MustParse("document:overlap#viewer@user:committed_last")
	relC := tuple.MustParse("document:overlap#viewer@user:sequential_control")

	// Deterministically overlap A and B, where B (the holder) holds its
	// transaction open until A (the inner) has committed.
	writeRevA, writeRevB := runOverlappingTransactionPair(t, ctx, ds, relB, relA)

	// A sequential control transaction, committed after both, that the snapshots
	// CAN order against A and B.
	revC, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(relC)})
	})
	require.NoError(err)
	writeRevC, ok := revC.(postgresRevision)
	require.True(ok)

	// The overlapping pair is concurrent, so the write revisions' snapshots
	// abstain from ordering them in either direction.
	require.False(writeRevA.GreaterThan(writeRevB))
	require.False(writeRevA.LessThan(writeRevB))
	require.False(writeRevA.Equal(writeRevB))
	require.False(writeRevB.GreaterThan(writeRevA))
	require.False(writeRevB.LessThan(writeRevA))

	// Where the snapshots do define an order, it is the commit order.
	require.True(writeRevC.GreaterThan(writeRevA))
	require.True(writeRevC.GreaterThan(writeRevB))

	events := collectChangesUntilXids(t, changes, errchan, revisionXids(t, writeRevA, writeRevB, writeRevC))

	indexA, eventA := requireChangeForSubject(t, events, "committed_first")
	indexB, eventB := requireChangeForSubject(t, events, "committed_last")
	indexC, eventC := requireChangeForSubject(t, events, "sequential_control")

	// Delivery is in commit order: A first, even though B started first.
	require.Less(indexA, indexB, "the first-committed transaction must be delivered first")
	require.Less(indexB, indexC)

	streamRevA, ok := eventA.Revision.(postgresRevision)
	require.True(ok)
	streamRevB, ok := eventB.Revision.(postgresRevision)
	require.True(ok)
	streamRevC, ok := eventC.Revision.(postgresRevision)
	require.True(ok)

	// All three carry commit positions, ascending in commit order.
	require.NotZero(streamRevA.optionalCommitLSN)
	require.NotZero(streamRevB.optionalCommitLSN)
	require.NotZero(streamRevC.optionalCommitLSN)
	require.Less(streamRevA.optionalCommitLSN, streamRevB.optionalCommitLSN)
	require.Less(streamRevB.optionalCommitLSN, streamRevC.optionalCommitLSN)

	// Token comparison agrees with the delivery order for the overlapping pair,
	// which the snapshots alone could not order...
	require.True(streamRevB.GreaterThan(streamRevA))
	require.True(streamRevA.LessThan(streamRevB))
	require.Less(streamRevA.String(), streamRevB.String(), "revision strings must be byte-sortable in commit order")

	// ...and never contradicts the snapshots where they speak: the position
	// order is a linear extension of the snapshot partial order.
	require.True(streamRevC.GreaterThan(streamRevA))
	require.True(streamRevC.GreaterThan(streamRevB))

	// The string form round-trips both components.
	for _, streamRev := range []postgresRevision{streamRevA, streamRevB, streamRevC} {
		parsed, err := ParseRevisionString(streamRev.String())
		require.NoError(err)
		require.True(parsed.Equal(streamRev))
		require.Equal(streamRev.optionalCommitLSN, parsed.(postgresRevision).optionalCommitLSN)
	}

	// Reads at the tokens follow snapshot visibility. B's token does not see the
	// concurrent A write (B's snapshot predates A's commit), while the sequential
	// C token sees everything. A change revision means "the writer's view plus
	// its own write", not "everything with a smaller position".
	subjectsAtB := readSubjectIDs(t, ctx, ds, eventB.Revision, "document")
	require.Contains(subjectsAtB, "committed_last")
	require.NotContains(subjectsAtB, "committed_first")

	subjectsAtC := readSubjectIDs(t, ctx, ds, eventC.Revision, "document")
	require.Contains(subjectsAtC, "committed_first")
	require.Contains(subjectsAtC, "committed_last")
	require.Contains(subjectsAtC, "sequential_control")
}

// testCursorWatchTokenReadRoundTrip asserts that a revision received over the
// cursor watch is usable, after a full round-trip through its string (token)
// form, against the rest of the datastore API of the same deployment:
// CheckRevision accepts it and SnapshotReader serves the state as of that write.
func testCursorWatchTokenReadRoundTrip(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, 1000*time.Second, 512, true)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	relFirst := tuple.MustParse("document:tokendoc#viewer@user:first")
	relSecond := tuple.MustParse("document:tokendoc#viewer@user:second")

	revFirst, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(relFirst)})
	})
	require.NoError(err)

	revSecond, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(relSecond)})
	})
	require.NoError(err)

	events := collectChangesUntilXids(t, changes, errchan, revisionXids(t, revFirst, revSecond))
	_, eventFirst := requireChangeForSubject(t, events, "first")
	_, eventSecond := requireChangeForSubject(t, events, "second")

	streamRevFirst, ok := eventFirst.Revision.(postgresRevision)
	require.True(ok)
	streamRevSecond, ok := eventSecond.Revision.(postgresRevision)
	require.True(ok)

	// Sequential writes: the snapshot order and the position order agree.
	require.True(streamRevSecond.GreaterThan(streamRevFirst))
	require.True(streamRevSecond.snapshot.GreaterThan(streamRevFirst.snapshot))
	require.Less(streamRevFirst.optionalCommitLSN, streamRevSecond.optionalCommitLSN)

	// The token round-trip: what a Watch API consumer receives, parsed back the
	// way the API layer parses incoming ZedTokens.
	parsedRevisions := make([]datastore.Revision, 0, 2)
	for _, event := range []datastore.RevisionChanges{eventFirst, eventSecond} {
		streamRev, ok := event.Revision.(postgresRevision)
		require.True(ok)

		parsed, err := ParseRevisionString(streamRev.String())
		require.NoError(err)
		require.True(parsed.Equal(streamRev))
		require.Equal(streamRev.optionalCommitLSN, parsed.(postgresRevision).optionalCommitLSN)

		// The parsed token is a valid read revision on the same deployment.
		require.NoError(ds.CheckRevision(ctx, parsed))
		parsedRevisions = append(parsedRevisions, parsed)
	}

	// Reads at the parsed tokens serve the state as of each write.
	subjectsAtFirst := readSubjectIDs(t, ctx, ds, parsedRevisions[0], "document")
	require.Contains(subjectsAtFirst, "first")
	require.NotContains(subjectsAtFirst, "second")

	subjectsAtSecond := readSubjectIDs(t, ctx, ds, parsedRevisions[1], "document")
	require.Contains(subjectsAtSecond, "first")
	require.Contains(subjectsAtSecond, "second")
}

// testCursorWatchCommitOrderLinearExtension drives overlapping concurrent
// writers and asserts the stream-level invariants over everything emitted:
// exactly-once delivery, total ordering by commit position, and the
// linear-extension property (a later-delivered change is never snapshot-ordered
// before an earlier one, so the position order refines but never contradicts the
// snapshot partial order).
func testCursorWatchCommitOrderLinearExtension(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, 1000*time.Second, 1024, true)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	// Deterministically produce overlapping (snapshot-concurrent) pairs: in each
	// pair the holder holds its write open until the inner has committed. Pairs
	// run sequentially, so at most two write connections are held at once and
	// the choreography cannot deadlock the pool.
	const pairCount = 8

	type overlappingPair struct {
		innerXid  uint64
		holderXid uint64
	}
	pairs := make([]overlappingPair, 0, pairCount)
	awaitedXids := make(map[uint64]struct{}, pairCount*2)

	for p := 0; p < pairCount; p++ {
		holderRel := tuple.MustParse(fmt.Sprintf("document:linext#viewer@user:holder_%d", p))
		innerRel := tuple.MustParse(fmt.Sprintf("document:linext#viewer@user:inner_%d", p))

		innerRev, holderRev := runOverlappingTransactionPair(t, ctx, ds, holderRel, innerRel)

		// The choreographed pair is concurrent: neither write revision's
		// snapshot orders the other.
		require.False(innerRev.GreaterThan(holderRev))
		require.False(innerRev.LessThan(holderRev))
		require.False(holderRev.GreaterThan(innerRev))
		require.False(holderRev.LessThan(innerRev))

		innerXid, ok := innerRev.OptionalTransactionID()
		require.True(ok)
		holderXid, ok := holderRev.OptionalTransactionID()
		require.True(ok)

		pairs = append(pairs, overlappingPair{innerXid: innerXid.Uint64, holderXid: holderXid.Uint64})
		awaitedXids[innerXid.Uint64] = struct{}{}
		awaitedXids[holderXid.Uint64] = struct{}{}
	}

	events := collectChangesUntilXids(t, changes, errchan, awaitedXids)

	observedCounts := make(map[uint64]int, len(awaitedXids))
	emissionIndexByXid := make(map[uint64]int, len(awaitedXids))
	changeRevisions := make([]postgresRevision, 0, len(awaitedXids))
	var lastChangeLSN, lastCheckpointLSN, lastAnyLSN uint64

	for _, event := range events {
		revision, ok := event.Revision.(postgresRevision)
		require.True(ok)
		require.True(revision.ByteSortable(), "every delivered event must carry a commit position")

		// Delivery never goes backwards, and changes and checkpoints each
		// strictly increase (a change and its own checkpoint share a position).
		require.GreaterOrEqual(revision.optionalCommitLSN, lastAnyLSN)
		lastAnyLSN = revision.optionalCommitLSN

		if event.IsCheckpoint {
			require.Greater(revision.optionalCommitLSN, lastCheckpointLSN, "checkpoints must strictly advance")
			lastCheckpointLSN = revision.optionalCommitLSN
			continue
		}

		require.Greater(revision.optionalCommitLSN, lastChangeLSN, "changes must strictly advance")
		lastChangeLSN = revision.optionalCommitLSN

		txid, ok := revision.OptionalTransactionID()
		require.True(ok)
		observedCounts[txid.Uint64]++
		emissionIndexByXid[txid.Uint64] = len(changeRevisions)
		changeRevisions = append(changeRevisions, revision)
	}

	// Exactly-once delivery: every committed transaction observed once, and
	// nothing else observed at all.
	for xid := range awaitedXids {
		require.Equal(1, observedCounts[xid], "transaction %d must be observed exactly once", xid)
	}
	for xid := range observedCounts {
		_, expected := awaitedXids[xid]
		require.True(expected, "unexpected transaction %d was delivered", xid)
	}

	// The linear-extension property against real snapshots: an
	// earlier-delivered change is never snapshot-greater than a later one.
	for i := 0; i < len(changeRevisions); i++ {
		for j := i + 1; j < len(changeRevisions); j++ {
			require.False(changeRevisions[i].snapshot.GreaterThan(changeRevisions[j].snapshot),
				"ordering reversal: change %d is snapshot-greater than later-delivered change %d", i, j)
		}
	}

	// Every choreographed pair overlapped by construction, so delivery must
	// place the inner (committed-first) transaction before the holder, with a
	// strictly smaller position. This is the concurrent case where only the
	// commit position, not the snapshots, can supply the order.
	for _, pair := range pairs {
		innerIndex := emissionIndexByXid[pair.innerXid]
		holderIndex := emissionIndexByXid[pair.holderXid]
		require.Less(innerIndex, holderIndex, "the inner (committed-first) transaction must be delivered before the holder")
		require.Less(changeRevisions[innerIndex].optionalCommitLSN, changeRevisions[holderIndex].optionalCommitLSN,
			"the inner transaction must carry a smaller commit position than the holder")
	}
}

// testCursorWatchArgumentValidation asserts the argument checks of the cursor
// watch entry point.
func testCursorWatchArgumentValidation(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, 1000*time.Second, 128, true)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// EmitImmediatelyStrategy requires checkpoints.
	_, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:          datastore.WatchRelationships,
		EmissionStrategy: datastore.EmitImmediatelyStrategy,
	})
	select {
	case err := <-errchan:
		require.ErrorContains(err, "EmitImmediatelyStrategy requires WatchCheckpoints")
	case <-time.After(cursorWatchTestTimeout):
		require.Fail("expected an immediate error for EmitImmediatelyStrategy without checkpoints")
	}

	// A revision of a foreign type is rejected instead of panicking.
	_, errchan = ds.Watch(ctx, datastore.NoRevision, datastore.WatchOptions{
		Content: datastore.WatchRelationships,
	})
	select {
	case err := <-errchan:
		require.Error(err)
	case <-time.After(cursorWatchTestTimeout):
		require.Fail("expected an immediate error for a non-postgres revision")
	}
}

// testCursorWatchCrossEpochTokenStability asserts that a transaction's position
// is a property of that transaction, not of the Watch call that delivered it.
// Two Watch calls from the same revision must mint byte-identical tokens for the
// same transaction, and tokens minted by different calls must still sort in
// commit order.
//
// This is the defect the ledger exists to fix: with positions fabricated per
// Watch call, a second call replaying old history minted tokens that outranked
// tokens an earlier call had already emitted for newer transactions.
func testCursorWatchCrossEpochTokenStability(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, 1000*time.Second, 512, true)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	writeRelationship := func(subjectID string) uint64 {
		rel := tuple.MustParse("document:epoch#viewer@user:" + subjectID)
		revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)

		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		return txid.Uint64
	}

	// Sequential writes, so write order is commit order is snapshot order: the
	// ground truth every call's tokens must agree with.
	writtenXidsInOrder := make([]uint64, 0, 4)
	for i := 0; i < 3; i++ {
		writtenXidsInOrder = append(writtenXidsInOrder, writeRelationship(fmt.Sprintf("historical_%d", i)))
	}

	watchOptions := datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	}

	// The first call backfills the historical writes, then observes one further
	// write as it commits.
	firstCtx, cancelFirst := context.WithCancel(ctx)
	firstChanges, firstErrs := ds.Watch(firstCtx, headRevision.Revision, watchOptions)
	require.Empty(firstErrs)

	firstTokens := collectTokensByXid(t, firstChanges, firstErrs, setOfXids(writtenXidsInOrder...))

	liveXid := writeRelationship("live")
	writtenXidsInOrder = append(writtenXidsInOrder, liveXid)
	for xid, token := range collectTokensByXid(t, firstChanges, firstErrs, setOfXids(liveXid)) {
		firstTokens[xid] = token
	}
	cancelFirst()

	// The second call starts from the same revision, so every transaction above,
	// including the one the first call saw as it happened, now arrives via the
	// backfill.
	secondCtx, cancelSecond := context.WithCancel(ctx)
	defer cancelSecond()
	secondChanges, secondErrs := ds.Watch(secondCtx, headRevision.Revision, watchOptions)
	require.Empty(secondErrs)

	secondTokens := collectTokensByXid(t, secondChanges, secondErrs, setOfXids(writtenXidsInOrder...))

	for _, xid := range writtenXidsInOrder {
		require.Equal(firstTokens[xid], secondTokens[xid],
			"transaction %d was delivered with different tokens by two watch calls", xid)
	}

	// Pairing every second-call token against every first-call token covers the
	// inversion an invocation-scoped position produces.
	for lhsIndex, lhsXid := range writtenXidsInOrder {
		for rhsIndex, rhsXid := range writtenXidsInOrder {
			if lhsIndex == rhsIndex {
				continue
			}

			lhs := positionPrefixOf(t, secondTokens[lhsXid])
			rhs := positionPrefixOf(t, firstTokens[rhsXid])
			if lhsIndex < rhsIndex {
				require.Less(lhs, rhs,
					"transaction %d committed before %d, but its token does not sort below", lhsXid, rhsXid)
			} else {
				require.Greater(lhs, rhs,
					"transaction %d committed after %d, but its token does not sort above", lhsXid, rhsXid)
			}
		}
	}
}

// testCursorWatchSameTokenFromAnyCursor asserts that a transaction is positioned
// identically whether it is delivered by a watch that was already running when
// it committed or by one that started afterwards from an older revision.
func testCursorWatchSameTokenFromAnyCursor(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, 1000*time.Second, 512, true)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	watchOptions := datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	}

	runningCtx, cancelRunning := context.WithCancel(ctx)
	defer cancelRunning()
	runningChanges, runningErrs := ds.Watch(runningCtx, headRevision.Revision, watchOptions)
	require.Empty(runningErrs)

	rel := tuple.MustParse("document:bothpaths#viewer@user:subject")
	writeRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
	})
	require.NoError(err)

	writtenXid, ok := writeRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	runningTokens := collectTokensByXid(t, runningChanges, runningErrs, setOfXids(writtenXid.Uint64))

	// A watcher starting from the older revision delivers the same transaction
	// out of the backfill.
	replayCtx, cancelReplay := context.WithCancel(ctx)
	defer cancelReplay()
	replayChanges, replayErrs := ds.Watch(replayCtx, headRevision.Revision, watchOptions)
	require.Empty(replayErrs)

	replayedTokens := collectTokensByXid(t, replayChanges, replayErrs, setOfXids(writtenXid.Uint64))

	require.Equal(runningTokens[writtenXid.Uint64], replayedTokens[writtenXid.Uint64],
		"the two cursors delivered transaction %d at different tokens", writtenXid.Uint64)
}

// testCursorWatchReconnectWithPositionedToken verifies the resume guarantee: a
// consumer that stops and reconnects with its last token receives exactly the
// remaining unseen changes, in order, at the very same tokens.
func testCursorWatchReconnectWithPositionedToken(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, 1000*time.Second, 512, true)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// All writes commit before either watch exists, so both are served from the
	// backfill: fully deterministic.
	const writeCount = 8
	writtenXidsInOrder := make([]uint64, 0, writeCount)
	for i := 0; i < writeCount; i++ {
		rel := tuple.MustParse(fmt.Sprintf("document:reconnect#viewer@user:step_%d", i))
		revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)

		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		writtenXidsInOrder = append(writtenXidsInOrder, txid.Uint64)
	}

	watchOptions := datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	}

	// The first consumer observes the writes, then "crashes" partway, with its
	// durable state being the token of the third change.
	firstCtx, cancelFirst := context.WithCancel(ctx)
	firstChanges, firstErrs := ds.Watch(firstCtx, headRevision.Revision, watchOptions)
	require.Empty(firstErrs)

	firstEvents := collectChangesUntilXids(t, firstChanges, firstErrs, setOfXids(writtenXidsInOrder...))

	firstChangeRevisions := make([]postgresRevision, 0, writeCount)
	for _, event := range firstEvents {
		if event.IsCheckpoint {
			continue
		}
		firstChangeRevisions = append(firstChangeRevisions, event.Revision.(postgresRevision))
	}
	require.Len(firstChangeRevisions, writeCount)

	// Delivery is in commit order, each transaction at its own recorded
	// position, which strictly ascends because the writes were sequential.
	var lastPosition uint64
	for index, revision := range firstChangeRevisions {
		txid, ok := revision.OptionalTransactionID()
		require.True(ok)
		require.Equal(writtenXidsInOrder[index], txid.Uint64, "delivery order must match write order")
		require.Greater(revision.optionalCommitLSN, lastPosition, "positions must strictly ascend")
		lastPosition = revision.optionalCommitLSN
	}

	const resumeAfter = 3
	resumeToken := firstChangeRevisions[resumeAfter-1].String()
	cancelFirst()

	// The second consumer resumes from that token, round-tripped through its
	// string form the way a real consumer would store it.
	parsedToken, err := ParseRevisionString(resumeToken)
	require.NoError(err)

	secondChanges, secondErrs := ds.Watch(ctx, parsedToken, watchOptions)
	require.Empty(secondErrs)

	secondEvents := collectChangesUntilXids(t, secondChanges, secondErrs, setOfXids(writtenXidsInOrder[resumeAfter:]...))

	secondChangeRevisions := make([]postgresRevision, 0, writeCount-resumeAfter)
	for _, event := range secondEvents {
		// Continuity: every event after the resume sorts strictly above the token.
		require.Greater(event.Revision.String(), resumeToken, "event does not sort above the resume token")
		if event.IsCheckpoint {
			continue
		}
		secondChangeRevisions = append(secondChangeRevisions, event.Revision.(postgresRevision))
	}

	// Exactly the unseen remainder arrives, in the same order, at the very same
	// tokens the first watch delivered them at: a resumed watch is not a new
	// epoch, because positions belong to transactions.
	require.Len(secondChangeRevisions, writeCount-resumeAfter)
	for index, revision := range secondChangeRevisions {
		txid, ok := revision.OptionalTransactionID()
		require.True(ok)
		require.Equal(writtenXidsInOrder[resumeAfter+index], txid.Uint64, "resumed delivery order must match write order")
		require.Equal(firstChangeRevisions[resumeAfter+index].String(), revision.String(),
			"transaction %d was re-delivered at a different token", txid.Uint64)
	}

	// And what commits afterwards continues sorting above everything before it.
	liveRel := tuple.MustParse("document:reconnect#viewer@user:live")
	revLive, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(liveRel)})
	})
	require.NoError(err)

	liveEvents := collectChangesUntilXids(t, secondChanges, secondErrs, revisionXids(t, revLive))
	_, liveEvent := requireChangeForSubject(t, liveEvents, "live")
	liveRevision := liveEvent.Revision.(postgresRevision)
	require.Greater(liveRevision.optionalCommitLSN, lastPosition)
	require.Greater(liveRevision.String(), secondChangeRevisions[len(secondChangeRevisions)-1].String())
}

// testCursorWatchConcurrentWatchers runs two watchers from the same revision.
// Each writes a marker transaction that the other can see, but no marker may be
// delivered as a change. Both must receive the same transactions exactly once, in
// strictly increasing position order.
func testCursorWatchConcurrentWatchers(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, 1000*time.Second, 512, true)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// With sequential writes, write order is commit order, so both watchers must
	// deliver write order regardless of where each backfill boundary falls.
	writtenXidsInOrder := make([]uint64, 0, 12)
	writeOne := func(index int) {
		rel := tuple.MustParse(fmt.Sprintf("document:duo#viewer@user:write_%d", index))
		revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)

		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		writtenXidsInOrder = append(writtenXidsInOrder, txid.Uint64)
	}

	for i := 0; i < 6; i++ {
		writeOne(i)
	}

	watchOptions := datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	}

	changesA, errsA := ds.Watch(ctx, headRevision.Revision, watchOptions)
	require.Empty(errsA)
	changesB, errsB := ds.Watch(ctx, headRevision.Revision, watchOptions)
	require.Empty(errsB)

	// More writes racing both watchers' handoffs.
	for i := 6; i < 12; i++ {
		writeOne(i)
	}

	awaited := setOfXids(writtenXidsInOrder...)

	for name, stream := range map[string]struct {
		changes <-chan datastore.RevisionChanges
		errs    <-chan error
	}{
		"A": {changesA, errsA},
		"B": {changesB, errsB},
	} {
		events := collectChangesUntilXids(t, stream.changes, stream.errs, awaited)

		observedXidsInOrder := make([]uint64, 0, len(writtenXidsInOrder))
		var lastChangeToken string
		for index, event := range events {
			revision, ok := event.Revision.(postgresRevision)
			require.True(ok)
			require.True(revision.ByteSortable(), "watcher %s: event %d is missing a position", name, index)

			if event.IsCheckpoint {
				continue
			}

			token := revision.String()
			require.Greater(token, lastChangeToken, "watcher %s: change %d does not sort strictly above the previous change", name, index)
			lastChangeToken = token

			txid, ok := revision.OptionalTransactionID()
			require.True(ok)
			observedXidsInOrder = append(observedXidsInOrder, txid.Uint64)
		}

		// The identical sequence, exactly once, and nothing else: in particular,
		// neither watcher's marker may surface as a change on the other's stream.
		require.Equal(writtenXidsInOrder, observedXidsInOrder, "watcher %s must observe the writes exactly once, in order, with no extras", name)
	}
}

// testCursorWatchDisconnectDuringBackfillLosesNothing verifies that a watch
// disconnected mid-backfill loses nothing: retrying from the same revision
// delivers every transaction exactly once, at the same tokens. The disconnect is
// forced deterministically with a one-slot buffer, a nanosecond write timeout,
// and a consumer that never reads.
func testCursorWatchDisconnectDuringBackfillLosesNothing(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, 1000*time.Second, 512, true)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	const writeCount = 5
	writtenXidsInOrder := make([]uint64, 0, writeCount)
	for i := 0; i < writeCount; i++ {
		rel := tuple.MustParse(fmt.Sprintf("document:backfillfail#viewer@user:write_%d", i))
		revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)

		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		writtenXidsInOrder = append(writtenXidsInOrder, txid.Uint64)
	}

	// This watch is expected to fail: a one-slot buffer, a nanosecond write
	// timeout, and no reader. It writes one event to the buffer, then
	// disconnects on the second.
	doomedChanges, doomedErrs := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:                 datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval:      100 * time.Millisecond,
		WatchBufferLength:       1,
		WatchBufferWriteTimeout: time.Nanosecond,
	})

	select {
	case err := <-doomedErrs:
		require.ErrorAs(err, &datastore.WatchDisconnectedError{})
	case <-time.After(cursorWatchTestTimeout):
		require.Fail("timed out waiting for the unread watch to be disconnected")
	}

	// At most the buffered prefix was delivered before the disconnect: exactly
	// the first change, and nothing after it.
	delivered := make([]datastore.RevisionChanges, 0, 1)
	for change := range doomedChanges {
		delivered = append(delivered, change)
	}
	require.Len(delivered, 1, "exactly the buffered prefix must have been delivered")
	prefixRevision := delivered[0].Revision.(postgresRevision)
	require.True(prefixRevision.ByteSortable())
	prefixXid, ok := prefixRevision.OptionalTransactionID()
	require.True(ok)
	require.Equal(writtenXidsInOrder[0], prefixXid.Uint64)

	// The retry from the same revision delivers everything exactly once: the
	// failed delivery lost nothing, and the one event that did get through
	// arrives at the same token it had before.
	retryChanges, retryErrs := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(retryErrs)

	events := collectChangesUntilXids(t, retryChanges, retryErrs, setOfXids(writtenXidsInOrder...))

	observedXidsInOrder := make([]uint64, 0, writeCount)
	var lastPosition uint64
	for _, event := range events {
		revision := event.Revision.(postgresRevision)
		require.True(revision.ByteSortable())
		if event.IsCheckpoint {
			continue
		}

		require.GreaterOrEqual(revision.optionalCommitLSN, lastPosition, "positions must ascend")
		lastPosition = revision.optionalCommitLSN

		txid, ok := revision.OptionalTransactionID()
		require.True(ok)
		observedXidsInOrder = append(observedXidsInOrder, txid.Uint64)

		if txid.Uint64 == prefixXid.Uint64 {
			require.Equal(prefixRevision.String(), revision.String(),
				"the re-delivered event must carry the token it was first delivered at")
		}
	}

	require.Equal(writtenXidsInOrder, observedXidsInOrder, "the retry must deliver every write exactly once, in order, with no extras")
}

// testCursorWatchLegacyTokenHandoff covers the transition from the backfill
// phase to the cursor loop for a consumer resuming from a snapshot-only token.
//
// It runs with the ledger deliberately stalled while the writes commit, so that
// at the moment the watch starts none of them have a recorded position. That is
// the case a fast local ledger never exercises, and the one the handoff's
// accounting turns on: without the frontier probe, an unrecorded transaction
// visible in the backfill snapshot would be skipped as already-seen and then
// delivered again by the loop once the ledger caught up.
func testCursorWatchLegacyTokenHandoff(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// A long retry interval keeps the ledger down until the test is ready.
	ds, dbURI := newCursorWatchTestDatastore(t, b,
		LogicalWatchLedgerRetryInterval(3*time.Second),
	)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)
	legacyToken := headRevision.Revision.(postgresRevision)
	require.False(legacyToken.ByteSortable(), "a HeadRevision carries no position")

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	pgds, ok := ds.(*pgDatastore)
	require.True(ok)
	slotName := pgds.ledgerSlotName

	// Wait for the ledger to attach, then evict it so the writes below commit
	// while nothing is recording.
	requireLedgerAttached(t, ctx, conn, slotName, true)
	_, err = conn.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL;", slotName)
	require.NoError(err)
	requireLedgerAttached(t, ctx, conn, slotName, false)

	const writeCount = 3
	writtenXidsInOrder := make([]uint64, 0, writeCount)
	for i := 0; i < writeCount; i++ {
		rel := tuple.MustParse(fmt.Sprintf("document:handoff#viewer@user:unrecorded_%d", i))
		revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)

		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		writtenXidsInOrder = append(writtenXidsInOrder, txid.Uint64)
	}

	// None of them has a recorded position yet, which is the state the handoff
	// has to survive.
	recorded := recordedCommitLSNs(t, ctx, conn)
	for _, xid := range writtenXidsInOrder {
		require.NotContains(recorded, xid, "the ledger was expected to be stalled")
	}

	// The watch blocks on its frontier probe until the ledger returns, then
	// delivers. Everything must arrive exactly once, in order, positioned.
	changes, errchan := ds.Watch(ctx, legacyToken, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	events := collectChangesUntilXids(t, changes, errchan, setOfXids(writtenXidsInOrder...))

	backfilled := make([]uint64, 0, writeCount)
	for _, event := range events {
		revision := event.Revision.(postgresRevision)
		require.True(revision.ByteSortable(), "the backfill must position transactions the ledger recorded")
		if event.IsCheckpoint {
			continue
		}
		txid, ok := revision.OptionalTransactionID()
		require.True(ok)
		backfilled = append(backfilled, txid.Uint64)
	}
	require.Equal(writtenXidsInOrder, backfilled, "the backfill must deliver every write exactly once, in order")

	// And the loop takes over cleanly: a write after the handoff is delivered
	// once, above everything the backfill emitted, with nothing re-delivered.
	rel := tuple.MustParse("document:handoff#viewer@user:after_handoff")
	afterRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
	})
	require.NoError(err)

	afterXid, ok := afterRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	afterEvents := collectChangesUntilXids(t, changes, errchan, setOfXids(afterXid.Uint64))
	for _, event := range afterEvents {
		if event.IsCheckpoint {
			continue
		}
		txid, ok := event.Revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		require.Equal(afterXid.Uint64, txid.Uint64,
			"transaction %d was delivered twice across the backfill/loop handoff", txid.Uint64)
	}
}

// testCursorWatchCheckpointsAreExactOnResume asserts the checkpoint contract:
// resuming from a checkpoint token delivers the transactions above it and
// nothing else, neither losing nor repeating. A small batch size produces
// several checkpoints so the resume happens mid-stream.
func testCursorWatchCheckpointsAreExactOnResume(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const batchSize = 2
	const writeCount = 7

	ds, _ := newCursorWatchTestDatastore(t, b, WatchBatchSize(batchSize))

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	writtenXidsInOrder := make([]uint64, 0, writeCount)
	for i := 0; i < writeCount; i++ {
		rel := tuple.MustParse(fmt.Sprintf("document:exact#viewer@user:write_%d", i))
		revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)

		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		writtenXidsInOrder = append(writtenXidsInOrder, txid.Uint64)
	}

	watchOptions := datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	}

	firstCtx, cancelFirst := context.WithCancel(ctx)
	firstChanges, firstErrs := ds.Watch(firstCtx, headRevision.Revision, watchOptions)
	require.Empty(firstErrs)

	events := collectChangesUntilXids(t, firstChanges, firstErrs, setOfXids(writtenXidsInOrder...))

	// Batched delivery: write order, each event positioned, with a checkpoint
	// after each batch rather than only at the end.
	observedXidsInOrder := make([]uint64, 0, writeCount)
	var checkpointTokens []string
	var lastPosition uint64
	firstCheckpointIndex := -1
	for index, event := range events {
		revision, ok := event.Revision.(postgresRevision)
		require.True(ok)
		require.True(revision.ByteSortable(), "event %d carries no commit position", index)
		require.GreaterOrEqual(revision.optionalCommitLSN, lastPosition, "positions must ascend across batches")
		lastPosition = revision.optionalCommitLSN

		if event.IsCheckpoint {
			if firstCheckpointIndex < 0 {
				firstCheckpointIndex = index
			}
			checkpointTokens = append(checkpointTokens, revision.String())
			continue
		}

		txid, ok := revision.OptionalTransactionID()
		require.True(ok)
		observedXidsInOrder = append(observedXidsInOrder, txid.Uint64)
	}
	require.Equal(writtenXidsInOrder, observedXidsInOrder, "delivery must cover every write exactly once, in order")
	require.Equal(batchSize, firstCheckpointIndex, "the first batch's checkpoint must directly follow its changes")
	require.GreaterOrEqual(len(checkpointTokens), writeCount/batchSize, "expected a checkpoint per completed batch")
	cancelFirst()

	// A consumer that persisted the first checkpoint resumes from it and
	// receives exactly the writes above it: no loss, no repetition.
	resumeToken, err := ParseRevisionString(checkpointTokens[0])
	require.NoError(err)

	secondChanges, secondErrs := ds.Watch(ctx, resumeToken, watchOptions)
	require.Empty(secondErrs)

	resumedEvents := collectChangesUntilXids(t, secondChanges, secondErrs, setOfXids(writtenXidsInOrder[batchSize:]...))

	resumedXidsInOrder := make([]uint64, 0, writeCount-batchSize)
	for _, event := range resumedEvents {
		require.Greater(event.Revision.String(), checkpointTokens[0], "resumed events must sort above the resume token")
		if event.IsCheckpoint {
			continue
		}
		txid, ok := event.Revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		resumedXidsInOrder = append(resumedXidsInOrder, txid.Uint64)
	}
	require.Equal(writtenXidsInOrder[batchSize:], resumedXidsInOrder,
		"resuming from a checkpoint must deliver exactly the transactions above it")
}

// testCursorWatchBacklogDrainsWithoutSleeping asserts that a backlog larger than
// one batch is delivered in consecutive full batches rather than one batch per
// poll interval. The poll interval is set far longer than the test's patience, so
// only a draining implementation can finish in time.
func testCursorWatchBacklogDrainsWithoutSleeping(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const batchSize = 2
	const backlogCount = 7
	const pollInterval = 5 * time.Second

	ds, _ := newCursorWatchTestDatastore(t, b,
		WatchBatchSize(batchSize),
		WatchPollInterval(pollInterval),
	)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	watchOptions := datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	}

	// A first watch yields a positioned token, so the backlog below is drained
	// by the cursor loop rather than by the backfill phase.
	seedCtx, cancelSeed := context.WithCancel(ctx)
	seedChanges, seedErrs := ds.Watch(seedCtx, headRevision.Revision, watchOptions)
	require.Empty(seedErrs)

	seedRel := tuple.MustParse("document:backlog#viewer@user:seed")
	seedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(seedRel)})
	})
	require.NoError(err)

	seedEvents := collectChangesUntilXids(t, seedChanges, seedErrs, revisionXids(t, seedRevision))
	_, seedEvent := requireChangeForSubject(t, seedEvents, "seed")
	positionedToken := seedEvent.Revision.(postgresRevision)
	require.True(positionedToken.ByteSortable())
	cancelSeed()

	backlogXidsInOrder := make([]uint64, 0, backlogCount)
	for i := 0; i < backlogCount; i++ {
		rel := tuple.MustParse(fmt.Sprintf("document:backlog#viewer@user:item_%d", i))
		revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)

		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		backlogXidsInOrder = append(backlogXidsInOrder, txid.Uint64)
	}

	// Wait until the ledger has recorded the whole backlog and confirmed past
	// it, so the frontier already covers all of it when the watch starts and the
	// first poll has the entire backlog available to drain.
	pgds, ok := ds.(*pgDatastore)
	require.True(ok)
	lastBacklogXid := NewXid8(backlogXidsInOrder[len(backlogXidsInOrder)-1])
	require.EventuallyWithT(func(collect *assert.CollectT) {
		var lastPositionText *string
		if !assert.NoError(collect, pgds.readPool.QueryRow(ctx, commitLSNForXidQuery, lastBacklogXid).Scan(&lastPositionText)) {
			return
		}
		if !assert.NotNil(collect, lastPositionText, "the last backlog transaction has no recorded position yet") {
			return
		}

		lastPosition, err := pglogrepl.ParseLSN(*lastPositionText)
		if !assert.NoError(collect, err) {
			return
		}
		state, err := pgds.readLedgerSlotState(ctx)
		if !assert.NoError(collect, err) {
			return
		}
		assert.GreaterOrEqual(collect, state.confirmed, lastPosition, "the frontier has not reached the end of the backlog")
	}, cursorWatchTestTimeout, 20*time.Millisecond)

	changes, errchan := ds.Watch(ctx, positionedToken, watchOptions)
	require.Empty(errchan)

	// The backlog spans four batches. Delivering it within a fraction of one
	// poll interval is only possible without sleeping between full batches.
	startedAt := time.Now()
	events := collectChangesUntilXids(t, changes, errchan, setOfXids(backlogXidsInOrder...))
	elapsed := time.Since(startedAt)
	require.Less(elapsed, pollInterval, "the backlog was drained one batch per poll interval instead of consecutively")

	observed := make([]uint64, 0, backlogCount)
	for _, event := range events {
		if event.IsCheckpoint {
			continue
		}
		txid, ok := event.Revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		observed = append(observed, txid.Uint64)
	}
	require.Equal(backlogXidsInOrder, observed, "the drained backlog must arrive exactly once, in order")
}

// testCursorWatchIdleCheckpointsFollowTheFrontier covers the idle path: with no
// SpiceDB writes but other WAL activity, the ledger confirms its progress
// anyway, and the watch turns that into checkpoints that advance.
//
// It also pins what such a checkpoint's snapshot must be. Delivery is complete
// only through the frontier, so an idle checkpoint carries the last delivered
// transaction's snapshot. Using a fresh pg_current_snapshot() would cover
// transactions above the frontier that have NOT been delivered, and feeding that
// token to any snapshot-filtered consumer would drop them silently.
func testCursorWatchIdleCheckpointsFollowTheFrontier(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// A one-second status interval bounds how long the ledger holds its
	// confirmed position back from the slot.
	ds, dbURI := newCursorWatchTestDatastore(t, b, LogicalWatchStatusInterval(time.Second))

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	// One write, so there is a "last delivered transaction" to compare against.
	rel := tuple.MustParse("document:idle#viewer@user:only_write")
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
	})
	require.NoError(err)

	writtenXid, ok := revision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	events := collectChangesUntilXids(t, changes, errchan, setOfXids(writtenXid.Uint64))

	var deliveredCheckpoint postgresRevision
	for _, event := range events {
		if event.IsCheckpoint {
			deliveredCheckpoint = event.Revision.(postgresRevision)
		}
	}
	if !deliveredCheckpoint.ByteSortable() {
		// The batch's checkpoint trails its changes; wait for it.
		deliveredCheckpoint = awaitCheckpoint(t, changes, errchan, 0)
	}

	// WAL activity that has nothing to do with SpiceDB: it produces no watch
	// event, and nothing for the ledger to record, but it does move the server's
	// WAL position, which the ledger may confirm and the watch may checkpoint.
	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, "CREATE TABLE unrelated_wal (id BIGSERIAL PRIMARY KEY, payload TEXT);")
	require.NoError(err)

	// The generator gets its own connection: a pgx connection is not safe for
	// concurrent use, and sharing one here would race this test's teardown
	// against an insert still in flight.
	generateCtx, stopGenerating := context.WithCancel(ctx)
	var generating sync.WaitGroup
	generating.Add(1)
	go func() {
		defer generating.Done()

		generateConn, err := pgx.Connect(generateCtx, dbURI)
		if err != nil {
			return
		}
		defer func() { _ = generateConn.Close(context.Background()) }()

		for generateCtx.Err() == nil {
			_, _ = generateConn.Exec(generateCtx, "INSERT INTO unrelated_wal (payload) SELECT repeat('x', 1000) FROM generate_series(1, 50);")
			time.Sleep(50 * time.Millisecond)
		}
	}()
	defer func() {
		stopGenerating()
		generating.Wait()
	}()

	// An idle checkpoint above the delivered one, carrying its snapshot
	// unchanged, and no change event in between.
	idleCheckpoint := awaitCheckpoint(t, changes, errchan, deliveredCheckpoint.optionalCommitLSN)
	stopGenerating()

	require.Greater(idleCheckpoint.optionalCommitLSN, deliveredCheckpoint.optionalCommitLSN,
		"an idle checkpoint must advance with the frontier")
	require.Equal(deliveredCheckpoint.snapshot, idleCheckpoint.snapshot,
		"an idle checkpoint must carry the last delivered transaction's snapshot, not a fresh one")

	// The honesty check: a consumer that resumes from the idle checkpoint still
	// receives everything committed after it.
	nextRel := tuple.MustParse("document:idle#viewer@user:after_idle")
	nextRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(nextRel)})
	})
	require.NoError(err)

	resumeChanges, resumeErrs := ds.Watch(ctx, idleCheckpoint, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(resumeErrs)

	resumed := collectChangesUntilXids(t, resumeChanges, resumeErrs, revisionXids(t, nextRevision))
	_, _ = requireChangeForSubject(t, resumed, "after_idle")
}

// awaitCheckpoint reads until a checkpoint above the given position arrives,
// requiring that no change event arrives before it.
func awaitCheckpoint(t *testing.T, changes <-chan datastore.RevisionChanges, errchan <-chan error, above uint64) postgresRevision {
	t.Helper()

	timeout := time.After(cursorWatchTestTimeout)
	for {
		select {
		case change, ok := <-changes:
			require.True(t, ok, "the watch closed while waiting for a checkpoint")
			revision, ok := change.Revision.(postgresRevision)
			require.True(t, ok)
			require.True(t, change.IsCheckpoint, "an unexpected change event arrived while idle: %v", change)
			if revision.optionalCommitLSN > above {
				return revision
			}
		case err := <-errchan:
			require.NoError(t, err, "unexpected watch error")
		case <-timeout:
			require.Fail(t, "timed out waiting for a checkpoint to advance")
		}
	}
}

// testCursorWatchStaleRevisionRejected asserts that a consumer resuming from a
// revision older than the garbage collection window is told so, rather than
// served a silently truncated stream. Both existing watches do the latter.
//
// The window is set to a millisecond, so a token delivered a moment ago already
// names a collectable transaction.
func testCursorWatchStaleRevisionRejected(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, time.Millisecond, 128, true)

	watchOptions := datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	}

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	// A HeadRevision names no transaction, so it is served: there is nothing to
	// have fallen behind of.
	seedCtx, cancelSeed := context.WithCancel(ctx)
	seedChanges, seedErrs := ds.Watch(seedCtx, headRevision.Revision, watchOptions)
	require.Empty(seedErrs)

	seedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:stale#viewer@user:old"))})
	})
	require.NoError(err)

	seedEvents := collectChangesUntilXids(t, seedChanges, seedErrs, revisionXids(t, seedRevision))
	_, seedEvent := requireChangeForSubject(t, seedEvents, "old")
	staleToken := seedEvent.Revision.(postgresRevision)
	cancelSeed()

	// Past the window.
	time.Sleep(20 * time.Millisecond)

	changes, errchan := ds.Watch(ctx, staleToken, watchOptions)

	select {
	case err := <-errchan:
		var invalid datastore.InvalidRevisionError
		require.ErrorAs(err, &invalid)
		require.Equal(datastore.RevisionStale, invalid.Reason())
	case change := <-changes:
		require.Fail("the watch delivered an event from a revision outside the retained window", "%v", change)
	case <-time.After(cursorWatchTestTimeout):
		require.Fail("the watch neither failed nor delivered for a stale revision")
	}
}

// testCursorWatchGapBelowCursorFailsLoudly asserts that a recorded gap above a
// watch's cursor fails the watch instead of being stepped over.
//
// This is the hazard the cursor design introduces and the gap table exists to
// close: transactions that committed while the ledger's slot was invalid have no
// recorded position, so they match neither the cursor's lower bound nor the
// frontier's upper bound, and nothing else would notice their absence.
func testCursorWatchGapBelowCursorFailsLoudly(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds, dbURI := newCursorWatchTestDatastore(t, b)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	watchOptions := datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	}

	// A positioned token to resume from.
	seedCtx, cancelSeed := context.WithCancel(ctx)
	seedChanges, seedErrs := ds.Watch(seedCtx, headRevision.Revision, watchOptions)
	require.Empty(seedErrs)

	seedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:gapguard#viewer@user:seed"))})
	})
	require.NoError(err)

	seedEvents := collectChangesUntilXids(t, seedChanges, seedErrs, revisionXids(t, seedRevision))
	_, seedEvent := requireChangeForSubject(t, seedEvents, "seed")
	positionedToken := seedEvent.Revision.(postgresRevision)
	cancelSeed()

	// Record a gap starting at that position: from here on, this consumer
	// cannot be shown to have received everything.
	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	gapFrom := pglogrepl.LSN(positionedToken.optionalCommitLSN)
	_, err = conn.Exec(ctx,
		"INSERT INTO ledger_gap (from_lsn, to_lsn) VALUES ($1::pg_lsn, $2::pg_lsn);",
		gapFrom.String(), (gapFrom + 0x1000).String())
	require.NoError(err)

	// Writes that a gap-blind implementation would happily deliver.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:gapguard#viewer@user:across_the_gap"))})
	})
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, positionedToken, watchOptions)

	select {
	case err := <-errchan:
		require.ErrorContains(err, "no recorded commit position")
		require.ErrorContains(err, "restart from a current revision")
	case change := <-changes:
		require.Fail("the watch delivered an event across a recorded gap", "%v", change)
	case <-time.After(cursorWatchTestTimeout):
		require.Fail("the watch neither failed nor delivered across a recorded gap")
	}
}

// newPreLedgerHistory creates a database whose history predates the commit LSN
// ledger: the writes happen with the cursor watch disabled, so they commit with
// no recorded position, exactly as a database adopting the feature would have.
// It returns the database URI and the transaction ids of that history, oldest
// first.
func newPreLedgerHistory(t *testing.T, b testdatastore.RunningEngineForTest, subjects ...string) (string, []uint64) {
	t.Helper()
	require := require.New(t)

	var dbURI string
	preLedgerDS := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		dbURI = uri
		ds, err := newPostgresDatastore(t.Context(), uri, primaryInstanceID,
			RevisionQuantization(0),
			GCWindow(1000*time.Second),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(512),
			WithRevisionHeartbeat(false),
			WithLogicalWatch(false),
		)
		require.NoError(err)
		return ds
	})

	xids := make([]uint64, 0, len(subjects))
	for _, subject := range subjects {
		revision, err := preLedgerDS.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(tuple.MustParse("document:pre#viewer@user:" + subject)),
			})
		})
		require.NoError(err)
		xid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		xids = append(xids, xid.Uint64)
	}
	require.NoError(preLedgerDS.Close())

	return dbURI, xids
}

// enableCursorWatchOn brings the cursor watch up against an existing database,
// which is what provisions the ledger for the first time.
func enableCursorWatchOn(t *testing.T, dbURI string, options ...Option) datastore.Datastore {
	t.Helper()

	allOptions := append([]Option{
		RevisionQuantization(0),
		GCWindow(1000 * time.Second),
		GCInterval(veryLargeGCInterval),
		WatchBufferLength(512),
		WithRevisionHeartbeat(false),
		WithLogicalWatch(true),
	}, options...)

	ds, err := newPostgresDatastore(t.Context(), dbURI, primaryInstanceID, allOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

// testPreLedgerPositionsAreBackfilled asserts that history predating the ledger
// is given positions once the ledger is provisioned, so that a consumer resuming
// across it compares tokens rather than meeting an unpositioned prefix.
//
// The positions must carry commit order and must sort below everything the
// ledger records itself, which is what makes them safe to mix with real ones.
func testPreLedgerPositionsAreBackfilled(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dbURI, preXids := newPreLedgerHistory(t, b, "first", "second", "third")

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	require.NotContains(recordedCommitLSNs(t, ctx, conn), preXids[0],
		"history written before the ledger must start out unpositioned")

	ds := enableCursorWatchOn(t, dbURI, LogicalWatchLedgerRetryInterval(100*time.Millisecond))

	// The backfill rides on the ledger's flushes, which writes drive.
	var recorded map[uint64]string
	require.EventuallyWithT(func(collect *assert.CollectT) {
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(tuple.MustParse("document:pre#viewer@user:driver")),
			})
		})
		assert.NoError(collect, err)

		recorded = recordedCommitLSNs(t, ctx, conn)
		for _, xid := range preXids {
			assert.Contains(collect, recorded, xid, "pre-ledger history was not backfilled")
		}
	}, cursorWatchTestTimeout, 100*time.Millisecond)

	// Commit order, oldest lowest.
	positions := make([]pglogrepl.LSN, 0, len(preXids))
	for _, xid := range preXids {
		lsn, err := pglogrepl.ParseLSN(recorded[xid])
		require.NoError(err)
		positions = append(positions, lsn)
	}
	for i := 1; i < len(positions); i++ {
		require.Less(positions[i-1], positions[i],
			"backfilled positions must follow commit order, oldest lowest")
	}

	// And below everything the ledger records for itself.
	liveRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("document:pre#viewer@user:live")),
		})
	})
	require.NoError(err)
	liveXid, ok := liveRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	var liveLSN pglogrepl.LSN
	require.EventuallyWithT(func(collect *assert.CollectT) {
		text, found := recordedCommitLSNs(t, ctx, conn)[liveXid.Uint64]
		if !assert.True(collect, found) {
			return
		}
		parsed, err := pglogrepl.ParseLSN(text)
		assert.NoError(collect, err)
		liveLSN = parsed
	}, cursorWatchTestTimeout, 50*time.Millisecond)

	require.Less(positions[len(positions)-1], liveLSN,
		"a backfilled position must sort below what the ledger records")

	// Re-running must not move a position already handed out.
	before := recordedCommitLSNs(t, ctx, conn)
	for range 3 {
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(tuple.MustParse("document:pre#viewer@user:again")),
			})
		})
		require.NoError(err)
	}
	after := recordedCommitLSNs(t, ctx, conn)
	for _, xid := range preXids {
		require.Equal(before[xid], after[xid], "a backfilled position must be stable")
	}
}

// testPreLedgerBackfillStopsAtGCWindow asserts that history older than the
// collection horizon is left unpositioned. Such a revision is already refused as
// stale, so it can never be resumed from, and positioning it would only invite
// comparisons against transactions that may already be collected.
func testPreLedgerBackfillStopsAtGCWindow(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	dbURI, preXids := newPreLedgerHistory(t, b, "stale")

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	// A collection horizon this short puts the existing history behind it.
	ds := enableCursorWatchOn(t, dbURI,
		GCWindow(time.Nanosecond),
		LogicalWatchLedgerRetryInterval(100*time.Millisecond),
	)

	liveRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("document:pre#viewer@user:live")),
		})
	})
	require.NoError(err)
	liveXid, ok := liveRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	// Once the ledger has recorded a live write it has had its chance to backfill.
	require.EventuallyWithT(func(collect *assert.CollectT) {
		assert.Contains(collect, recordedCommitLSNs(t, ctx, conn), liveXid.Uint64)
	}, cursorWatchTestTimeout, 50*time.Millisecond)

	require.NotContains(recordedCommitLSNs(t, ctx, conn), preXids[0],
		"history behind the collection horizon must be left unpositioned")
}

// testLedgerGapIsReplayedFromTheTables asserts that transactions a slot
// recreation swallowed are replayed out of the tables rather than failing every
// watch below them, and that a replay carries removals as well as additions.
//
// A gap loses commit positions, not changes: the rows are still there, and a
// removal is a soft delete that survives until collection. So the window can be
// given positions after the fact, and the watch delivers it as ordinary
// changes.
func testLedgerGapIsReplayedFromTheTables(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds, dbURI := newCursorWatchTestDatastore(t, b,
		LogicalWatchLedgerRetryInterval(100*time.Millisecond),
	)

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	pgds, ok := ds.(*pgDatastore)
	require.True(ok)
	slotName := pgds.ledgerSlotName

	// Established and recorded before the outage, so the gap has a sound lower
	// bound and there is something for the outage to remove.
	seeded := tuple.MustParse("document:heal#viewer@user:removed")
	beforeRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(seeded)})
	})
	require.NoError(err)

	beforeXid, ok := beforeRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)
	require.EventuallyWithT(func(collect *assert.CollectT) {
		assert.Contains(collect, recordedCommitLSNs(t, ctx, conn), beforeXid.Uint64)
	}, cursorWatchTestTimeout, 20*time.Millisecond)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)
	requireLedgerAttached(t, ctx, conn, slotName, true)

	// Drop the slot so the ledger stops recording, then commit through the
	// outage: one addition and one removal, the removal being the case a
	// re-assertion could never recover.
	_, err = conn.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL;", slotName)
	require.NoError(err)
	_, err = conn.Exec(ctx, "SELECT pg_drop_replication_slot($1);", slotName)
	require.NoError(err)

	added := tuple.MustParse("document:heal#viewer@user:added")
	addedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(added)})
	})
	require.NoError(err)

	removedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Delete(seeded)})
	})
	require.NoError(err)

	addedXid, ok := addedRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)
	removedXid, ok := removedRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	// The gap has to be recorded before it can be replayed; asserting on its
	// absence first would pass before it was ever opened.
	require.EventuallyWithT(func(collect *assert.CollectT) {
		var gapCount int
		assert.NoError(collect, conn.QueryRow(ctx, "SELECT count(*) FROM ledger_gap;").Scan(&gapCount))
		assert.NotZero(collect, gapCount, "the dropped slot must be recorded as a gap")
	}, cursorWatchTestTimeout, 50*time.Millisecond)

	// The replay runs once the re-provisioned ledger has confirmed past the
	// outage, which the writes in this loop drive.
	require.EventuallyWithT(func(collect *assert.CollectT) {
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:heal#viewer@user:after"))})
		})
		assert.NoError(collect, err)

		recorded := recordedCommitLSNs(t, ctx, conn)
		assert.Contains(collect, recorded, addedXid.Uint64, "the addition the outage swallowed was not replayed")
		assert.Contains(collect, recorded, removedXid.Uint64, "the removal the outage swallowed was not replayed")
	}, cursorWatchTestTimeout, 100*time.Millisecond)

	// With every swallowed transaction positioned, the gap is retired and a
	// watch from before the outage resumes instead of failing across it.
	var gapCount int
	require.NoError(conn.QueryRow(ctx, "SELECT count(*) FROM ledger_gap;").Scan(&gapCount))
	require.Zero(gapCount, "the gap must be retired once its transactions are replayed")

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	events := collectChangesUntilXids(t, changes, errchan, revisionXids(t, removedRevision))
	_, addedEvent := requireChangeForSubject(t, events, "added")
	require.True(addedEvent.Revision.(postgresRevision).ByteSortable(),
		"a replayed transaction must be delivered with a position")

	// The removal is the point of replaying rather than re-asserting.
	_, removedEvent := requireChangeForSubject(t, events, "removed")
	require.True(removedEvent.Revision.(postgresRevision).ByteSortable())

	// Replayed positions land inside the gap's interval, so they order below
	// everything the recreated slot went on to record.
	lastRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:heal#viewer@user:last"))})
	})
	require.NoError(err)
	lastXid, ok := lastRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	var lastLSN pglogrepl.LSN
	require.EventuallyWithT(func(collect *assert.CollectT) {
		text, found := recordedCommitLSNs(t, ctx, conn)[lastXid.Uint64]
		if !assert.True(collect, found, "the ledger did not record the write following the replay") {
			return
		}
		parsed, err := pglogrepl.ParseLSN(text)
		assert.NoError(collect, err)
		lastLSN = parsed
	}, cursorWatchTestTimeout, 50*time.Millisecond)

	require.Less(
		removedEvent.Revision.(postgresRevision).optionalCommitLSN, uint64(lastLSN),
		"a replayed position must sort below what the recreated slot records",
	)
}

// testLedgerGapRecordedOnOperatorDrop asserts that a replication slot dropped
// out from under the ledger is recorded as a gap and that recording resumes
// afterwards.
//
// The gap has to be recorded here because after the slot is recreated its
// confirmed position starts past the WAL that was skipped, so the frontier jumps
// over the affected transactions and no later observation can tell they are
// missing. An invalidated slot (wal_status = 'lost') takes the same path.
func testLedgerGapRecordedOnOperatorDrop(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds, dbURI := newCursorWatchTestDatastore(t, b,
		LogicalWatchLedgerRetryInterval(100*time.Millisecond),
	)

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	pgds, ok := ds.(*pgDatastore)
	require.True(ok)
	slotName := pgds.ledgerSlotName

	// A write that is recorded before the slot is dropped, so the gap's start
	// has a sound lower bound to be derived from.
	beforeRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:drop#viewer@user:before"))})
	})
	require.NoError(err)

	beforeXid, ok := beforeRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)
	require.EventuallyWithT(func(collect *assert.CollectT) {
		assert.Contains(collect, recordedCommitLSNs(t, ctx, conn), beforeXid.Uint64)
	}, cursorWatchTestTimeout, 20*time.Millisecond)

	requireLedgerAttached(t, ctx, conn, slotName, true)

	// The operator drops the slot. Its confirmed position dies with it, so the
	// ledger has to bound the gap by what it is known to have recorded.
	_, err = conn.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL;", slotName)
	require.NoError(err)
	require.EventuallyWithT(func(collect *assert.CollectT) {
		_, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot($1);", slotName)
		assert.NoError(collect, err)
	}, cursorWatchTestTimeout, 50*time.Millisecond)

	// The ledger re-provisions the slot, records the gap, and resumes.
	var gapFrom, gapTo string
	require.EventuallyWithT(func(collect *assert.CollectT) {
		err := conn.QueryRow(ctx, "SELECT from_lsn::text, to_lsn::text FROM ledger_gap ORDER BY from_lsn LIMIT 1;").Scan(&gapFrom, &gapTo)
		assert.NoError(collect, err, "no gap was recorded for the dropped slot")
	}, cursorWatchTestTimeout, 50*time.Millisecond)

	fromLSN, err := pglogrepl.ParseLSN(gapFrom)
	require.NoError(err)
	toLSN, err := pglogrepl.ParseLSN(gapTo)
	require.NoError(err)
	require.Less(fromLSN, toLSN, "a recorded gap must be bounded once the slot is back")
	require.NotEqual(ledgerGapPendingToLSN, gapTo, "the pending sentinel must be replaced once the recreation completes")

	// Recording resumes for what commits afterwards.
	afterRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:drop#viewer@user:after"))})
	})
	require.NoError(err)

	afterXid, ok := afterRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)
	require.EventuallyWithT(func(collect *assert.CollectT) {
		assert.Contains(collect, recordedCommitLSNs(t, ctx, conn), afterXid.Uint64,
			"recording did not resume after the slot was re-provisioned")
	}, cursorWatchTestTimeout, 20*time.Millisecond)

	// Nothing committed while the slot was gone, so the recorded interval
	// swallowed no transactions and is replayed away as an empty window. A
	// consumer positioned below it then resumes: there is nothing it missed.
	// The loud failure is asserted where a gap cannot be replayed, in
	// testGapBelowCursorFailsLoudly.
	require.EventuallyWithT(func(collect *assert.CollectT) {
		var gapCount int
		assert.NoError(collect, conn.QueryRow(ctx, "SELECT count(*) FROM ledger_gap;").Scan(&gapCount))
		assert.Zero(collect, gapCount, "an empty gap must be replayed away once the ledger passes it")
	}, cursorWatchTestTimeout, 100*time.Millisecond)

	staleCursor := postgresRevision{
		snapshot:          beforeRevision.(postgresRevision).snapshot,
		optionalCommitLSN: uint64(fromLSN),
	}
	changes, errchan := ds.Watch(ctx, staleCursor, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	events := collectChangesUntilXids(t, changes, errchan, revisionXids(t, afterRevision))
	_, _ = requireChangeForSubject(t, events, "after")
}

// testCursorWatchLedgerWithoutWriterFails asserts that a watch whose frontier
// has stopped moving because nothing is recording fails with the state an
// operator acts on, rather than stalling indistinguishably from "no writes".
func testCursorWatchLedgerWithoutWriterFails(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// A long retry interval keeps the ledger from coming back, and a short wait
	// timeout bounds how long the watch tolerates that.
	ds, dbURI := newCursorWatchTestDatastore(t, b,
		LogicalWatchLedgerRetryInterval(time.Minute),
		LogicalWatchLedgerWaitTimeout(time.Second),
	)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	watchOptions := datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	}

	// A positioned token, so the failing watch below is exercising the loop's
	// liveness guard rather than the starting probe.
	seedCtx, cancelSeed := context.WithCancel(ctx)
	seedChanges, seedErrs := ds.Watch(seedCtx, headRevision.Revision, watchOptions)
	require.Empty(seedErrs)

	seedRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:nowriter#viewer@user:seed"))})
	})
	require.NoError(err)

	seedEvents := collectChangesUntilXids(t, seedChanges, seedErrs, revisionXids(t, seedRevision))
	_, seedEvent := requireChangeForSubject(t, seedEvents, "seed")
	positionedToken := seedEvent.Revision.(postgresRevision)
	cancelSeed()

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	pgds, ok := ds.(*pgDatastore)
	require.True(ok)

	_, err = conn.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL;", pgds.ledgerSlotName)
	require.NoError(err)
	requireLedgerAttached(t, ctx, conn, pgds.ledgerSlotName, false)

	changes, errchan := ds.Watch(ctx, positionedToken, watchOptions)

	// Checkpoints up to the frontier the ledger confirmed before it died are
	// legitimate: delivery really is complete through them. What must not happen
	// is the watch sitting there indefinitely, or inventing a change.
	timeout := time.After(cursorWatchTestTimeout)
	for {
		select {
		case err := <-errchan:
			require.ErrorContains(err, "no writer")
			require.ErrorContains(err, pgds.ledgerSlotName)
			return
		case change := <-changes:
			require.True(change.IsCheckpoint, "the watch delivered a change with nothing recording: %v", change)
		case <-timeout:
			require.Fail("the watch neither failed nor delivered while the ledger had no writer")
		}
	}
}

// requireLedgerAttached waits until the ledger slot is (or is no longer)
// attached to a consumer.
func requireLedgerAttached(t *testing.T, ctx context.Context, conn *pgx.Conn, slotName string, attached bool) {
	t.Helper()

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var active bool
		if !assert.NoError(collect, conn.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name = $1;", slotName).Scan(&active)) {
			return
		}
		assert.Equal(collect, attached, active)
	}, cursorWatchTestTimeout, 20*time.Millisecond)
}

// testLedgerFrontierWaitTimeout asserts that a watch whose positions cannot be
// trusted fails, loudly and quickly, rather than delivering transactions at
// unknown positions or hanging forever. Dropping the ledger's publication is one
// way to stall recording: on PostgreSQL 18 the walsender warns and keeps
// streaming, so the ledger stays attached but never decodes anything again.
func testLedgerFrontierWaitTimeout(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds, dbURI := newCursorWatchTestDatastore(t, b, LogicalWatchLedgerWaitTimeout(time.Second))

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, "DROP PUBLICATION spicedb_ledger;")
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})

	select {
	case err := <-errchan:
		require.ErrorContains(err, "did not record the watch marker transaction")
		// The message has to name the slot and its state, since that is what an
		// operator acts on.
		require.ErrorContains(err, "spicedb_ledger")
	case change := <-changes:
		require.Fail("the watch delivered an event despite an unusable ledger", "%v", change)
	case <-time.After(cursorWatchTestTimeout):
		require.Fail("the watch neither failed nor delivered while the ledger was stalled")
	}
}

// testLedgerRecordsWhatTheStreamReports asserts that the position the ledger
// records for a transaction is the position its commit record actually occupies
// in the WAL, as reported by the transaction's own commit LSN on the stream.
// Every token the watch mints is that value, so a discrepancy here would be a
// discrepancy in every token.
func testLedgerRecordsWhatTheStreamReports(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds, dbURI := newCursorWatchTestDatastore(t, b)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	const writeCount = 5
	writtenXids := make([]uint64, 0, writeCount)
	for i := 0; i < writeCount; i++ {
		rel := tuple.MustParse(fmt.Sprintf("document:recorded#viewer@user:write_%d", i))
		revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)

		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		writtenXids = append(writtenXids, txid.Uint64)
	}

	deliveredTokens := collectTokensByXid(t, changes, errchan, setOfXids(writtenXids...))

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	recorded := recordedCommitLSNs(t, ctx, conn)
	for _, xid := range writtenXids {
		require.Contains(recorded, xid)

		deliveredRevision, err := ParseRevisionString(deliveredTokens[xid])
		require.NoError(err)

		deliveredLSN := pglogrepl.LSN(deliveredRevision.(postgresRevision).optionalCommitLSN)
		require.Equal(recorded[xid], deliveredLSN.String(),
			"transaction %d was delivered at a position other than the one recorded for it", xid)
	}
}

// testLedgerTakeover asserts that recording survives the loss of the instance
// doing it. The replication slot admits one consumer, so a survivor takes over as
// soon as the holder's session is gone, and writes that landed in between are
// still recorded because the slot retained their WAL.
func testLedgerTakeover(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// A short retry interval bounds how long the standby waits to notice.
	ds, dbURI := newCursorWatchTestDatastore(t, b, LogicalWatchLedgerRetryInterval(100*time.Millisecond))

	// A second instance against the same database stands by: it cannot attach to
	// the slot while the first holds it.
	standby, err := newPostgresDatastore(
		t.Context(), dbURI, primaryInstanceID,
		RevisionQuantization(0),
		GCWindow(1000*time.Second),
		GCInterval(veryLargeGCInterval),
		WatchBufferLength(512),
		WithRevisionHeartbeat(false),
		WithLogicalWatch(true),
		LogicalWatchLedgerRetryInterval(100*time.Millisecond),
	)
	require.NoError(err)
	t.Cleanup(func() { _ = standby.Close() })

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	pgds, ok := ds.(*pgDatastore)
	require.True(ok)
	slotName := pgds.ledgerSlotName

	// Exactly one instance is recording, even though both are running it.
	requireLedgerAttached(t, ctx, conn, slotName, true)

	// Evict the holder, then write. The write's WAL is retained by the slot, so
	// whichever instance attaches next must still record its position.
	_, err = conn.Exec(ctx, "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL;", slotName)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:takeover#viewer@user:after_eviction"))})
	})
	require.NoError(err)

	writtenXid, ok := revision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	require.EventuallyWithT(func(collect *assert.CollectT) {
		assert.Contains(collect, recordedCommitLSNs(t, ctx, conn), writtenXid.Uint64,
			"recording did not resume after the holding instance was evicted")
	}, cursorWatchTestTimeout, 20*time.Millisecond)

	// And a watch on the survivor is served normally, which is what a live
	// frontier means in practice.
	standbyHead, err := standby.HeadRevision(ctx)
	require.NoError(err)

	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	changes, errchan := standby.Watch(watchCtx, standbyHead.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	probeRevision, err := standby.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:takeover#viewer@user:probe"))})
	})
	require.NoError(err)

	probeEvents := collectChangesUntilXids(t, changes, errchan, revisionXids(t, probeRevision))
	_, _ = requireChangeForSubject(t, probeEvents, "probe")
}

// testLedgerPreLedgerHistoryIsDeliveredPositioned covers enabling the watch on a
// database that already holds history. Those transactions committed before the
// ledger existed, so they have no recoverable commit LSN, but inside the
// collection window their commit timestamps still order them: the backfill gives
// them reconstructed positions below the ledger's genesis.
//
// A consumer resuming from a polling-watch token therefore receives one
// comparable stream across the upgrade, in commit order, rather than an
// unpositioned prefix it must not compare.
func testLedgerPreLedgerHistoryIsDeliveredPositioned(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// The database starts out serving the polling watch, so nothing records
	// commit positions.
	var dbURI string
	pollingDS := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		dbURI = uri
		ds, err := newPostgresDatastore(
			t.Context(), uri, primaryInstanceID,
			RevisionQuantization(0),
			GCWindow(1000*time.Second),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(512),
			WithRevisionHeartbeat(false),
		)
		require.NoError(err)
		return ds
	})

	headRevision, err := pollingDS.HeadRevision(ctx)
	require.NoError(err)
	legacyToken := headRevision.Revision.(postgresRevision)
	require.False(legacyToken.ByteSortable(), "a revision from the polling watch carries no position")

	const historyCount = 3
	historicalXids := make([]uint64, 0, historyCount)
	for i := 0; i < historyCount; i++ {
		rel := tuple.MustParse(fmt.Sprintf("document:upgrade#viewer@user:history_%d", i))
		revision, err := pollingDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)

		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		historicalXids = append(historicalXids, txid.Uint64)
	}
	require.NoError(pollingDS.Close())

	// The upgrade: the cursor watch is enabled, which provisions the ledger and
	// records the genesis snapshot that separates the history above from
	// everything the ledger will see.
	upgradedDS, err := newPostgresDatastore(
		t.Context(), dbURI, primaryInstanceID,
		RevisionQuantization(0),
		GCWindow(1000*time.Second),
		GCInterval(veryLargeGCInterval),
		WatchBufferLength(512),
		WithRevisionHeartbeat(false),
		WithLogicalWatch(true),
	)
	require.NoError(err)
	t.Cleanup(func() { _ = upgradedDS.Close() })

	// Writes drive the flushes the backfill rides on; once it reports itself
	// finished, the reachable history carries positions.
	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	require.EventuallyWithT(func(collect *assert.CollectT) {
		_, err := upgradedDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:upgrade#viewer@user:driver"))})
		})
		assert.NoError(collect, err)

		var complete bool
		assert.NoError(collect, conn.QueryRow(ctx, "SELECT backfill_complete FROM ledger_state;").Scan(&complete))
		assert.True(collect, complete, "the pre-ledger backfill did not finish")
	}, cursorWatchTestTimeout, 100*time.Millisecond)

	postUpgradeRevision, err := upgradedDS.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:upgrade#viewer@user:after_upgrade"))})
	})
	require.NoError(err)

	postUpgradeXid, ok := postUpgradeRevision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	changes, errchan := upgradedDS.Watch(ctx, legacyToken, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	awaited := setOfXids(append(historicalXids, postUpgradeXid.Uint64)...)
	events := collectChangesUntilXids(t, changes, errchan, awaited)

	positionedByXid := make(map[uint64]bool, len(awaited))
	observedOrder := make([]uint64, 0, len(awaited))
	seenPositioned := false
	for _, event := range events {
		revision := event.Revision.(postgresRevision)
		if revision.ByteSortable() {
			seenPositioned = true
		} else {
			require.False(seenPositioned, "an unpositioned event arrived after a positioned one")
		}

		if event.IsCheckpoint {
			continue
		}
		txid, ok := revision.OptionalTransactionID()
		require.True(ok)
		positionedByXid[txid.Uint64] = revision.ByteSortable()
		observedOrder = append(observedOrder, txid.Uint64)
	}

	for _, xid := range historicalXids {
		require.True(positionedByXid[xid],
			"transaction %d predates the ledger but is inside the collection window, so the backfill must have positioned it", xid)
	}
	require.True(positionedByXid[postUpgradeXid.Uint64],
		"a transaction committed after the ledger was provisioned must carry a recorded position")

	// The writes that drove the backfill are delivered too, so the boundary is
	// checked as a subsequence rather than the whole stream.
	expected := make([]uint64, 0, len(historicalXids)+1)
	expected = append(expected, historicalXids...)
	expected = append(expected, postUpgradeXid.Uint64)
	next := 0
	for _, xid := range observedOrder {
		if next < len(expected) && xid == expected[next] {
			next++
		}
	}
	require.Equal(len(expected), next,
		"delivery must stay in commit order across the boundary; saw %v, wanted %v in order", observedOrder, expected)
}

// testLedgerUnrecordedTransactionFailsLoudly covers a transaction that postdates
// the ledger's genesis yet has no recorded position, which is what an invalidated
// and recreated slot leaves behind. Its position is unrecoverable, so the watch
// refuses rather than delivering something it cannot order.
func testLedgerUnrecordedTransactionFailsLoudly(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds, dbURI := newCursorWatchTestDatastore(t, b)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:gap#viewer@user:lost"))})
	})
	require.NoError(err)

	lostXid, ok := revision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	// Wait for the position to be recorded, then erase it: the ledger has
	// already confirmed past this transaction, so nothing will record it again.
	// This is the state a slot recreation leaves for the transactions it skipped.
	require.EventuallyWithT(func(collect *assert.CollectT) {
		assert.Contains(collect, recordedCommitLSNs(t, ctx, conn), lostXid.Uint64)
	}, cursorWatchTestTimeout, 20*time.Millisecond)

	_, err = conn.Exec(ctx, "DELETE FROM ledger_xid_lsn WHERE xid = $1;", lostXid)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})

	select {
	case err := <-errchan:
		require.ErrorContains(err, "no recorded commit LSN")
		require.ErrorContains(err, "unrecoverable")
	case change := <-changes:
		require.Fail("the watch delivered an event across an unrecoverable gap", "%v", change)
	case <-time.After(cursorWatchTestTimeout):
		require.Fail("the watch neither failed nor delivered across an unrecoverable gap")
	}
}

// testLedgerWritesAreInvisibleToWatchers asserts that the ledger's own writes
// never reach a watcher. They update the transaction table, and the watch keys
// off inserts there, so they carry no transaction row of their own and nothing
// would identify them as a revision.
func testLedgerWritesAreInvisibleToWatchers(t *testing.T, newDatastore newDatastoreFunc) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds := newDatastore(t, 0, 1000*time.Second, 512, true)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchSchema | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:ledgerecho#viewer@user:only_write"))})
	})
	require.NoError(err)

	writtenXid, ok := revision.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	events := collectChangesUntilXids(t, changes, errchan, setOfXids(writtenXid.Uint64))
	for _, event := range events {
		if event.IsCheckpoint {
			continue
		}
		txid, ok := event.Revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		require.Equal(writtenXid.Uint64, txid.Uint64, "an unexpected transaction reached the watcher")
	}

	// The ledger records this write moments later, well within the window below.
	// Nothing it does may surface as a change; checkpoints are the watch's own.
	drainDeadline := time.After(2 * time.Second)
	for {
		select {
		case change, ok := <-changes:
			require.True(ok, "the watch closed unexpectedly")
			require.True(change.IsCheckpoint, "the ledger's write surfaced as a change event: %v", change)
		case err := <-errchan:
			require.NoError(err)
		case <-drainDeadline:
			return
		}
	}
}

// testLedgerDisabledFeatureDetectsAbandonedSlot covers switching the feature off
// on a database where it ran. The durable slot is deliberately left in place,
// because dropping database objects on a configuration change is not this
// datastore's call, so startup has to be able to point at it: an unattended slot
// retains WAL until the disk fills.
func testLedgerDisabledFeatureDetectsAbandonedSlot(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	enabledDS, dbURI := newCursorWatchTestDatastore(t, b)

	enabledPGDS, ok := enabledDS.(*pgDatastore)
	require.True(ok)
	slotName := enabledPGDS.ledgerSlotName
	require.NoError(enabledDS.Close())

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	// The slot outlives the datastore that created it.
	var exists bool
	require.NoError(conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1);", slotName).Scan(&exists))
	require.True(exists, "the ledger slot must survive a datastore shutdown")

	disabledDS, err := newPostgresDatastore(
		t.Context(), dbURI, primaryInstanceID,
		RevisionQuantization(0),
		GCWindow(1000*time.Second),
		GCInterval(veryLargeGCInterval),
		WatchBufferLength(512),
		WithRevisionHeartbeat(false),
		WithLogicalWatch(false),
	)
	require.NoError(err)
	t.Cleanup(func() { _ = disabledDS.Close() })

	disabledPGDS, ok := disabledDS.(*pgDatastore)
	require.True(ok)

	// Startup resolved the slot's name even with the feature off, which is what
	// lets it report the abandoned slot, and observes it unattached.
	require.Equal(slotName, disabledPGDS.ledgerSlotName)

	state, err := disabledPGDS.readLedgerSlotState(ctx)
	require.NoError(err)
	require.True(state.exists)
	require.False(state.active, "nothing may be consuming the ledger slot with the feature disabled")
}

// testLedgerStorageShape asserts where commit positions live and what recording
// one costs.
//
// Positions are kept in their own table precisely so that recording one is an
// append rather than an update of the transaction row: the watch needs positions
// indexed, so such an update could never be heap-only, and every write would
// rewrite a ~100-byte row and re-enter all four of that table's indexes. This
// pins both halves — the column is absent, and writes leave the transaction
// table's update counter untouched.
func testLedgerStorageShape(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds, dbURI := newCursorWatchTestDatastore(t, b)

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	var gapCount int
	require.NoError(conn.QueryRow(ctx, "SELECT count(*) FROM ledger_gap;").Scan(&gapCount))
	require.Zero(gapCount, "a healthy database must have no recorded ledger gaps")

	var hasColumn bool
	require.NoError(conn.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = 'relation_tuple_transaction' AND column_name = 'commit_lsn');`).Scan(&hasColumn))
	require.False(hasColumn, "commit positions must live in their own table, not on the transaction row")

	// The baseline is taken after migration, which does backfill some rows of
	// its own; what matters is that serving writes adds nothing to it.
	transactionUpdates := func() int64 {
		var updates int64
		require.NoError(conn.QueryRow(ctx, `SELECT coalesce(n_tup_upd, 0)
			FROM pg_stat_user_tables WHERE relname = 'relation_tuple_transaction';`).Scan(&updates))
		return updates
	}
	baseline := transactionUpdates()

	writtenXids := make([]uint64, 0, 3)
	for i := 0; i < 3; i++ {
		rel := tuple.MustParse(fmt.Sprintf("document:shape#viewer@user:subject_%d", i))
		revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)

		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		writtenXids = append(writtenXids, txid.Uint64)
	}

	require.EventuallyWithT(func(collect *assert.CollectT) {
		recorded := recordedCommitLSNs(t, ctx, conn)
		for _, xid := range writtenXids {
			assert.Contains(collect, recorded, xid)
		}
	}, cursorWatchTestTimeout, 20*time.Millisecond)

	require.Equal(baseline, transactionUpdates(),
		"recording commit positions must not update the transaction table")
}

// testLedgerPositionsAreGarbageCollected asserts that recorded positions are
// collected along with the transactions they describe, and that the watch keeps
// working across a collection pass.
//
// The delete order is what is really under test. Positions are removed *after*
// the transaction rows, because the reverse would briefly leave transactions
// that look unrecorded, which is indistinguishable from a transaction lost to a
// slot recreation and would fail watches for no reason.
func testLedgerPositionsAreGarbageCollected(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// A tiny GC window makes everything written here immediately collectable.
	ds, dbURI := newCursorWatchTestDatastore(t, b, GCWindow(time.Millisecond))

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	for i := 0; i < 3; i++ {
		rel := tuple.MustParse(fmt.Sprintf("document:collected#viewer@user:subject_%d", i))
		_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
		})
		require.NoError(err)
	}

	require.EventuallyWithT(func(collect *assert.CollectT) {
		assert.NotEmpty(collect, recordedCommitLSNs(t, ctx, conn), "the ledger recorded nothing to collect")
	}, cursorWatchTestTimeout, 20*time.Millisecond)

	pgds, ok := ds.(*pgDatastore)
	require.True(ok)

	gc, err := pgds.BuildGarbageCollector(ctx)
	require.NoError(err)
	defer gc.Close()

	now, err := gc.Now(ctx)
	require.NoError(err)
	collectBefore, err := gc.TxIDBefore(ctx, now)
	require.NoError(err)
	_, err = gc.DeleteBeforeTx(ctx, collectBefore)
	require.NoError(err)

	// No position may outlive the horizon: whatever transactions were collected,
	// their positions went with them.
	collectedXid, ok := collectBefore.(postgresRevision).OptionalTransactionID()
	require.True(ok)

	var stragglers int
	require.NoError(conn.QueryRow(ctx,
		"SELECT count(*) FROM ledger_xid_lsn WHERE xid < $1;", collectedXid).Scan(&stragglers))
	require.Zero(stragglers, "collected transactions left their positions behind")

	// And a watch started afterwards still works: collection below a consumer's
	// position is not a gap, and must not be reported as one.
	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	afterRevision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:collected#viewer@user:after_gc"))})
	})
	require.NoError(err)

	events := collectChangesUntilXids(t, changes, errchan, revisionXids(t, afterRevision))
	_, _ = requireChangeForSubject(t, events, "after_gc")
}

// testLedgerOrphanPositionsAreIgnored asserts that a position whose transaction
// row is gone is inert. The two are deleted by separate statements, so the state
// is reachable whenever collection is interrupted, and it must not surface as an
// event, an error, or a cursor that skips ahead.
func testLedgerOrphanPositionsAreIgnored(t *testing.T, b testdatastore.RunningEngineForTest) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ds, dbURI := newCursorWatchTestDatastore(t, b)

	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	// An orphan: a position for a transaction that has no row. Its xid is far
	// above anything real, so it also sits above the watch's cursor, where a
	// naive implementation would trip over it.
	var currentLSN string
	require.NoError(conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text;").Scan(&currentLSN))
	_, err = conn.Exec(ctx,
		"INSERT INTO ledger_xid_lsn (xid, commit_lsn) VALUES ($1::xid8, $2::pg_lsn);",
		NewXid8(1<<40), currentLSN)
	require.NoError(err)

	rel := tuple.MustParse("document:orphan#viewer@user:real")
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(rel)})
	})
	require.NoError(err)

	events := collectChangesUntilXids(t, changes, errchan, revisionXids(t, revision))
	for _, event := range events {
		if event.IsCheckpoint {
			continue
		}
		txid, ok := event.Revision.(postgresRevision).OptionalTransactionID()
		require.True(ok)
		require.NotEqual(uint64(1<<40), txid.Uint64, "an orphan position was delivered as a change")
	}
	_, _ = requireChangeForSubject(t, events, "real")
}

// TestPostgresCursorWatchRequiresLogicalWALLevel asserts that the startup
// preflight refuses to bring up the cursor watch on a server that cannot support
// the commit LSN ledger.
func TestPostgresCursorWatchRequiresLogicalWALLevel(t *testing.T) {
	// The default test server runs with wal_level=replica, not logical.
	b := testdatastore.RunPostgresForTesting(t, postgresTestVersion(), false)

	var constructErr error
	_ = b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		_, constructErr = newPostgresDatastore(
			t.Context(), uri, primaryInstanceID,
			RevisionQuantization(0),
			GCWindow(1000*time.Second),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(16),
			WithRevisionHeartbeat(false),
			WithLogicalWatch(true),
		)
		return nil
	})

	require.Error(t, constructErr, "constructing a cursor-watch datastore on a non-logical server must fail")
	require.ErrorContains(t, constructErr, "wal_level")
}

// TestPostgresCursorWatchRequiresCommitTimestamps asserts that the startup
// preflight refuses a server without commit timestamps, rather than provisioning
// a WAL-retaining slot for a watch that could not replay a gap.
func TestPostgresCursorWatchRequiresCommitTimestamps(t *testing.T) {
	b := testdatastore.RunPostgresForTestingWithLogicalReplication(
		t, postgresTestVersion(),
		testcontainers.WithCmdArgs("-c", "track_commit_timestamp=off"),
	)

	var constructErr error
	_ = b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		_, constructErr = newPostgresDatastore(
			t.Context(), uri, primaryInstanceID,
			RevisionQuantization(0),
			GCWindow(1000*time.Second),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(16),
			WithRevisionHeartbeat(false),
			WithLogicalWatch(true),
		)
		return nil
	})

	require.Error(t, constructErr, "constructing a cursor-watch datastore without commit timestamps must fail")
	require.ErrorContains(t, constructErr, "track_commit_timestamp")
}

// TestPostgresCursorWatchNonDefaultServerConfig runs the cursor watch against a
// server with a non-UTC timezone, validating that the ledger's decoding and the
// watch's timestamps are pinned by their own session settings rather than the
// server defaults.
func TestPostgresCursorWatchNonDefaultServerConfig(t *testing.T) {
	require := require.New(t)

	b := testdatastore.RunPostgresForTestingWithLogicalReplication(
		t, postgresTestVersion(),
		// Command-line settings take precedence over the mounted config file.
		testcontainers.WithCmdArgs("-c", "track_commit_timestamp=on", "-c", "TimeZone=America/New_York"),
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var dbURI string
	ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		dbURI = uri
		ds, err := newPostgresDatastore(
			t.Context(), uri, primaryInstanceID,
			RevisionQuantization(0),
			GCWindow(1000*time.Second),
			GCInterval(veryLargeGCInterval),
			WatchBufferLength(512),
			WithRevisionHeartbeat(false),
			WithLogicalWatch(true),
		)
		require.NoError(err)
		return ds
	})

	// The server renders timestamps in a timezone neither the ledger nor the
	// watch may inherit.
	conn, err := pgx.Connect(ctx, dbURI)
	require.NoError(err)
	defer func() { _ = conn.Close(ctx) }()

	var setting string
	require.NoError(conn.QueryRow(ctx, "SHOW TimeZone;").Scan(&setting))
	require.Equal("America/New_York", setting)

	headRevision, err := ds.HeadRevision(ctx)
	require.NoError(err)

	expiration := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)

	// Committed before the watch: delivered by the backfill phase.
	backfillRel := tuple.MustParse(`document:tzdoc#viewer@user:backfill[somecaveat:{"origin":"backfill"}]`)
	backfillRel.OptionalExpiration = &expiration
	revBackfill, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(backfillRel)})
	})
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRevision.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships | datastore.WatchCheckpoints,
		CheckpointInterval: 100 * time.Millisecond,
	})
	require.Empty(errchan)

	backfillEvents := collectChangesUntilXids(t, changes, errchan, revisionXids(t, revBackfill))
	_, backfillEvent := requireChangeForSubject(t, backfillEvents, "backfill")
	backfillRevision := backfillEvent.Revision.(postgresRevision)
	require.True(backfillRevision.ByteSortable(), "the backfill must carry a recorded commit position")

	// And one committed while the watch runs, whose position the ledger decodes
	// out of the WAL under the non-UTC server default.
	laterRel := tuple.MustParse(`document:tzdoc#viewer@user:later[somecaveat:{"origin":"later"}]`)
	laterRel.OptionalExpiration = &expiration
	revLater, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(laterRel)})
	})
	require.NoError(err)

	laterEvents := collectChangesUntilXids(t, changes, errchan, revisionXids(t, revLater))
	_, laterEvent := requireChangeForSubject(t, laterEvents, "later")
	laterRevision := laterEvent.Revision.(postgresRevision)
	require.True(laterRevision.ByteSortable())
	require.Greater(laterRevision.optionalCommitLSN, backfillRevision.optionalCommitLSN)

	// Both must decode the timestamptz expiration to the exact instant written,
	// regardless of the server timezone, along with the caveat context.
	for _, entry := range []struct {
		name  string
		event datastore.RevisionChanges
		want  string
	}{
		{"backfill", backfillEvent, "backfill"},
		{"later", laterEvent, "later"},
	} {
		require.Len(entry.event.RelationshipChanges, 1, "%s event must carry exactly one relationship change", entry.name)
		relationship := entry.event.RelationshipChanges[0].Relationship
		require.NotNil(relationship.OptionalExpiration, "%s delivery lost the expiration", entry.name)
		require.True(relationship.OptionalExpiration.Equal(expiration),
			"%s delivery decoded expiration %s, want %s", entry.name, relationship.OptionalExpiration, expiration)
		require.NotNil(relationship.OptionalCaveat, "%s delivery lost the caveat", entry.name)
		require.Equal("somecaveat", relationship.OptionalCaveat.CaveatName)
		require.Equal(entry.want, relationship.OptionalCaveat.Context.AsMap()["origin"])
	}
}

// collectChangesUntilXids reads from the watch channel until every awaited
// transaction ID has been observed on a non-checkpoint change, returning all
// events (checkpoints included) seen along the way.
func collectChangesUntilXids(t *testing.T, changes <-chan datastore.RevisionChanges, errchan <-chan error, awaited map[uint64]struct{}) []datastore.RevisionChanges {
	t.Helper()

	remaining := make(map[uint64]struct{}, len(awaited))
	for xid := range awaited {
		remaining[xid] = struct{}{}
	}

	var collected []datastore.RevisionChanges
	timeout := time.After(cursorWatchTestTimeout)
	for len(remaining) > 0 {
		select {
		case change, ok := <-changes:
			require.True(t, ok, "watch channel closed while awaiting transactions")
			collected = append(collected, change)
			if !change.IsCheckpoint {
				revision, ok := change.Revision.(postgresRevision)
				require.True(t, ok)
				if txid, ok := revision.OptionalTransactionID(); ok {
					delete(remaining, txid.Uint64)
				}
			}
		case err := <-errchan:
			require.NoError(t, err, "unexpected watch error")
		case <-timeout:
			require.Fail(t, "timed out waiting for watched transactions", "%d transactions still unobserved", len(remaining))
		}
	}

	return collected
}

// collectChangesUntilRevision reads from the watch channel until a change or
// checkpoint at or beyond the given revision is observed, returning all
// non-checkpoint changes seen.
func collectChangesUntilRevision(t *testing.T, changes <-chan datastore.RevisionChanges, errchan <-chan error, untilRevision datastore.Revision) []datastore.RevisionChanges {
	t.Helper()

	var collected []datastore.RevisionChanges
	timeout := time.After(cursorWatchTestTimeout)
	for {
		select {
		case change, ok := <-changes:
			require.True(t, ok, "watch channel closed while waiting for revision %s", untilRevision)
			if !change.IsCheckpoint {
				collected = append(collected, change)
			}
			if change.Revision != nil && (change.Revision.Equal(untilRevision) || change.Revision.GreaterThan(untilRevision)) {
				return collected
			}
		case err := <-errchan:
			require.NoError(t, err, "unexpected watch error")
		case <-timeout:
			require.Fail(t, "timed out waiting for watch to reach revision", "revision: %s", untilRevision)
		}
	}
}

// collectTokensByXid reads until every awaited transaction has been delivered,
// returning the string form of the revision each was delivered at. A transaction
// delivered more than once within one stream must carry the same token every
// time.
func collectTokensByXid(t *testing.T, changes <-chan datastore.RevisionChanges, errchan <-chan error, awaited map[uint64]struct{}) map[uint64]string {
	t.Helper()

	tokens := make(map[uint64]string, len(awaited))
	for _, event := range collectChangesUntilXids(t, changes, errchan, awaited) {
		if event.IsCheckpoint {
			continue
		}

		revision, ok := event.Revision.(postgresRevision)
		require.True(t, ok)
		txid, ok := revision.OptionalTransactionID()
		require.True(t, ok, "change revision missing transaction ID")
		if _, isAwaited := awaited[txid.Uint64]; !isAwaited {
			continue
		}

		token := revision.String()
		if existing, seen := tokens[txid.Uint64]; seen {
			require.Equal(t, existing, token, "transaction %d delivered at two different tokens in one stream", txid.Uint64)
			continue
		}
		tokens[txid.Uint64] = token
	}

	for xid := range awaited {
		require.Contains(t, tokens, xid, "transaction %d was never delivered", xid)
	}

	return tokens
}

// setOfXids collects transaction IDs into the set form the watch test helpers await.
func setOfXids(xids ...uint64) map[uint64]struct{} {
	set := make(map[uint64]struct{}, len(xids))
	for _, xid := range xids {
		set[xid] = struct{}{}
	}
	return set
}

// positionPrefixOf returns the fixed-width position prefix of a revision token,
// which is the portion of the string form that carries commit order.
func positionPrefixOf(t *testing.T, token string) string {
	t.Helper()

	prefix, _, found := strings.Cut(token, string(lsnRevisionSeparator))
	require.True(t, found, "token %q carries no position prefix", token)
	return prefix
}

// revisionXids returns the set of transaction IDs of the given write revisions.
func revisionXids(t *testing.T, revisions ...datastore.Revision) map[uint64]struct{} {
	t.Helper()

	xids := make(map[uint64]struct{}, len(revisions))
	for _, revision := range revisions {
		txid, ok := revision.(postgresRevision).OptionalTransactionID()
		require.True(t, ok, "write revision missing its transaction ID")
		xids[txid.Uint64] = struct{}{}
	}
	return xids
}

// requireChangeForSubject returns the position and the change event carrying a
// relationship change for the given subject object ID.
func requireChangeForSubject(t *testing.T, events []datastore.RevisionChanges, subjectObjectID string) (int, datastore.RevisionChanges) {
	t.Helper()

	for index, change := range events {
		if change.IsCheckpoint {
			continue
		}
		for _, relChange := range change.RelationshipChanges {
			if relChange.Relationship.Subject.ObjectID == subjectObjectID {
				return index, change
			}
		}
	}

	require.Failf(t, "change not found", "no change observed for subject %s", subjectObjectID)
	return 0, datastore.RevisionChanges{}
}

// readSubjectIDs reads all relationships of the given resource type at the given
// revision and returns their subject object IDs.
func readSubjectIDs(t *testing.T, ctx context.Context, ds datastore.Datastore, revision datastore.Revision, resourceType string) []string {
	t.Helper()

	iterator, err := ds.SnapshotReader(revision).QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType: resourceType,
	})
	require.NoError(t, err)

	subjectIDs := make([]string, 0, 8)
	for rel, err := range iterator {
		require.NoError(t, err)
		subjectIDs = append(subjectIDs, rel.Subject.ObjectID)
	}
	return subjectIDs
}

// normalizedTransactionChanges is the order-insensitive, per-transaction view of
// watch output used to compare the two implementations.
type normalizedTransactionChanges struct {
	RelationshipChanges []string
	ChangedDefinitions  []string
	DeletedNamespaces   []string
	DeletedCaveats      []string
	Metadata            []string
}

func normalizeChangesByTransaction(t *testing.T, changes []datastore.RevisionChanges) map[uint64]normalizedTransactionChanges {
	t.Helper()

	normalized := make(map[uint64]normalizedTransactionChanges)
	for _, change := range changes {
		revision, ok := change.Revision.(postgresRevision)
		require.True(t, ok)
		txid, ok := revision.OptionalTransactionID()
		require.True(t, ok, "change revision missing transaction ID")

		entry := normalized[txid.Uint64]
		for _, relChange := range change.RelationshipChanges {
			entry.RelationshipChanges = append(entry.RelationshipChanges, relChange.DebugString())
		}
		for _, def := range change.ChangedDefinitions {
			entry.ChangedDefinitions = append(entry.ChangedDefinitions, fmt.Sprintf("%T:%s", def, def.GetName()))
		}
		entry.DeletedNamespaces = append(entry.DeletedNamespaces, change.DeletedNamespaces...)
		entry.DeletedCaveats = append(entry.DeletedCaveats, change.DeletedCaveats...)
		for _, metadata := range change.Metadatas {
			entry.Metadata = append(entry.Metadata, fmt.Sprintf("%v", metadata.AsMap()))
		}

		sort.Strings(entry.RelationshipChanges)
		sort.Strings(entry.ChangedDefinitions)
		sort.Strings(entry.DeletedNamespaces)
		sort.Strings(entry.DeletedCaveats)
		sort.Strings(entry.Metadata)
		normalized[txid.Uint64] = entry
	}

	return normalized
}
