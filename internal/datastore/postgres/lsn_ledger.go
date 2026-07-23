package postgres

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	log "github.com/authzed/spicedb/internal/logging"
)

// The commit LSN ledger records, for every SpiceDB write transaction, the WAL
// position at which that transaction committed.
//
// A transaction cannot record its own commit LSN, because writing it would
// create a new commit. PostgreSQL keeps no commit-order record either: xids are
// assigned when a transaction begins, the transaction row's timestamp is taken
// at statement time, and pg_xact_commit_timestamp is optional and tie-prone. So
// the only durable source of commit order is the WAL itself.
//
// The ledger reads it. One instance in the cluster tails a durable logical
// replication slot publishing relation_tuple_transaction inserts, and appends
// each transaction's commit LSN to the ledger_xid_lsn table. The slot's
// confirmed_flush_lsn is only advanced after those appends commit, which makes
// it a completeness frontier: every transaction that committed at or below it
// has its commit LSN recorded.
//
// Recording is an append rather than an update of the transaction row, so a
// SpiceDB write costs one narrow row here instead of a second version of its
// transaction row plus an entry in each of that table's four indexes. The
// transaction table stays insert/delete-only.
//
// This is what lets the logical watch position a replayed transaction at its
// real commit LSN instead of one fabricated per Watch call. A token is then a
// function of the transaction it points at, identical no matter which Watch call
// delivered it, and comparable against every other token ever emitted.
//
// Exactly one instance may consume a replication slot at a time: PostgreSQL
// fails START_REPLICATION on a slot that is already attached. That error is the
// cluster's leader election. Every instance runs the ledger, the losers stand by
// and retry, and a leader's death is noticed within one retry interval.
const (
	// pgOutputPlugin is the logical decoding plugin the ledger consumes,
	// PostgreSQL's built-in one.
	pgOutputPlugin = "pgoutput"

	// pgOutputProtoVersion is the pgoutput protocol version requested at
	// START_REPLICATION.
	pgOutputProtoVersion = "1"

	// minimumStatusInterval floors how often the ledger sends standby status
	// updates so the server can release WAL.
	minimumStatusInterval = time.Second

	// pgDuplicateObjectErr is returned when creating an object that already
	// exists, which is how creation races between instances are detected.
	pgDuplicateObjectErr = "42710"

	// ledgerIdleFlushDelay bounds how long recorded commit positions wait when
	// the stream has gone quiet. Under load, batches fill and flush by size
	// instead. This is the dominant term in how long a starting Watch call waits
	// for the frontier to reach its marker, so it is deliberately short.
	ledgerIdleFlushDelay = 10 * time.Millisecond

	// minimumLedgerRetryInterval floors the wait between attempts to attach to
	// the ledger slot, so a misconfigured interval cannot spin.
	minimumLedgerRetryInterval = 100 * time.Millisecond

	// pgObjectInUseErr is returned by START_REPLICATION when another session
	// already holds the replication slot.
	pgObjectInUseErr = "55006"

	// walStatusLost marks a replication slot whose WAL has been removed. Such a
	// slot can never be resumed and must be recreated.
	walStatusLost = "lost"

	// ledgerGapPendingToLSN is the to_lsn of a ledger_gap row for a slot
	// recreation that has not completed. It is the maximum pg_lsn, so the row
	// fails every watch positioned below it until the recreation finishes and
	// bounds the gap at the new slot's first position. A crash mid-recreation
	// therefore leaves a loud, over-wide gap rather than an unrecorded one.
	ledgerGapPendingToLSN = "FFFFFFFF/FFFFFFFF"
)

var (
	// ledgerPublicationTables is only the transaction table: one row per
	// SpiceDB write, carrying the xid8 the commit LSN is recorded against.
	ledgerPublicationTables = []string{schema.TableTransaction}

	// recordCommitLSNsQuery appends a batch of decoded commit positions. The
	// positions travel as parallel text arrays, so the statement has the same
	// shape for every batch size, and are zipped by unnest.
	//
	// This is the ledger's only write, and it is an append: the transaction rows
	// it describes are never touched, so recording a position costs one narrow
	// row and one entry in each of this table's two indexes, rather than
	// rewriting a transaction row and all four of its indexes.
	//
	// A batch replays after a crash between writing it and confirming the slot,
	// which re-appends identical rows; conflicting on the transaction id makes
	// that a no-op. The conflict is on xid rather than the position because
	// "this transaction is already recorded" is the condition being tolerated.
	recordCommitLSNsQuery = fmt.Sprintf(`
	INSERT INTO %[1]s (%[2]s, %[3]s)
	SELECT recorded.xid::xid8, recorded.commit_lsn::pg_lsn
	FROM unnest($1::text[], $2::text[]) AS recorded(xid, commit_lsn)
	ON CONFLICT (%[2]s) DO NOTHING;`,
		schema.TableLedgerXidLSN, schema.ColXID, schema.ColCommitLSN)

	// commitLSNForXidQuery reads one transaction's recorded commit position. It
	// returns no row when the ledger has not reached that transaction yet, which
	// is how a starting watch waits for its marker. pgx has no pg_lsn codec, so
	// the value is rendered as text and parsed with pglogrepl.
	commitLSNForXidQuery = fmt.Sprintf(
		`SELECT %[1]s::text FROM %[2]s WHERE %[3]s = $1;`,
		schema.ColCommitLSN, schema.TableLedgerXidLSN, schema.ColXID)

	// insertLedgerGenesisQuery records the snapshot taken when the ledger slot
	// was first created. It is never overwritten: it is the only way to tell a
	// transaction that committed before the ledger existed from one lost to a
	// slot recreation.
	//
	// ledger_state holds at most one row, enforced by its singleton column: a
	// boolean primary key constrained to be true. Recording the genesis snapshot
	// is therefore an insert that conflicts with itself on every attempt after
	// the first, which makes it exactly-once across every instance without any
	// coordination between them. The conflict target is named rather than left
	// implicit so that a violation of some other constraint is reported instead
	// of being silently read as "already recorded".
	insertLedgerGenesisQuery = fmt.Sprintf(
		`INSERT INTO %[1]s (%[3]s, %[2]s, %[4]s) VALUES (true, pg_current_snapshot(), $1::pg_lsn)
		ON CONFLICT (%[3]s) DO NOTHING;`,
		schema.TableLedgerState, schema.ColLedgerGenesisSnapshot, schema.ColLedgerSingleton,
		schema.ColLedgerGenesisLSN)

	// selectLedgerBackfillStateQuery reads what the pre-ledger backfill needs to
	// hand out its next batch: the snapshot that identifies pre-ledger history,
	// the position everything it assigns must stay below, and how many positions
	// it has already handed out below that.
	selectLedgerBackfillStateQuery = fmt.Sprintf(
		`SELECT %[2]s, %[3]s::text, %[4]s, %[5]s FROM %[1]s LIMIT 1;`,
		schema.TableLedgerState, schema.ColLedgerGenesisSnapshot, schema.ColLedgerGenesisLSN,
		schema.ColLedgerBackfillOffset, schema.ColLedgerBackfillComplete)

	// preLedgerTransactionsQuery returns the next batch of history to position,
	// newest first.
	//
	// A transaction qualifies when it predates the ledger (visible in the genesis
	// snapshot), has no position yet, and is still inside the collection window.
	// The window bound is what makes the work finite: a revision older than it is
	// already refused as stale, so positioning it would serve nobody.
	//
	// Newest first is deliberate. Positions are handed out counting down from the
	// genesis position, so collection pruning the old end never shifts a position
	// already assigned, and a partially finished backfill always leaves the
	// *oldest* slice unpositioned, which is exactly where the catch-up query's
	// NULLS FIRST puts it.
	preLedgerTransactionsQuery = fmt.Sprintf(`
	SELECT t.%[1]s
	FROM %[2]s t
	LEFT JOIN %[3]s p ON p.%[1]s = t.%[1]s
	WHERE p.%[1]s IS NULL
		AND pg_visible_in_snapshot(t.%[1]s, $1)
		AND t.%[4]s >= $2
		AND pg_xact_commit_timestamp(t.%[1]s::xid) IS NOT NULL
	ORDER BY pg_xact_commit_timestamp(t.%[1]s::xid) DESC, t.%[1]s DESC
	LIMIT $3;`,
		schema.ColXID, schema.TableTransaction, schema.TableLedgerXidLSN, schema.ColTimestamp)

	// advanceLedgerBackfillQuery records how far the backfill has counted down,
	// and marks it finished when a batch comes back short.
	advanceLedgerBackfillQuery = fmt.Sprintf(
		`UPDATE %[1]s SET %[2]s = $1, %[3]s = $2, %[4]s = (NOW() AT TIME ZONE 'utc');`,
		schema.TableLedgerState, schema.ColLedgerBackfillOffset,
		schema.ColLedgerBackfillComplete, schema.ColLedgerUpdatedAt)

	// selectLedgerGenesisQuery reads the genesis snapshot, if one was ever recorded.
	selectLedgerGenesisQuery = fmt.Sprintf(
		`SELECT %[1]s FROM %[2]s LIMIT 1;`,
		schema.ColLedgerGenesisSnapshot, schema.TableLedgerState)

	// selectSlotRecreationsQuery reads how many times the ledger slot has been
	// recreated, which is reported as a gauge for alerting.
	selectSlotRecreationsQuery = fmt.Sprintf(
		`SELECT COALESCE(MAX(%[1]s), 0) FROM %[2]s;`,
		schema.ColLedgerSlotRecreations, schema.TableLedgerState)

	// recordSlotRecreationQuery counts recreations of an invalidated slot, which
	// is the operator-visible signal that a gap in recorded commit positions exists.
	recordSlotRecreationQuery = fmt.Sprintf(
		`UPDATE %[1]s SET %[2]s = %[2]s + 1, %[3]s = (NOW() AT TIME ZONE 'utc');`,
		schema.TableLedgerState, schema.ColLedgerSlotRecreations, schema.ColLedgerUpdatedAt)

	// selectSlotStateQuery reports whether the ledger slot exists, whether it is
	// attached, how far it has durably confirmed, whether its WAL still exists,
	// and which database it is bound to.
	selectSlotStateQuery = `
	SELECT confirmed_flush_lsn::text, active, COALESCE(wal_status, ''), COALESCE(database, '')
	FROM pg_replication_slots WHERE slot_name = $1;`

	// createLedgerSlotQuery creates the ledger's durable slot and returns its
	// consistent point: the position from which the new slot decodes, which is
	// where a recreation's gap in recorded positions ends.
	createLedgerSlotQuery = `SELECT lsn::text FROM pg_create_logical_replication_slot($1, $2);`
	dropLedgerSlotQuery   = `SELECT pg_drop_replication_slot($1);`

	// insertLedgerGapQuery opens a gap: an interval of WAL the ledger will never
	// decode. It is written with the pending sentinel as its end position before
	// the slot is touched, so that no crash can leave the gap unrecorded.
	insertLedgerGapQuery = fmt.Sprintf(
		`INSERT INTO %[1]s (%[2]s, %[3]s) VALUES ($1::pg_lsn, $2::pg_lsn) ON CONFLICT DO NOTHING;`,
		schema.TableLedgerGap, schema.ColGapFromLSN, schema.ColGapToLSN)

	// closeLedgerGapsQuery bounds every pending gap at the given position, which
	// is the first position the recreated slot decodes from. Replacing the rows
	// rather than updating them in place keeps the operation atomic under the
	// table's (from_lsn, to_lsn) primary key.
	closeLedgerGapsQuery = fmt.Sprintf(`
	WITH pending AS (
		DELETE FROM %[1]s WHERE %[3]s = '%[4]s'::pg_lsn AND %[2]s < $1::pg_lsn RETURNING %[2]s
	)
	INSERT INTO %[1]s (%[2]s, %[3]s) SELECT %[2]s, $1::pg_lsn FROM pending ON CONFLICT DO NOTHING;`,
		schema.TableLedgerGap, schema.ColGapFromLSN, schema.ColGapToLSN, ledgerGapPendingToLSN)

	// firstLedgerGapAboveQuery finds the lowest recorded gap that a cursor at the
	// given position has not passed. A match means transactions above the cursor
	// were never recorded and delivery from it cannot be complete. The detection
	// time comes along so the watch can report *when* the gap opened, which is
	// what lets an operator line it up against the rest of an incident.
	firstLedgerGapAboveQuery = fmt.Sprintf(
		`SELECT %[2]s::text, %[3]s::text, %[4]s::text FROM %[1]s
		WHERE %[3]s > $1::pg_lsn ORDER BY %[2]s LIMIT 1;`,
		schema.TableLedgerGap, schema.ColGapFromLSN, schema.ColGapToLSN, schema.ColGapDetectedAt)

	// selectLedgerGapsQuery lists every recorded gap. Healing replays a gap's
	// transactions out of the tables, and a transaction carries no evidence of
	// which gap swallowed it, so a database holding more than one gap is left to
	// the loud failure path rather than guessing an interval.
	selectLedgerGapsQuery = fmt.Sprintf(
		`SELECT %[2]s::text, %[3]s::text, %[4]s::text FROM %[1]s ORDER BY %[2]s;`,
		schema.TableLedgerGap, schema.ColGapFromLSN, schema.ColGapToLSN, schema.ColGapDetectedAt)

	// ledgerHealTargetQuery samples the position the ledger must confirm past
	// before a gap can be healed, together with the snapshot that bounds the
	// window at the same instant. A transaction visible in that snapshot
	// committed before the sample, so once the ledger has confirmed past the
	// sampled position, such a transaction is either recorded or was swallowed
	// by the gap; nothing that is merely still in flight can be mistaken for a
	// victim.
	ledgerHealTargetQuery = `SELECT pg_current_wal_lsn()::text, pg_current_snapshot();`

	// gapWindowTransactionsQuery returns the transactions a gap swallowed, in
	// true commit order.
	//
	// A transaction belongs to the window when it postdates the ledger's genesis
	// snapshot (so it is not pre-ledger history, which never had a position and
	// is delivered unpositioned), committed before the window was bounded, and
	// either has no recorded position or already carries one an earlier healing
	// attempt assigned. Including the already-assigned ones is what keeps the
	// rank below stable: a retry that finds part of its own work committed still
	// ranks the same set and so reproduces the same positions.
	//
	// pg_xact_commit_timestamp is the commit order the lost WAL would have given.
	// Commit timestamps are only microsecond resolution, so the transaction id
	// breaks ties, which also makes the order total and the rank deterministic.
	gapWindowTransactionsQuery = fmt.Sprintf(`
	SELECT t.%[1]s, t.%[4]s
	FROM %[2]s t
	LEFT JOIN %[3]s p ON p.%[1]s = t.%[1]s
	WHERE NOT pg_visible_in_snapshot(t.%[1]s, $1)
		AND pg_visible_in_snapshot(t.%[1]s, $2)
		AND (p.%[1]s IS NULL OR (p.%[5]s > $3::pg_lsn AND p.%[5]s < $4::pg_lsn))
	ORDER BY pg_xact_commit_timestamp(t.%[1]s::xid), t.%[1]s;`,
		schema.ColXID, schema.TableTransaction, schema.TableLedgerXidLSN,
		schema.ColTimestamp, schema.ColCommitLSN)

	// deleteLedgerGapQuery retires a gap whose transactions have all been
	// replayed, which is what lets watches positioned below it resume.
	deleteLedgerGapQuery = fmt.Sprintf(
		`DELETE FROM %[1]s WHERE %[2]s = $1::pg_lsn AND %[3]s = $2::pg_lsn;`,
		schema.TableLedgerGap, schema.ColGapFromLSN, schema.ColGapToLSN)

	// maxRecordedCommitLSNQuery reports the highest commit position the ledger
	// has recorded, or NULL when none remain. Everything at or below it is
	// recorded, which makes it the tightest sound start for a gap whose true
	// start died with an operator-dropped slot.
	maxRecordedCommitLSNQuery = fmt.Sprintf(
		`SELECT max(%[1]s)::text FROM %[2]s;`,
		schema.ColCommitLSN, schema.TableLedgerXidLSN)

	// errLedgerGenesisMissing indicates no genesis snapshot has been recorded,
	// which means the ledger has never been provisioned against this database.
	errLedgerGenesisMissing = errors.New("the commit LSN ledger has not been initialized")

	// errLedgerReprovisioned indicates the ledger's slot was missing or
	// invalidated and has been re-created, so the stream has to be established
	// against the new slot. It is a step in recovering, not a failure, and in
	// particular it is not the clean end of the stream that shutdown produces.
	errLedgerReprovisioned = errors.New("the commit LSN ledger slot was re-provisioned")
)

var (
	ledgerActiveGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "ledger_active",
		Help:      "Whether this instance is the one recording commit LSNs for the logical watch.",
	})

	ledgerBackfilledCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "ledger_backfilled_transactions_total",
		Help:      "The number of transactions whose commit LSN has been recorded.",
	})

	ledgerFlushDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "ledger_flush_duration_seconds",
		Help:      "The duration of writing a batch of recorded commit LSNs.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
	})

	ledgerLagBytesGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "ledger_lag_bytes",
		Help:      "WAL bytes between the server's current position and the commit LSN ledger's confirmed frontier.",
	})

	ledgerAttachmentsCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "ledger_attachments_total",
		Help:      "The number of times this instance has taken over recording commit LSNs.",
	})

	ledgerFailureCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "ledger_failure_total",
		Help:      "The number of times the commit LSN ledger disconnected with an error.",
	})

	ledgerSlotRecreationsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "ledger_slot_recreations",
		Help:      "The number of times the commit LSN ledger's replication slot has been recreated, each of which leaves a gap in recorded commit positions.",
	})

	ledgerGapsHealedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "ledger_gaps_healed_total",
		Help:      "Gaps in recorded commit positions replayed from the transaction tables, after which watches positioned below them resume instead of failing.",
	})

	ledgerPreLedgerBackfilledCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "ledger_pre_ledger_backfilled_total",
		Help:      "Transactions predating the commit LSN ledger given a reconstructed commit position.",
	})

	ledgerGapTransactionsHealedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "ledger_gap_transactions_healed_total",
		Help:      "Transactions given a replayed commit position while healing a gap.",
	})

	watchLedgerWaitHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "spicedb",
		Subsystem: "datastore_postgres",
		Name:      "watch_ledger_wait_seconds",
		Help:      "How long a starting logical watch waited for the commit LSN ledger to record its marker transaction.",
		Buckets:   []float64{0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30},
	})
)

// ledgerSlotState is what pg_replication_slots reports about the ledger's slot.
type ledgerSlotState struct {
	confirmed pglogrepl.LSN
	active    bool
	walStatus string
	database  string
	exists    bool
}

// ledgerPosition is one transaction's commit position, as read from the WAL.
type ledgerPosition struct {
	xid       xid8
	commitLSN pglogrepl.LSN
}

// ledgerGap is a recorded interval of WAL the ledger never decoded, left behind
// when its replication slot was invalidated or dropped and had to be recreated.
// Transactions that committed inside it have no recorded commit position.
type ledgerGap struct {
	// from is the last position known complete before the gap, to the first
	// position complete after it.
	from, to pglogrepl.LSN

	// detectedAt is when the recreation was noticed, rendered as text: it is
	// reported to operators rather than compared, so it is never parsed.
	detectedAt string
}

// ledgerBatch accumulates commit positions until they are worth a write, and
// tracks the stream position those writes let the slot confirm. It is the
// ledger's flush policy, kept separate from the replication session.
type ledgerBatch struct {
	maxSize  int
	maxDelay time.Duration

	positions []ledgerPosition

	// confirmLSN is the end position of the last transaction added, which
	// becomes the slot's frontier once the batch is durable.
	confirmLSN pglogrepl.LSN

	// startedAt is when the oldest un-flushed position was added.
	startedAt time.Time
}

// ledgerTransaction is one WAL transaction, reduced to what the ledger records.
type ledgerTransaction struct {
	commitLSN pglogrepl.LSN
	endLSN    pglogrepl.LSN

	// hasTransactionRow reports whether the transaction inserted a
	// relation_tuple_transaction row. Transactions without one have no commit
	// position to record: the ledger's own writes are the common case.
	hasTransactionRow bool
	xid               xid8
}

// ledgerDecoder is a minimal pgoutput state machine that extracts the xid8 of
// each committed transaction. It decodes only the transaction table's xid
// column, deliberately sharing none of the watch's semantic decoding.
type ledgerDecoder struct {
	relations map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map
	current   *ledgerTransaction
}

// prepareCommitLSNLedger provisions the ledger's publication and durable slot.
// It runs on every instance at construction; the database objects are shared and
// created at most once.
func (pgd *pgDatastore) prepareCommitLSNLedger(ctx context.Context) error {
	if err := pgd.resolveLedgerSlotName(ctx); err != nil {
		return err
	}

	if err := pgd.ensurePublication(ctx, pgd.logicalWatchLedgerPublicationName, ledgerPublicationTables, "insert"); err != nil {
		return err
	}

	pgd.warnAboutLedgerSlotLimits(ctx)

	return pgd.ensureCommitLSNLedgerSlot(ctx)
}

// warnAboutLedgerSlotLimits reports the two server settings that decide what
// happens when the ledger falls behind or cannot get a slot. Neither is fatal and
// neither can be set from here: they are cluster-wide, and on managed PostgreSQL
// they live in a parameter group.
func (pgd *pgDatastore) warnAboutLedgerSlotLimits(ctx context.Context) {
	var maxSlots, usedSlots int
	if err := pgd.writePool.QueryRow(ctx,
		"SELECT current_setting('max_replication_slots')::int, (SELECT count(*) FROM pg_replication_slots);",
	).Scan(&maxSlots, &usedSlots); err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("unable to check replication slot headroom for the commit LSN ledger")
	} else if usedSlots >= maxSlots {
		log.Ctx(ctx).Warn().Int("max_replication_slots", maxSlots).Int("in_use", usedSlots).
			Msg("no replication slot headroom: the commit LSN ledger needs one durable slot for this database, so raise max_replication_slots")
	}

	// Unlimited retention means a stalled ledger grows pg_wal until the disk
	// fills, taking the database down with it. A bounded setting makes PostgreSQL
	// invalidate the ledger's slot instead, which is recoverable: the watch
	// reports the resulting gap and consumers restart from a current revision.
	var keepSize string
	if err := pgd.writePool.QueryRow(ctx, "SHOW max_slot_wal_keep_size;").Scan(&keepSize); err != nil {
		return
	}
	if keepSize == "-1" {
		log.Ctx(ctx).Warn().
			Msg("max_slot_wal_keep_size is unlimited, so a stalled commit LSN ledger will retain WAL until the disk fills; bound it so PostgreSQL invalidates the slot instead, and alert on spicedb_datastore_postgres_ledger_lag_bytes")
	}
}

// warnIfAbandonedLedgerSlot reports a leftover ledger slot on a database where
// the feature it serves has been switched off. Preflight does not run in that
// configuration, so nothing else would notice a durable slot that no longer has
// a consumer and retains WAL indefinitely.
func (pgd *pgDatastore) warnIfAbandonedLedgerSlot(ctx context.Context) {
	if err := pgd.resolveLedgerSlotName(ctx); err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("unable to check for an abandoned commit LSN ledger replication slot")
		return
	}

	state, err := pgd.readLedgerSlotState(ctx)
	if err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("unable to check for an abandoned commit LSN ledger replication slot")
		return
	}

	// An attached slot is another instance's, still running with the feature
	// enabled; only an unattached one is abandoned.
	if state.exists && !state.active {
		log.Ctx(ctx).Warn().Str("slot", pgd.ledgerSlotName).Str("wal_status", state.walStatus).
			Msgf("a commit LSN ledger replication slot exists but the watch it serves is disabled; nothing consumes it, so it will retain WAL indefinitely. If the feature is staying off, drop it: SELECT pg_drop_replication_slot('%s');", pgd.ledgerSlotName)
	}
}

// resolveLedgerSlotName qualifies the configured slot name with a digest of the
// database name.
//
// Replication slot names are cluster-global, but a logical slot decodes exactly
// one database and the ledger is per-database. Two SpiceDB databases in one
// PostgreSQL cluster would otherwise fight over a single slot: the first would
// create it and the second would find it already present but bound elsewhere,
// leaving it unable to record anything. Qualifying the name gives each database
// its own slot with no configuration.
func (pgd *pgDatastore) resolveLedgerSlotName(ctx context.Context) error {
	var database string
	if err := pgd.writePool.QueryRow(ctx, "SELECT current_database();").Scan(&database); err != nil {
		return fmt.Errorf("unable to determine the current database for the commit LSN ledger: %w", err)
	}

	digest := fnv.New32a()
	if _, err := digest.Write([]byte(database)); err != nil {
		return fmt.Errorf("unable to derive the commit LSN ledger slot name: %w", err)
	}

	pgd.ledgerSlotName = fmt.Sprintf("%s_%08x", pgd.logicalWatchLedgerSlotName, digest.Sum32())
	pgd.ledgerDatabase = database
	return nil
}

// ensureCommitLSNLedgerSlot creates the ledger's replication slot if it is
// missing, recreates it if its WAL has been lost, and records the genesis
// snapshot the watch uses to interpret transactions with no recorded position.
//
// Every recreation leaves an interval of WAL the ledger never decoded, and
// after it the slot's frontier jumps past that interval, so nothing downstream
// can detect it. The interval is therefore recorded as a ledger_gap row, and
// recorded *before* the slot is touched: the gap opens with a pending sentinel
// end position and is bounded at the new slot's consistent point once creation
// succeeds. A crash between the two leaves a loud over-wide gap instead of a
// silent one.
func (pgd *pgDatastore) ensureCommitLSNLedgerSlot(ctx context.Context) error {
	slotName := pgd.ledgerSlotName

	state, err := pgd.readLedgerSlotState(ctx)
	if err != nil {
		return err
	}
	slotPreExisted := state.exists

	if state.exists && state.database != pgd.ledgerDatabase {
		return fmt.Errorf(
			"the commit LSN ledger's replication slot %q decodes database %q rather than %q; configure a distinct slot name for this datastore",
			slotName, state.database, pgd.ledgerDatabase)
	}

	// Whether the ledger has ever been provisioned against this database
	// decides whether a missing slot is a first creation or a recreation that
	// skipped WAL.
	provisioned := true
	if _, err := pgd.ledgerGenesisSnapshot(ctx); err != nil {
		if !errors.Is(err, errLedgerGenesisMissing) {
			return err
		}
		provisioned = false
	}

	if state.exists && state.walStatus == walStatusLost {
		// The slot's WAL is gone, so the transactions that committed while it was
		// unattended can never have their commit positions recovered. Recreating
		// it restores the ledger going forward; the genesis snapshot stays as it
		// was, which is what lets the watch tell these transactions apart from
		// ones that predate the ledger. The gap opens at the slot's last
		// confirmed position: everything at or below it is recorded, nothing
		// above it was decoded.
		log.Ctx(ctx).Error().Str("slot", slotName).
			Msg("the commit LSN ledger's replication slot was invalidated; recreating it. Transactions that committed while it was invalid have no recorded commit position, and watches positioned below the recorded gap will fail until their consumers restart from a current revision")

		if err := pgd.openLedgerGap(ctx, state.confirmed); err != nil {
			return err
		}
		if _, err := pgd.writePool.Exec(ctx, dropLedgerSlotQuery, slotName); err != nil {
			return fmt.Errorf("unable to drop the invalidated commit LSN ledger slot %s: %w", slotName, err)
		}
		state = ledgerSlotState{}
	}

	recreated := !state.exists && provisioned

	if !state.exists && provisioned {
		// The slot is gone without the ledger having invalidation-dropped it,
		// which means an operator dropped it, and its last confirmed position
		// died with it. The highest recorded commit position is a sound lower
		// bound for the gap: everything at or below it is recorded.
		log.Ctx(ctx).Error().Str("slot", slotName).
			Msg("the commit LSN ledger's replication slot is missing on a database where the ledger was provisioned; recreating it and recording the gap. Transactions that committed while it was gone have no recorded commit position")

		var maxRecordedText *string
		if err := pgd.writePool.QueryRow(ctx, maxRecordedCommitLSNQuery).Scan(&maxRecordedText); err != nil {
			return fmt.Errorf("unable to bound the commit LSN ledger gap: %w", err)
		}
		var gapFrom pglogrepl.LSN
		if maxRecordedText != nil {
			gapFrom, err = pglogrepl.ParseLSN(*maxRecordedText)
			if err != nil {
				return fmt.Errorf("unable to parse the highest recorded commit position: %w", err)
			}
		}
		if err := pgd.openLedgerGap(ctx, gapFrom); err != nil {
			return err
		}
	}

	// The slot's consistent point is the position everything predating the ledger
	// committed below, which is where the pre-ledger backfill counts down from.
	var consistentPointText string

	if !state.exists {
		if err := pgd.writePool.QueryRow(ctx, createLedgerSlotQuery, slotName, pgOutputPlugin).Scan(&consistentPointText); err != nil {
			// Another instance may have raced us to create the slot; its slot
			// state read below bounds any pending gap all the same.
			if pgerr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgerr.Code != pgDuplicateObjectErr {
				return fmt.Errorf("unable to create the commit LSN ledger slot %s: %w", slotName, err)
			}
		}

		state, err = pgd.readLedgerSlotState(ctx)
		if err != nil {
			return err
		}
		if !state.exists {
			return fmt.Errorf("the commit LSN ledger slot %s disappeared while being provisioned", slotName)
		}
	}

	// Bound every pending gap at the slot's first decoded position. After a
	// crash mid-recreation the slot may have confirmed further by now, so this
	// can over-approximate the gap's end, which fails a few more watches than
	// strictly necessary but never hides a skipped interval.
	if err := pgd.closePendingLedgerGaps(ctx, state.confirmed); err != nil {
		return err
	}

	if recreated {
		if _, err := pgd.writePool.Exec(ctx, recordSlotRecreationQuery); err != nil {
			return fmt.Errorf("unable to record the commit LSN ledger slot recreation: %w", err)
		}
	}

	// The genesis snapshot must be taken at or after the slot's creation: a
	// transaction invisible in it commits after the slot exists, and so is
	// guaranteed to reach the ledger. Transactions that commit between the
	// slot's creation and this snapshot are visible in it *and* reach the
	// ledger, which is harmless because the watch only consults the genesis
	// snapshot for rows that still have no recorded position after the frontier
	// has passed them.
	// A slot this process did not create reports no consistent point, so its
	// confirmed position stands in: everything predating the ledger committed
	// below that too.
	genesisLSN := consistentPointText
	if genesisLSN == "" {
		genesisLSN = state.confirmed.String()
	}

	tag, err := pgd.writePool.Exec(ctx, insertLedgerGenesisQuery, genesisLSN)
	if err != nil {
		return fmt.Errorf("unable to record the commit LSN ledger genesis snapshot: %w", err)
	}
	if tag.RowsAffected() > 0 && slotPreExisted {
		log.Ctx(ctx).Warn().Str("slot", slotName).
			Msg("recorded a commit LSN ledger genesis snapshot for a pre-existing replication slot; transactions that committed while that slot was unattended may be treated as predating the ledger")
	}

	var recreations int64
	if err := pgd.writePool.QueryRow(ctx, selectSlotRecreationsQuery).Scan(&recreations); err == nil {
		ledgerSlotRecreationsGauge.Set(float64(recreations))
	}

	return nil
}

// openLedgerGap records the start of a WAL interval the ledger is about to
// lose, with the pending sentinel as its end. It runs before the slot surgery
// it protects, so no crash between the two can leave the interval unrecorded.
func (pgd *pgDatastore) openLedgerGap(ctx context.Context, fromLSN pglogrepl.LSN) error {
	if _, err := pgd.writePool.Exec(ctx, insertLedgerGapQuery, fromLSN.String(), ledgerGapPendingToLSN); err != nil {
		return fmt.Errorf("unable to record the commit LSN ledger gap: %w", err)
	}
	return nil
}

// closePendingLedgerGaps bounds every pending gap at the given position, the
// first position the recreated slot decodes from.
func (pgd *pgDatastore) closePendingLedgerGaps(ctx context.Context, toLSN pglogrepl.LSN) error {
	if _, err := pgd.writePool.Exec(ctx, closeLedgerGapsQuery, toLSN.String()); err != nil {
		return fmt.Errorf("unable to bound the commit LSN ledger gap: %w", err)
	}
	return nil
}

// pendingGapHeal is a recorded gap the ledger may be able to replay, together
// with the state that bounds the replay: the position the ledger must confirm
// past first, and the snapshot taken at the same instant that decides which
// transactions the gap is answerable for.
type pendingGapHeal struct {
	gap      ledgerGap
	target   pglogrepl.LSN
	boundary pgSnapshot
}

// planGapHeal reports the gap this attachment should try to replay, if any.
//
// It returns nothing unless exactly one bounded gap exists: replaying works
// backwards from the transaction tables, where a transaction carries no
// evidence of which gap swallowed it, so attributing transactions among several
// gaps would be guesswork. A still-pending gap is skipped because its interval
// has no end yet.
func (pgd *pgDatastore) planGapHeal(ctx context.Context) (*pendingGapHeal, error) {
	rows, err := pgd.readPool.Query(ctx, selectLedgerGapsQuery)
	if err != nil {
		return nil, fmt.Errorf("unable to list commit LSN ledger gaps: %w", err)
	}
	defer rows.Close()

	var gaps []ledgerGap
	for rows.Next() {
		var fromText, toText, detectedAt string
		if err := rows.Scan(&fromText, &toText, &detectedAt); err != nil {
			return nil, fmt.Errorf("unable to decode a recorded ledger gap: %w", err)
		}
		from, err := pglogrepl.ParseLSN(fromText)
		if err != nil {
			return nil, fmt.Errorf("unable to parse a recorded ledger gap: %w", err)
		}
		to, err := pglogrepl.ParseLSN(toText)
		if err != nil {
			return nil, fmt.Errorf("unable to parse a recorded ledger gap: %w", err)
		}
		gaps = append(gaps, ledgerGap{from: from, to: to, detectedAt: detectedAt})
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("unable to list commit LSN ledger gaps: %w", rows.Err())
	}

	if len(gaps) != 1 {
		return nil, nil
	}
	gap := gaps[0]

	pending, err := pglogrepl.ParseLSN(ledgerGapPendingToLSN)
	if err != nil {
		return nil, fmt.Errorf("unable to parse the pending gap sentinel: %w", err)
	}
	if gap.to >= pending {
		return nil, nil
	}

	var targetText string
	var boundary pgSnapshot
	if err := pgd.readPool.QueryRow(ctx, ledgerHealTargetQuery).Scan(&targetText, &boundary); err != nil {
		return nil, fmt.Errorf("unable to sample the commit LSN ledger heal target: %w", err)
	}
	target, err := pglogrepl.ParseLSN(targetText)
	if err != nil {
		return nil, fmt.Errorf("unable to parse the commit LSN ledger heal target: %w", err)
	}

	return &pendingGapHeal{gap: gap, target: target, boundary: boundary}, nil
}

// healLedgerGap replays a gap out of the transaction tables instead of leaving
// every watch below it to fail.
//
// A gap loses commit positions, not changes. The transactions it swallowed
// still have their relation_tuple_transaction rows, and the relationships they
// wrote are still in relation_tuple, removals included, because a removal is an
// MVCC soft delete that leaves the row in place until garbage collection.
// Giving each of those transactions a position inside the gap's interval
// therefore restores delivery of both the adds and the removes through the
// watch's ordinary query, with no special case on the read side.
//
// Positions are assigned in true commit order and land strictly inside the
// interval, so they sort above everything recorded before the gap and below
// everything the recreated slot recorded after it. They are stable: the window
// is closed, the order is total, and the rank covers positions an earlier
// attempt already assigned, so a retry reproduces them exactly and the insert
// ignores the conflicts.
//
// Replaying is only sound while the window is newer than the garbage collection
// horizon, because a removal can only be replayed while its soft-deleted row
// survives. An older window keeps the loud failure, which is the one case no
// stream can recover.
func (pgd *pgDatastore) healLedgerGap(ctx context.Context, heal *pendingGapHeal) error {
	genesis, err := pgd.ledgerGenesisSnapshot(ctx)
	if err != nil {
		return err
	}

	rows, err := pgd.readPool.Query(ctx, gapWindowTransactionsQuery,
		genesis, heal.boundary, heal.gap.from.String(), heal.gap.to.String())
	if err != nil {
		return fmt.Errorf("unable to load the transactions a commit LSN ledger gap swallowed: %w", err)
	}
	defer rows.Close()

	var xids []xid8
	var oldest time.Time
	for rows.Next() {
		var txid xid8
		var timestamp time.Time
		if err := rows.Scan(&txid, &timestamp); err != nil {
			return fmt.Errorf("unable to decode a gap transaction: %w", err)
		}
		if oldest.IsZero() || timestamp.Before(oldest) {
			oldest = timestamp
		}
		xids = append(xids, txid)
	}
	if rows.Err() != nil {
		return fmt.Errorf("unable to load the transactions a commit LSN ledger gap swallowed: %w", rows.Err())
	}

	if len(xids) > 0 {
		var now time.Time
		if err := pgd.readPool.QueryRow(ctx, "SELECT NOW() AT TIME ZONE 'utc';").Scan(&now); err != nil {
			return fmt.Errorf("unable to determine the database time: %w", err)
		}
		if oldest.Before(now.Add(-pgd.gcWindow)) {
			log.Ctx(ctx).Warn().Str("slot", pgd.ledgerSlotName).
				Str("from", heal.gap.from.String()).Str("to", heal.gap.to.String()).
				Msg("a commit LSN ledger gap is older than the garbage collection window, so the relationships it removed may already be collected and it cannot be replayed; watches below it will continue to fail")
			return nil
		}

		// Each transaction needs its own position strictly inside the interval.
		// A commit occupies far more WAL than the one byte a position consumes
		// here, so this only fails if the interval was bounded far too tightly.
		if uint64(len(xids)) >= uint64(heal.gap.to-heal.gap.from) {
			log.Ctx(ctx).Warn().Str("slot", pgd.ledgerSlotName).Int("transactions", len(xids)).
				Str("from", heal.gap.from.String()).Str("to", heal.gap.to.String()).
				Msg("a commit LSN ledger gap holds more transactions than its interval has positions, so it cannot be replayed")
			return nil
		}

		positions := make([]ledgerPosition, 0, len(xids))
		for i, txid := range xids {
			positions = append(positions, ledgerPosition{
				xid:       txid,
				commitLSN: heal.gap.from + pglogrepl.LSN(i) + 1,
			})
		}
		if err := pgd.recordCommitLSNs(ctx, positions); err != nil {
			return err
		}
		ledgerGapTransactionsHealedCounter.Add(float64(len(positions)))
	}

	if _, err := pgd.writePool.Exec(ctx, deleteLedgerGapQuery,
		heal.gap.from.String(), heal.gap.to.String()); err != nil {
		return fmt.Errorf("unable to retire a replayed commit LSN ledger gap: %w", err)
	}
	ledgerGapsHealedCounter.Inc()

	log.Ctx(ctx).Info().Str("slot", pgd.ledgerSlotName).Int("transactions", len(xids)).
		Str("from", heal.gap.from.String()).Str("to", heal.gap.to.String()).
		Msg("replayed a commit LSN ledger gap from the transaction tables; watches positioned below it can resume")

	return nil
}

// backfillPreLedgerPositions gives one batch of pre-ledger history a position,
// so that a consumer resuming across the upgrade compares tokens instead of
// meeting an unpositioned prefix. It reports whether there is more to do.
//
// History predating the ledger has no recoverable commit LSN: it committed
// before anything was decoding WAL. What it does still have, inside the
// collection window, is a commit timestamp, and that is enough to reconstruct
// its order. Positions are handed out counting down from the ledger's genesis
// position, so they sort below everything the ledger records for itself while
// preserving commit order among themselves.
//
// Counting down is what makes the work resumable. Collection prunes the old end,
// so a position already assigned never shifts, and the slice left unpositioned
// by a partial pass is always the oldest one, which the catch-up query already
// orders first. Every intermediate state is therefore correct, and the batch can
// stop and resume freely.
//
// It runs one batch per flush: recording live positions is the ledger's real
// job, and the backfill is the background tenant that fills in behind it.
func (pgd *pgDatastore) backfillPreLedgerPositions(ctx context.Context) (bool, error) {
	var genesis pgSnapshot
	var genesisLSNText *string
	var offset int64
	var complete bool
	if err := pgd.readPool.QueryRow(ctx, selectLedgerBackfillStateQuery).
		Scan(&genesis, &genesisLSNText, &offset, &complete); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("unable to read the pre-ledger backfill state: %w", err)
	}

	// Nothing to anchor against, so pre-ledger history keeps the unpositioned
	// delivery it has always had.
	if complete || genesisLSNText == nil {
		return false, nil
	}

	genesisLSN, err := pglogrepl.ParseLSN(*genesisLSNText)
	if err != nil {
		return false, fmt.Errorf("unable to parse the commit LSN ledger genesis position: %w", err)
	}

	var horizon time.Time
	if err := pgd.readPool.QueryRow(ctx, "SELECT (NOW() AT TIME ZONE 'utc') - $1::interval;",
		pgd.gcWindow.String()).Scan(&horizon); err != nil {
		return false, fmt.Errorf("unable to determine the collection horizon: %w", err)
	}

	batchSize := max(pgd.logicalWatchLedgerBatchSize, 1)
	rows, err := pgd.readPool.Query(ctx, preLedgerTransactionsQuery, genesis, horizon, batchSize)
	if err != nil {
		return false, fmt.Errorf("unable to load pre-ledger history: %w", err)
	}
	defer rows.Close()

	var xids []xid8
	for rows.Next() {
		var txid xid8
		if err := rows.Scan(&txid); err != nil {
			return false, fmt.Errorf("unable to decode a pre-ledger transaction: %w", err)
		}
		xids = append(xids, txid)
	}
	if rows.Err() != nil {
		return false, fmt.Errorf("unable to load pre-ledger history: %w", rows.Err())
	}

	// Guarded rather than converted blindly: a negative offset could only come
	// from a corrupted row, and the unsigned arithmetic below would turn it into
	// a position far above the genesis rather than below it.
	handedOut, err := safecast.Convert[uint64](offset)
	if err != nil {
		return false, fmt.Errorf("unable to use the recorded pre-ledger backfill offset %d: %w", offset, err)
	}

	// Each position must stay strictly inside (0, genesis). Every commit writes
	// WAL, so the genesis position dwarfs the number of transactions below it;
	// running out means the anchor is wrong, and stopping is the safe answer.
	if handedOut+uint64(len(xids)) >= uint64(genesisLSN) {
		log.Ctx(ctx).Warn().Str("slot", pgd.ledgerSlotName).
			Msg("pre-ledger history does not fit below the ledger's genesis position; leaving the remainder unpositioned")
		return false, pgd.finishPreLedgerBackfill(ctx, offset)
	}

	// Newest first, walking down from the genesis position.
	next := genesisLSN - pglogrepl.LSN(handedOut) - 1
	positions := make([]ledgerPosition, 0, len(xids))
	for _, txid := range xids {
		positions = append(positions, ledgerPosition{xid: txid, commitLSN: next})
		next--
	}
	if len(positions) > 0 {
		if err := pgd.recordCommitLSNs(ctx, positions); err != nil {
			return false, err
		}
		ledgerPreLedgerBackfilledCounter.Add(float64(len(positions)))
	}

	// A short batch means the reachable history is exhausted.
	if len(xids) < batchSize {
		return false, pgd.finishPreLedgerBackfill(ctx, offset+int64(len(xids)))
	}

	if _, err := pgd.writePool.Exec(ctx, advanceLedgerBackfillQuery, offset+int64(len(xids)), false); err != nil {
		return false, fmt.Errorf("unable to advance the pre-ledger backfill: %w", err)
	}

	return true, nil
}

// finishPreLedgerBackfill marks the backfill done so later flushes stop looking.
func (pgd *pgDatastore) finishPreLedgerBackfill(ctx context.Context, offset int64) error {
	if _, err := pgd.writePool.Exec(ctx, advanceLedgerBackfillQuery, offset, true); err != nil {
		return fmt.Errorf("unable to complete the pre-ledger backfill: %w", err)
	}
	return nil
}

// firstLedgerGapAbove returns the lowest recorded gap not entirely at or below
// the given position, if any. A watch whose cursor is below a gap cannot prove
// completeness and must fail rather than skip the gap's transactions silently.
func (pgd *pgDatastore) firstLedgerGapAbove(ctx context.Context, position pglogrepl.LSN) (ledgerGap, bool, error) {
	var fromText, toText, detectedAt string
	if err := pgd.readPool.QueryRow(ctx, firstLedgerGapAboveQuery, position.String()).Scan(&fromText, &toText, &detectedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledgerGap{}, false, nil
		}
		return ledgerGap{}, false, fmt.Errorf("unable to check for commit LSN ledger gaps: %w", err)
	}

	from, err := pglogrepl.ParseLSN(fromText)
	if err != nil {
		return ledgerGap{}, false, fmt.Errorf("unable to parse a recorded ledger gap: %w", err)
	}
	to, err := pglogrepl.ParseLSN(toText)
	if err != nil {
		return ledgerGap{}, false, fmt.Errorf("unable to parse a recorded ledger gap: %w", err)
	}

	return ledgerGap{from: from, to: to, detectedAt: detectedAt}, true, nil
}

// readLedgerSlotState reports the ledger slot's durable frontier, whether it is
// currently attached, its WAL status, the database it decodes, and whether it
// exists at all.
//
// It reads through the read pool: every caller runs on a primary, where both
// pools address the same server, and the cursor watch calls this once per poll
// per watcher, which does not belong on the write pool. That per-watcher traffic
// is why a single shared frontier poller per datastore is the noted refinement if
// watcher counts ever grow.
func (pgd *pgDatastore) readLedgerSlotState(ctx context.Context) (ledgerSlotState, error) {
	var state ledgerSlotState

	var confirmedText *string
	row := pgd.readPool.QueryRow(ctx, selectSlotStateQuery, pgd.ledgerSlotName)
	if err := row.Scan(&confirmedText, &state.active, &state.walStatus, &state.database); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ledgerSlotState{}, nil
		}
		return ledgerSlotState{}, fmt.Errorf("unable to inspect the commit LSN ledger slot: %w", err)
	}
	state.exists = true

	if confirmedText != nil {
		confirmed, err := pglogrepl.ParseLSN(*confirmedText)
		if err != nil {
			return state, fmt.Errorf("unable to parse the commit LSN ledger slot's confirmed position: %w", err)
		}
		state.confirmed = confirmed
	}

	return state, nil
}

// ledgerGenesisSnapshot returns the snapshot taken when the ledger slot was
// first created. Transactions visible in it that still have no recorded commit
// position predate the ledger; transactions invisible in it that have none are a
// gap left by a slot recreation.
func (pgd *pgDatastore) ledgerGenesisSnapshot(ctx context.Context) (pgSnapshot, error) {
	var genesis pgSnapshot
	if err := pgd.readPool.QueryRow(ctx, selectLedgerGenesisQuery).Scan(&genesis); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return genesis, errLedgerGenesisMissing
		}
		return genesis, fmt.Errorf("unable to read the commit LSN ledger genesis snapshot: %w", err)
	}
	return genesis, nil
}

// runCommitLSNLedger records commit positions for as long as the datastore
// lives. Only one instance can hold the slot, so the others stand by and retry;
// the loop therefore only returns when the datastore shuts down.
func (pgd *pgDatastore) runCommitLSNLedger(ctx context.Context) error {
	retryInterval := max(pgd.logicalWatchLedgerRetryInterval, minimumLedgerRetryInterval)

	for {
		if ctx.Err() != nil {
			return nil
		}

		err := pgd.recordCommitLSNsFromWAL(ctx)

		reattachNow := false
		switch {
		case ctx.Err() != nil:
			return nil

		case err == nil:
			// The stream ended without an error, which only happens on shutdown.
			return nil

		case errors.Is(err, errLedgerReprovisioned):
			// The slot was just created, so there is nothing to wait for: the
			// sooner recording resumes, the smaller the gap that was recorded.
			log.Ctx(ctx).Info().Str("slot", pgd.ledgerSlotName).
				Msg("re-provisioned the commit LSN ledger slot; resuming recording")
			reattachNow = true

		case isReplicationSlotInUseError(err):
			log.Ctx(ctx).Trace().Str("slot", pgd.ledgerSlotName).
				Msg("the commit LSN ledger slot is held by another instance; standing by")

		default:
			ledgerFailureCounter.Inc()
			log.Ctx(ctx).Warn().Err(err).Str("slot", pgd.ledgerSlotName).
				Msg("the commit LSN ledger disconnected; retrying")
		}

		if reattachNow {
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retryInterval):
		}
	}
}

// recordCommitLSNsFromWAL attaches to the ledger slot and records commit
// positions until the stream fails or the context is cancelled.
func (pgd *pgDatastore) recordCommitLSNsFromWAL(ctx context.Context) error {
	// The slot's own confirmed position is where recording resumes, and is read
	// before attaching because the query cannot run on a replication connection.
	state, err := pgd.readLedgerSlotState(ctx)
	if err != nil {
		return err
	}
	if !state.exists || state.walStatus == walStatusLost {
		// A slot dropped or invalidated underneath a running instance:
		// re-provision it, which records the resulting gap, and report that so
		// the caller reattaches rather than reading the successful
		// re-provisioning as the stream having ended.
		if err := pgd.ensureCommitLSNLedgerSlot(ctx); err != nil {
			return err
		}
		return errLedgerReprovisioned
	}

	// Planned before attaching, on the same connection kind as the rest of the
	// ledger's bookkeeping, and acted on once the stream has confirmed past the
	// sampled position. Only the instance that wins the slot reaches the stream,
	// so the replay has a single writer without any further coordination.
	heal, err := pgd.planGapHeal(ctx)
	if err != nil {
		return err
	}

	conn, err := pgd.connectLogicalReplication(ctx)
	if err != nil {
		return fmt.Errorf("unable to establish the commit LSN ledger's replication connection: %w", err)
	}
	defer func() {
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelClose()
		_ = conn.Close(closeCtx)
	}()

	// The ledger only reads xid8 values, but the walsender renders every value
	// using this session's settings, so they are pinned exactly as the watch
	// pins them.
	if _, err := conn.Exec(ctx, "SET TIME ZONE 'UTC'; SET datestyle TO ISO; SET bytea_output = 'hex';").ReadAll(); err != nil {
		return fmt.Errorf("unable to configure the commit LSN ledger's replication session: %w", err)
	}

	publicationOption := "publication_names " + quotePGOutputStringOption(quotePGOutputIdentifier(pgd.logicalWatchLedgerPublicationName))
	if err := pglogrepl.StartReplication(ctx, conn, pgd.ledgerSlotName, state.confirmed, pglogrepl.StartReplicationOptions{
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{
			fmt.Sprintf("proto_version '%s'", pgOutputProtoVersion),
			publicationOption,
		},
	}); err != nil {
		return fmt.Errorf("unable to start the commit LSN ledger's replication stream: %w", err)
	}

	log.Ctx(ctx).Info().Str("slot", pgd.ledgerSlotName).Stringer("from", state.confirmed).
		Msg("recording commit LSNs for the logical watch")
	ledgerAttachmentsCounter.Inc()
	ledgerActiveGauge.Set(1)
	defer ledgerActiveGauge.Set(0)

	return pgd.consumeLedgerStream(ctx, conn, state.confirmed, heal)
}

// consumeLedgerStream reads the ledger's replication stream, writing each
// transaction's commit position back to its row and confirming the slot only
// once those writes are durable.
func (pgd *pgDatastore) consumeLedgerStream(ctx context.Context, conn *pgconn.PgConn, confirmedLSN pglogrepl.LSN, heal *pendingGapHeal) error {
	statusInterval := max(pgd.logicalWatchStatusInterval, minimumStatusInterval)
	nextStatusDeadline := time.Now().Add(statusInterval)
	var nextLagSample time.Time

	decoder := newLedgerDecoder()
	batch := newLedgerBatch(pgd.logicalWatchLedgerBatchSize, pgd.logicalWatchLedgerFlushMaxDelay)

	// Attempted until a pass reports the reachable history exhausted; the state
	// it reads is persisted, so a reattach resumes rather than restarts.
	backfilling := true

	sendStatus := func() error {
		if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: confirmedLSN,
			WALFlushPosition: confirmedLSN,
			WALApplyPosition: confirmedLSN,
			ClientTime:       time.Now(),
		}); err != nil {
			return fmt.Errorf("unable to send the commit LSN ledger's standby status update: %w", err)
		}
		nextStatusDeadline = time.Now().Add(statusInterval)
		return nil
	}

	// flush is the only place the frontier moves, and it moves strictly after
	// the recorded positions are committed. A crash in between replays the batch
	// on the next attach, which rewrites the same values.
	flush := func() error {
		positions, confirmTo := batch.take()
		if len(positions) == 0 {
			return nil
		}

		startedAt := time.Now()
		if err := pgd.recordCommitLSNs(ctx, positions); err != nil {
			return err
		}
		ledgerFlushDurationHistogram.Observe(time.Since(startedAt).Seconds())
		ledgerBackfilledCounter.Add(float64(len(positions)))

		confirmedLSN = confirmTo

		// Replaying a gap is only sound once the ledger has confirmed past the
		// position sampled when the replay was planned: until then a transaction
		// that has merely not been decoded yet is indistinguishable from one the
		// gap swallowed. It is attempted once per attachment; a replay left
		// undone (an interval older than the collection window, say) stays
		// recorded and keeps failing watches, which is the safe outcome.
		if heal != nil && confirmedLSN >= heal.target {
			if err := pgd.healLedgerGap(ctx, heal); err != nil {
				return err
			}
			heal = nil
		}

		// One batch of pre-ledger history per flush, so filling in behind the
		// ledger never competes with recording in front of it.
		if backfilling {
			more, err := pgd.backfillPreLedgerPositions(ctx)
			if err != nil {
				return err
			}
			backfilling = more
		}

		// Sampling the lag costs a round-trip, and a trickle of writes flushes as
		// often as every ledgerIdleFlushDelay, so it is sampled on the same
		// cadence as the status updates rather than per flush.
		if time.Now().After(nextLagSample) {
			pgd.observeLedgerLag(ctx, confirmedLSN)
			nextLagSample = time.Now().Add(statusInterval)
		}

		return sendStatus()
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		if time.Now().After(nextStatusDeadline) {
			if err := sendStatus(); err != nil {
				return err
			}
		}

		deadline := nextStatusDeadline
		if flushDeadline, pending := batch.flushDeadline(); pending && flushDeadline.Before(deadline) {
			deadline = flushDeadline
		}

		receiveCtx, cancelReceive := context.WithDeadline(ctx, deadline)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		cancelReceive()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if pgconn.Timeout(err) {
				// Either the batch is due or the status update is; both are
				// handled at the top of the loop, so flush here and continue.
				if err := flush(); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("the commit LSN ledger's replication stream failed: %w", err)
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("unable to parse the commit LSN ledger's keepalive message: %w", err)
				}
				// A keepalive's ServerWALEnd may be confirmed when, and only
				// when, the ledger is idle (see canConfirmLedgerIdle): the
				// walsender streams WAL in order and reports positions it has
				// fully sent, so with nothing pending and no transaction open,
				// everything at or below that position is decoded and recorded.
				// Without this, a quiet stream pins WAL forever and the
				// frontier never crosses stretches with nothing to record.
				if canConfirmLedgerIdle(batch, decoder) && keepalive.ServerWALEnd > confirmedLSN {
					confirmedLSN = keepalive.ServerWALEnd
				}
				if keepalive.ReplyRequested {
					if err := flush(); err != nil {
						return err
					}
					if err := sendStatus(); err != nil {
						return err
					}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("unable to parse the commit LSN ledger's replication data: %w", err)
				}

				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return fmt.Errorf("unable to parse the commit LSN ledger's pgoutput message: %w", err)
				}

				committed, err := decoder.handleMessage(logicalMsg)
				if err != nil {
					return err
				}
				if committed == nil {
					continue
				}

				if !committed.hasTransactionRow {
					// Nothing to record: the ledger's own writes and any other
					// transaction that touched no transaction row land here.
					// Their WAL is confirmable immediately when the ledger is
					// otherwise idle, and is covered by a later batch when not.
					if canConfirmLedgerIdle(batch, decoder) && committed.endLSN > confirmedLSN {
						confirmedLSN = committed.endLSN
					}
					continue
				}

				batch.add(ledgerPosition{xid: committed.xid, commitLSN: committed.commitLSN}, committed.endLSN, time.Now())
				if batch.full() {
					if err := flush(); err != nil {
						return err
					}
				}
			}

		case *pgproto3.ErrorResponse:
			return fmt.Errorf("the commit LSN ledger's replication stream errored: %s", msg.Message)

		default:
			// Other protocol messages carry nothing the ledger records.
		}
	}
}

// recordCommitLSNs appends a batch of commit positions in a single transaction.
// A position for a transaction that has already been garbage collected is
// appended all the same and simply never read: it sits below every consumer's
// horizon, and the next collection pass removes it.
func (pgd *pgDatastore) recordCommitLSNs(ctx context.Context, positions []ledgerPosition) error {
	xids := make([]string, 0, len(positions))
	commitLSNs := make([]string, 0, len(positions))
	for _, position := range positions {
		xids = append(xids, strconv.FormatUint(position.xid.Uint64, 10))
		commitLSNs = append(commitLSNs, position.commitLSN.String())
	}

	if _, err := pgd.writePool.Exec(ctx, recordCommitLSNsQuery, xids, commitLSNs); err != nil {
		return fmt.Errorf("unable to record commit LSNs: %w", err)
	}

	return nil
}

// observeLedgerLag reports how far the confirmed frontier trails the server's
// current WAL position, which is the WAL the slot is holding on to.
func (pgd *pgDatastore) observeLedgerLag(ctx context.Context, confirmedLSN pglogrepl.LSN) {
	var currentText string
	if err := pgd.writePool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text;").Scan(&currentText); err != nil {
		return
	}

	current, err := pglogrepl.ParseLSN(currentText)
	if err != nil || current < confirmedLSN {
		return
	}

	ledgerLagBytesGauge.Set(float64(current - confirmedLSN))
}

// canConfirmLedgerIdle reports whether it is safe to confirm a stream position
// the ledger has reached but recorded nothing at.
//
// Confirming a position asserts "everything at or below it is recorded", so it
// is only safe when nothing contradicts that assertion. Two things do: pending
// positions, which sit below the stream position and are not yet durable, and
// an open transaction in the decoder, whose records have been seen but not yet
// turned into a pending position. Confirming past either silently loses the
// events between the confirmed position and what was actually recorded.
func canConfirmLedgerIdle(batch *ledgerBatch, decoder *ledgerDecoder) bool {
	return !batch.pending() && !decoder.inTransaction()
}

func newLedgerBatch(maxSize int, maxDelay time.Duration) *ledgerBatch {
	return &ledgerBatch{
		maxSize:  max(maxSize, 1),
		maxDelay: max(maxDelay, ledgerIdleFlushDelay),
	}
}

// add records one transaction's commit position, along with the stream position
// that becomes confirmable once the batch is written.
func (b *ledgerBatch) add(position ledgerPosition, endLSN pglogrepl.LSN, now time.Time) {
	if len(b.positions) == 0 {
		b.startedAt = now
	}

	b.positions = append(b.positions, position)
	if endLSN > b.confirmLSN {
		b.confirmLSN = endLSN
	}
}

// full reports whether the batch is worth writing on size alone.
func (b *ledgerBatch) full() bool {
	return len(b.positions) >= b.maxSize
}

// pending reports whether any recorded positions await a flush.
func (b *ledgerBatch) pending() bool {
	return len(b.positions) > 0
}

// flushDeadline returns when the pending positions must be written, and whether
// any are pending at all. A quiet stream flushes promptly so that a Watch call
// waiting on the frontier is not held up; a busy one is bounded by maxDelay.
func (b *ledgerBatch) flushDeadline() (time.Time, bool) {
	if len(b.positions) == 0 {
		return time.Time{}, false
	}

	idle := time.Now().Add(ledgerIdleFlushDelay)
	if maximum := b.startedAt.Add(b.maxDelay); maximum.Before(idle) {
		return maximum, true
	}
	return idle, true
}

// take drains the batch, returning its positions and the position the slot may
// confirm once they are durable.
func (b *ledgerBatch) take() ([]ledgerPosition, pglogrepl.LSN) {
	positions, confirmLSN := b.positions, b.confirmLSN
	b.positions = nil
	b.startedAt = time.Time{}
	return positions, confirmLSN
}

// inTransaction reports whether the decoder is between a BEGIN and its COMMIT.
func (d *ledgerDecoder) inTransaction() bool {
	return d.current != nil
}

func newLedgerDecoder() *ledgerDecoder {
	typeMap := pgtype.NewMap()
	RegisterTypes(typeMap)

	return &ledgerDecoder{
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:   typeMap,
	}
}

// handleMessage processes one decoded pgoutput message, returning a transaction
// when a COMMIT completes it.
func (d *ledgerDecoder) handleMessage(msg pglogrepl.Message) (*ledgerTransaction, error) {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		d.relations[m.RelationID] = m

	case *pglogrepl.BeginMessage:
		d.current = &ledgerTransaction{commitLSN: m.FinalLSN}

	case *pglogrepl.InsertMessage:
		if d.current == nil {
			return nil, errors.New("the commit LSN ledger received an INSERT outside of a transaction")
		}
		relation, ok := d.relations[m.RelationID]
		if !ok {
			return nil, fmt.Errorf("the commit LSN ledger received an INSERT for unknown relation OID %d", m.RelationID)
		}
		if relation.RelationName != schema.TableTransaction {
			// The publication only publishes the transaction table, so this can
			// only happen against a publication someone else has widened.
			return nil, nil
		}

		row, err := decodeLogicalRow(d.typeMap, relation, m.Tuple, nil)
		if err != nil {
			return nil, fmt.Errorf("the commit LSN ledger could not decode a transaction row: %w", err)
		}
		xid, err := row.xid8Column(schema.ColXID)
		if err != nil {
			return nil, err
		}

		d.current.hasTransactionRow = true
		d.current.xid = xid

	case *pglogrepl.CommitMessage:
		committed := d.current
		if committed == nil {
			return nil, errors.New("the commit LSN ledger received a COMMIT outside of a transaction")
		}
		d.current = nil

		// BEGIN carries the commit position the watch stamps its live revisions
		// with, and COMMIT carries it again. They must agree, or the two paths a
		// transaction reaches a consumer by would position it differently.
		if committed.commitLSN != m.CommitLSN {
			return nil, fmt.Errorf(
				"the commit LSN ledger observed disagreeing commit positions for one transaction: BEGIN reported %s and COMMIT reported %s",
				committed.commitLSN, m.CommitLSN)
		}
		committed.endLSN = m.TransactionEndLSN
		return committed, nil

	default:
		// Updates, deletes, truncations, origins and type messages carry no
		// commit position to record.
	}

	return nil, nil
}

// isReplicationSlotInUseError reports whether the error is PostgreSQL refusing a
// second consumer for a replication slot, which is how the ledger elects its
// single writer.
func isReplicationSlotInUseError(err error) bool {
	pgerr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pgerr.Code == pgObjectInUseErr
}

// connectLogicalReplication opens a pgconn connection in logical replication
// mode (replication=database) using the datastore's connection configuration.
func (pgd *pgDatastore) connectLogicalReplication(ctx context.Context) (*pgconn.PgConn, error) {
	poolConfig, err := pgxpool.ParseConfig(pgd.dburl)
	if err != nil {
		return nil, err
	}

	connConfig := poolConfig.ConnConfig.Copy()
	connConfig.RuntimeParams["replication"] = "database"

	if pgd.credentialsProvider != nil {
		connConfig.User, connConfig.Password, err = pgd.credentialsProvider.Get(ctx, fmt.Sprintf("%s:%d", connConfig.Host, connConfig.Port), connConfig.User)
		if err != nil {
			return nil, err
		}
	}

	return pgconn.ConnectConfig(ctx, &connConfig.Config)
}

// ensurePublication creates the named publication over exactly the given tables
// if it does not exist, or verifies (and repairs) its table list if it does.
// publishOperations is the value of the publication's `publish` parameter, which
// is only applied at creation: an operator who has narrowed an existing
// publication is not overruled.
func (pgd *pgDatastore) ensurePublication(ctx context.Context, publicationName string, publicationTables []string, publishOperations string) error {
	var exists bool
	if err := pgd.writePool.QueryRow(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1);", publicationName,
	).Scan(&exists); err != nil {
		return fmt.Errorf("unable to check for publication %s: %w", publicationName, err)
	}

	if !exists {
		tables := make([]string, 0, len(publicationTables))
		for _, table := range publicationTables {
			tables = append(tables, pgx.Identifier{table}.Sanitize())
		}

		createPublication := fmt.Sprintf(
			"CREATE PUBLICATION %s FOR TABLE %s WITH (publish = '%s');",
			pgx.Identifier{publicationName}.Sanitize(),
			strings.Join(tables, ", "),
			publishOperations,
		)
		if _, err := pgd.writePool.Exec(ctx, createPublication); err != nil {
			// Another instance may have raced us to create the publication.
			if pgerr, ok := errors.AsType[*pgconn.PgError](err); ok && pgerr.Code == pgDuplicateObjectErr {
				return nil
			}
			return fmt.Errorf("unable to create publication %s: %w", publicationName, err)
		}
		return nil
	}

	rows, err := pgd.writePool.Query(ctx,
		"SELECT tablename FROM pg_publication_tables WHERE pubname = $1;", publicationName)
	if err != nil {
		return fmt.Errorf("unable to list tables for publication %s: %w", publicationName, err)
	}
	defer rows.Close()

	published := make(map[string]struct{})
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("unable to list tables for publication %s: %w", publicationName, err)
		}
		published[tableName] = struct{}{}
	}
	if rows.Err() != nil {
		return fmt.Errorf("unable to list tables for publication %s: %w", publicationName, rows.Err())
	}

	for _, table := range publicationTables {
		if _, ok := published[table]; !ok {
			log.Ctx(ctx).Info().Str("table", table).Str("publication", publicationName).Msg("adding missing table to publication")
			alterPublication := fmt.Sprintf(
				"ALTER PUBLICATION %s ADD TABLE %s;",
				pgx.Identifier{publicationName}.Sanitize(),
				pgx.Identifier{table}.Sanitize(),
			)
			if _, err := pgd.writePool.Exec(ctx, alterPublication); err != nil {
				return fmt.Errorf("unable to add table %s to publication %s: %w", table, publicationName, err)
			}
		}
	}

	return nil
}

// quotePGOutputIdentifier double-quotes an identifier for use inside the value
// of a pgoutput option such as publication_names.
func quotePGOutputIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quotePGOutputStringOption single-quotes a string value for use in a
// START_REPLICATION plugin option list.
func quotePGOutputStringOption(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// registerLedgerMetrics registers the commit LSN ledger's and the cursor
// watch's metrics and returns them so they can be unregistered when the
// datastore closes.
func registerLedgerMetrics() ([]prometheus.Collector, error) {
	collectors := []prometheus.Collector{
		ledgerActiveGauge,
		ledgerBackfilledCounter,
		ledgerFlushDurationHistogram,
		ledgerLagBytesGauge,
		ledgerAttachmentsCounter,
		ledgerFailureCounter,
		ledgerSlotRecreationsGauge,
		ledgerGapsHealedCounter,
		ledgerGapTransactionsHealedCounter,
		ledgerPreLedgerBackfilledCounter,
		watchLedgerWaitHistogram,
		watchFrontierLagGauge,
		watchPollDurationHistogram,
		watchBatchTransactionsHistogram,
		watchStaleRevisionCounter,
		watchGapRejectionsCounter,
	}

	for _, collector := range collectors {
		if err := prometheus.Register(collector); err != nil {
			return nil, fmt.Errorf("failed to register commit LSN ledger metric: %w", err)
		}
	}

	return collectors, nil
}
