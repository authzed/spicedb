package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// commitLSNLedgerStatements create the three tables the commit LSN ledger owns.
//
// ledger_xid_lsn records, per transaction, the WAL position at which it
// committed. A transaction cannot write its own commit LSN, so the position is
// appended afterwards by the commit LSN ledger, which reads it out of the WAL.
// The watch reads this table to deliver transactions in true commit order.
//
// It is deliberately a side table rather than a column on
// relation_tuple_transaction. A column would have to be filled in by an UPDATE
// per write, and because the watch needs it indexed, that update could never be
// heap-only: every write would rewrite the ~100-byte transaction row, re-enter
// every index on it, and leave a dead tuple behind. Appending a narrow row
// here keeps relation_tuple_transaction insert/delete-only, which is what makes
// the feature affordable on a deployment retaining a day of high-rate writes.
//
// There is deliberately no foreign key to relation_tuple_transaction: the ledger
// appends positions from a replication stream, after those transactions
// committed and possibly after garbage collection has removed them. A position
// whose transaction is gone is inert, and readers join it away.
//
// ledger_state holds a single row. genesis_snapshot is the snapshot taken when
// the ledger's replication slot was first created, and distinguishes the two
// reasons a transaction can have no recorded position: one that committed before
// the ledger existed (visible in the snapshot, safely ordered ahead of
// everything recorded) from one lost to a slot recreation (not visible, and
// therefore unorderable).
//
// ledger_gap records WAL intervals the ledger never decoded, which exist when
// its replication slot was invalidated or dropped and had to be recreated.
// Transactions that committed inside such an interval have no recorded commit
// position, and after the recreation the ledger's frontier jumps past them, so
// nothing else can tell they were skipped. from_lsn is the last position known
// complete before the gap, and to_lsn the first position complete after it. A
// row whose to_lsn is the maximum pg_lsn marks a recreation still in progress
// (or one that crashed partway), and keeps failing watches until the recreation
// completes and bounds it.
var commitLSNLedgerStatements = []string{
	`CREATE TABLE ledger_xid_lsn (
		xid xid8 NOT NULL,
		commit_lsn pg_lsn NOT NULL,
		CONSTRAINT pk_ledger_xid_lsn PRIMARY KEY (xid));`,

	// The watch scans this index by position and reads the xid straight out of
	// it, so its hot query never touches the heap. It is unique because a commit
	// record occupies one position, which exactly one transaction wrote.
	`CREATE UNIQUE INDEX ix_ledger_xid_lsn_by_lsn
		ON ledger_xid_lsn (commit_lsn) INCLUDE (xid);`,

	`CREATE TABLE ledger_state (
		singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
		genesis_snapshot pg_snapshot NOT NULL,
		genesis_lsn pg_lsn,
		backfill_offset BIGINT NOT NULL DEFAULT 0,
		backfill_complete BOOLEAN NOT NULL DEFAULT false,
		slot_recreations BIGINT NOT NULL DEFAULT 0,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'));`,

	`CREATE TABLE ledger_gap (
		from_lsn pg_lsn NOT NULL,
		to_lsn   pg_lsn NOT NULL,
		detected_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'utc'),
		CONSTRAINT pk_ledger_gap PRIMARY KEY (from_lsn, to_lsn));`,
}

func init() {
	if err := DatabaseMigrations.Register("add-commit-lsn-ledger", "populate-schema-tables",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			for _, stmt := range commitLSNLedgerStatements {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
