//go:build datastore && postgres

package postgres

import (
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// TestLedgerBatchFlushPolicy covers when the commit LSN ledger decides a batch of
// recorded positions is worth writing, and what the slot may confirm once it is.
func TestLedgerBatchFlushPolicy(t *testing.T) {
	position := func(xid uint64, commitLSN uint64) ledgerPosition {
		return ledgerPosition{xid: NewXid8(xid), commitLSN: pglogrepl.LSN(commitLSN)}
	}

	t.Run("size triggers a flush", func(t *testing.T) {
		batch := newLedgerBatch(2, time.Second)
		now := time.Now()

		require.False(t, batch.full(), "an empty batch is never full")

		batch.add(position(100, 0x100), 0x108, now)
		require.False(t, batch.full())

		batch.add(position(101, 0x110), 0x118, now)
		require.True(t, batch.full(), "a batch at its size limit must flush")
	})

	t.Run("non-positive limits are clamped", func(t *testing.T) {
		batch := newLedgerBatch(0, 0)
		require.Equal(t, 1, batch.maxSize, "a batch must hold at least one position")
		require.Equal(t, ledgerIdleFlushDelay, batch.maxDelay, "the maximum delay cannot be shorter than the idle flush delay")

		batch.add(position(100, 0x100), 0x108, time.Now())
		require.True(t, batch.full())
	})

	t.Run("no flush deadline while the batch is empty", func(t *testing.T) {
		batch := newLedgerBatch(8, time.Second)

		_, pending := batch.flushDeadline()
		require.False(t, pending, "an empty batch has nothing to flush")
	})

	t.Run("a quiet stream flushes promptly, a busy one within the maximum delay", func(t *testing.T) {
		const maxDelay = 50 * time.Millisecond
		batch := newLedgerBatch(1024, maxDelay)

		startedAt := time.Now()
		batch.add(position(100, 0x100), 0x108, startedAt)

		deadline, pending := batch.flushDeadline()
		require.True(t, pending)
		require.False(t, deadline.After(startedAt.Add(maxDelay)),
			"the deadline must never exceed the age the batch is allowed to reach")

		// A batch whose allowance has already elapsed is due immediately rather
		// than waiting out another idle delay.
		stale := newLedgerBatch(1024, maxDelay)
		stale.add(position(101, 0x110), 0x118, startedAt.Add(-time.Second))
		staleDeadline, pending := stale.flushDeadline()
		require.True(t, pending)
		require.True(t, staleDeadline.Before(time.Now()), "an overdue batch must be due immediately")
	})

	t.Run("take drains the batch and reports the position the slot may confirm", func(t *testing.T) {
		batch := newLedgerBatch(8, time.Second)
		now := time.Now()

		batch.add(position(100, 0x100), 0x108, now)
		batch.add(position(101, 0x110), 0x118, now)

		positions, confirmTo := batch.take()
		require.Len(t, positions, 2)
		require.Equal(t, NewXid8(100), positions[0].xid)
		require.Equal(t, pglogrepl.LSN(0x110), positions[1].commitLSN)
		require.Equal(t, pglogrepl.LSN(0x118), confirmTo, "the slot may confirm through the last transaction's end position")

		drained, _ := batch.take()
		require.Empty(t, drained, "a drained batch must not hand out its positions twice")

		_, pending := batch.flushDeadline()
		require.False(t, pending, "a drained batch has nothing left to flush")
	})

	t.Run("the confirmable position never moves backwards", func(t *testing.T) {
		batch := newLedgerBatch(8, time.Second)
		now := time.Now()

		batch.add(position(100, 0x200), 0x208, now)
		// A transaction whose WAL ends earlier must not pull the frontier back.
		batch.add(position(101, 0x100), 0x108, now)

		_, confirmTo := batch.take()
		require.Equal(t, pglogrepl.LSN(0x208), confirmTo)
	})
}

// TestLedgerDecoding covers the minimal pgoutput decoding the ledger performs: the
// xid8 of each committed transaction, the agreement between the two commit
// positions pgoutput reports, and rejection of malformed message sequences.
func TestLedgerDecoding(t *testing.T) {
	const (
		commitLSN = pglogrepl.LSN(0x1A00)
		endLSN    = pglogrepl.LSN(0x1A40)
	)

	transactionRelation := testTransactionRelation()
	tupleRelation := testTupleRelation()

	begin := &pglogrepl.BeginMessage{Xid: 900, FinalLSN: commitLSN}
	commit := &pglogrepl.CommitMessage{CommitLSN: commitLSN, TransactionEndLSN: endLSN}

	transactionInsert := &pglogrepl.InsertMessage{
		RelationID: transactionRelation.RelationID,
		Tuple:      transactionRowTuple("900"),
	}

	testCases := []struct {
		name        string
		messages    []pglogrepl.Message
		wantXid     uint64
		wantNoTxRow bool
		errContains string
	}{
		{
			name:     "a transaction row insert yields its transaction ID",
			messages: []pglogrepl.Message{transactionRelation, begin, transactionInsert, commit},
			wantXid:  900,
		},
		{
			// The ledger's own writes are updates, not inserts, so they reach it as
			// transactions with nothing to record.
			name:        "a transaction with no transaction row has nothing to record",
			messages:    []pglogrepl.Message{transactionRelation, begin, commit},
			wantNoTxRow: true,
		},
		{
			// Only the transaction table is published, so anything else can only
			// come from a publication someone widened; it is ignored rather than
			// misread.
			name: "inserts on other tables are ignored",
			messages: []pglogrepl.Message{
				transactionRelation, tupleRelation, begin,
				&pglogrepl.InsertMessage{RelationID: tupleRelation.RelationID, Tuple: relationshipTuple("subject", nullValue{}, testLiveSentinel)},
				transactionInsert,
				commit,
			},
			wantXid: 900,
		},
		{
			// The watch stamps live revisions with the position BEGIN reports, so
			// a disagreement between the two would position the same transaction
			// differently depending on which phase delivered it.
			name: "disagreeing commit positions are rejected",
			messages: []pglogrepl.Message{
				transactionRelation, begin, transactionInsert,
				&pglogrepl.CommitMessage{CommitLSN: commitLSN + 8, TransactionEndLSN: endLSN},
			},
			errContains: "disagreeing commit positions",
		},
		{
			name:        "an insert outside a transaction is rejected",
			messages:    []pglogrepl.Message{transactionRelation, transactionInsert},
			errContains: "INSERT outside of a transaction",
		},
		{
			name:        "a commit outside a transaction is rejected",
			messages:    []pglogrepl.Message{commit},
			errContains: "COMMIT outside of a transaction",
		},
		{
			name:        "an insert for an unknown relation is rejected",
			messages:    []pglogrepl.Message{begin, transactionInsert},
			errContains: "unknown relation OID",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			decoder := newLedgerDecoder()

			var committed *ledgerTransaction
			var err error
			for _, message := range tc.messages {
				committed, err = decoder.handleMessage(message)
				if err != nil {
					break
				}
			}

			if tc.errContains != "" {
				require.ErrorContains(t, err, tc.errContains)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, committed, "the commit must complete a transaction")

			require.Equal(t, commitLSN, committed.commitLSN)
			require.Equal(t, endLSN, committed.endLSN)
			if tc.wantNoTxRow {
				require.False(t, committed.hasTransactionRow)
				return
			}
			require.True(t, committed.hasTransactionRow)
			require.Equal(t, tc.wantXid, committed.xid.Uint64)
		})
	}

	t.Run("a transaction is only reported once", func(t *testing.T) {
		decoder := newLedgerDecoder()
		for _, message := range []pglogrepl.Message{transactionRelation, begin, transactionInsert} {
			committed, err := decoder.handleMessage(message)
			require.NoError(t, err)
			require.Nil(t, committed, "only a commit completes a transaction")
		}

		committed, err := decoder.handleMessage(commit)
		require.NoError(t, err)
		require.NotNil(t, committed)

		_, err = decoder.handleMessage(commit)
		require.ErrorContains(t, err, "COMMIT outside of a transaction")
	})
}

// TestLedgerIdleConfirmSafety covers the condition under which the ledger may
// confirm a stream position it recorded nothing at.
//
// Confirming a position asserts that everything at or below it is recorded, and
// the cursor watch turns that assertion into a delivery bound. Confirming while
// anything is pending or in flight therefore does not merely retain less WAL: it
// advertises transactions as delivered that were never recorded, and the watch
// steps over them in silence.
func TestLedgerIdleConfirmSafety(t *testing.T) {
	transactionRelation := testTransactionRelation()

	testCases := []struct {
		name string
		// prepare drives the batch and decoder into the state under test.
		prepare    func(batch *ledgerBatch, decoder *ledgerDecoder)
		wantAllows bool
	}{
		{
			name:       "an idle ledger may confirm what the stream reports",
			prepare:    func(*ledgerBatch, *ledgerDecoder) {},
			wantAllows: true,
		},
		{
			name: "a pending batch may not: its positions sit below the stream and are not durable",
			prepare: func(batch *ledgerBatch, _ *ledgerDecoder) {
				batch.add(ledgerPosition{xid: NewXid8(100), commitLSN: 0x100}, 0x108, time.Now())
			},
		},
		{
			name: "an open transaction may not: its records are seen but not yet a position",
			prepare: func(_ *ledgerBatch, decoder *ledgerDecoder) {
				_, err := decoder.handleMessage(&pglogrepl.BeginMessage{Xid: 900, FinalLSN: 0x200})
				require.NoError(t, err)
			},
		},
		{
			name: "both at once may not",
			prepare: func(batch *ledgerBatch, decoder *ledgerDecoder) {
				batch.add(ledgerPosition{xid: NewXid8(100), commitLSN: 0x100}, 0x108, time.Now())
				_, err := decoder.handleMessage(&pglogrepl.BeginMessage{Xid: 900, FinalLSN: 0x200})
				require.NoError(t, err)
			},
		},
		{
			name: "a drained batch may again",
			prepare: func(batch *ledgerBatch, _ *ledgerDecoder) {
				batch.add(ledgerPosition{xid: NewXid8(100), commitLSN: 0x100}, 0x108, time.Now())
				batch.take()
			},
			wantAllows: true,
		},
		{
			name: "a completed transaction may again",
			prepare: func(_ *ledgerBatch, decoder *ledgerDecoder) {
				for _, message := range []pglogrepl.Message{
					transactionRelation,
					&pglogrepl.BeginMessage{Xid: 900, FinalLSN: 0x200},
					&pglogrepl.InsertMessage{RelationID: transactionRelation.RelationID, Tuple: transactionRowTuple("900")},
					&pglogrepl.CommitMessage{CommitLSN: 0x200, TransactionEndLSN: 0x240},
				} {
					_, err := decoder.handleMessage(message)
					require.NoError(t, err)
				}
			},
			wantAllows: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			batch := newLedgerBatch(8, time.Second)
			decoder := newLedgerDecoder()

			tc.prepare(batch, decoder)

			require.Equal(t, tc.wantAllows, canConfirmLedgerIdle(batch, decoder))
		})
	}
}

// TestLedgerSlotInUseDetection covers the error the ledger reads as "another
// instance already holds the slot", which is how it elects a single writer.
func TestLedgerSlotInUseDetection(t *testing.T) {
	testCases := []struct {
		name  string
		err   error
		wantR bool
	}{
		{
			name:  "the slot is held by another session",
			err:   &pgconn.PgError{Code: pgObjectInUseErr, Message: `replication slot "spicedb_ledger" is active for PID 1`},
			wantR: true,
		},
		{
			name: "another PostgreSQL error is not slot contention",
			err:  &pgconn.PgError{Code: "42P01", Message: "relation does not exist"},
		},
		{
			name: "a non-PostgreSQL error is not slot contention",
			err:  errLedgerGenesisMissing,
		},
		{
			name: "no error at all",
			err:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantR, isReplicationSlotInUseError(tc.err))
		})
	}
}
