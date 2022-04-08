package crdb

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

const (
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#restart-transaction
	crdbRetryErrCode = "40001"
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#result-is-ambiguous
	crdbAmbiguousErrorCode = "40003"
	// Error when SqlState is unknown
	crdbUnknownSQLState = "XXUUU"
	// Error message encountered when crdb nodes have large clock skew
	crdbClockSkewMessage = "cannot specify timestamp in the future"

	errReachedMaxRetry = "maximum retries reached"
)

var resetHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "crdb_client_resets",
	Help:    "cockroachdb client-side tx reset distribution",
	Buckets: []float64{0, 1, 2, 5, 10, 20, 50},
})

func init() {
	prometheus.MustRegister(resetHistogram)
}

type conn interface {
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type transactionFn func(tx pgx.Tx) error

type executeTxRetryFunc func(context.Context, conn, pgx.TxOptions, transactionFn) error

func executeWithMaxRetries(max int) executeTxRetryFunc {
	return func(ctx context.Context, conn conn, txOptions pgx.TxOptions, fn transactionFn) (err error) {
		return executeWithResets(ctx, conn, txOptions, fn, max)
	}
}

// executeWithResets executes transactionFn and resets the tx when ambiguous crdb errors are encountered.
func executeWithResets(ctx context.Context, conn conn, txOptions pgx.TxOptions, fn transactionFn, maxRetries int) (err error) {
	var resets int
	defer func() {
		resetHistogram.Observe(float64(resets))
	}()

	var tx pgx.Tx
	defer func() {
		if tx == nil {
			return
		}

		if err == nil {
			commitErr := tx.Commit(ctx)
			if commitErr == nil {
				return
			}
			log.Err(commitErr).Msg("failed tx commit")
		}

		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			log.Err(rollbackErr).Msg("error during tx rollback")
		}
	}()

	// NOTE: n maxRetries can yield n+1 executions of the transaction fn
	for resets = 0; resets <= maxRetries; resets++ {
		tx, err = resetExecution(ctx, conn, tx, txOptions)
		if err != nil {
			log.Err(err).Msg("error resetting transaction")
			if resetable(ctx, err) {
				continue
			} else {
				return
			}
		}

		if err = fn(tx); resetable(ctx, err) {
			log.Err(err).Msg("resettable error, will attempt to reset tx")
			continue
		}

		return
	}
	err = errors.New(errReachedMaxRetry)
	return
}

// resetExecution attempts to rollback the given tx and begins a new tx with a new connection.
func resetExecution(ctx context.Context, conn conn, tx pgx.Tx, txOptions pgx.TxOptions) (newTx pgx.Tx, err error) {
	log.Debug().Msg("attempting to initialize new tx")
	if tx != nil {
		err = tx.Rollback(ctx)
		if err != nil {
			return nil, err
		}
	}
	newTx, err = conn.BeginTx(ctx, txOptions)
	if err != nil {
		return nil, err
	}

	return newTx, nil
}

func resetable(ctx context.Context, err error) bool {
	sqlState := sqlErrorCode(ctx, err)
	// Ambiguous result error includes connection closed errors
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#result-is-ambiguous
	return sqlState == crdbAmbiguousErrorCode ||
		// Reset for retriable errors
		sqlState == crdbRetryErrCode ||
		// Error encountered when crdb nodes have large clock skew
		(sqlState == crdbUnknownSQLState && strings.Contains(err.Error(), crdbClockSkewMessage))
}

// sqlErrorCode attempts to extract the crdb error code from the error state.
func sqlErrorCode(ctx context.Context, err error) string {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		log.Debug().Err(err).Msg("couldn't determine a sqlstate error code")
		return ""
	}

	return pgerr.SQLState()
}
