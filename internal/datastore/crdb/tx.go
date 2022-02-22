package crdb

import (
	"context"
	"errors"
	"fmt"

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

	errReachedMaxRetry = "maximum retries reached"

	sqlRollback         = "ROLLBACK TO SAVEPOINT cockroach_restart"
	sqlSavepoint        = "SAVEPOINT cockroach_restart"
	sqlReleaseSavepoint = "RELEASE SAVEPOINT cockroach_restart"
)

var retryHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "crdb_client_retries",
	Help:    "cockroachdb client-side retry distribution",
	Buckets: []float64{0, 1, 2, 5, 10, 20, 50},
})

var resetHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "crdb_client_resets",
	Help:    "cockroachdb client-side tx reset distribution",
	Buckets: []float64{0, 1, 2, 5, 10, 20, 50},
})

func init() {
	prometheus.MustRegister(retryHistogram)
	prometheus.MustRegister(resetHistogram)
}

type conn interface {
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type transactionFn func(tx pgx.Tx) error

type executeTxRetryFunc func(context.Context, conn, pgx.TxOptions, transactionFn) error

// RetryError wraps an error that prevented the transaction function from being retried.
type RetryError struct {
	err error
}

// Error returns the wrapped error
func (e RetryError) Error() string {
	return fmt.Errorf("unable to retry conflicted transaction: %w", e.err).Error()
}

// Unwrap returns the wrapped, non-retriable error
func (e RetryError) Unwrap() error {
	return e.err
}

func executeWithMaxRetries(max int) executeTxRetryFunc {
	return func(ctx context.Context, conn conn, txOptions pgx.TxOptions, fn transactionFn) (err error) {
		return executeWithResets(ctx, conn, txOptions, fn, max)
	}
}

// executeWithResets executes transactionFn and resets the tx when ambiguous crdb errors are encountered.
func executeWithResets(ctx context.Context, conn conn, txOptions pgx.TxOptions, fn transactionFn, maxRetries int) (err error) {
	var retries, resets int
	defer func() {
		retryHistogram.Observe(float64(retries))
		resetHistogram.Observe(float64(resets))
	}()

	var tx pgx.Tx
	defer func() {
		if tx != nil {
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
		}
	}()

	// NOTE: n maxRetries can yield n+1 executions of the transaction fn
	for retries = 0; retries <= maxRetries; retries++ {
		tx, err = resetExecution(ctx, conn, tx, txOptions)
		if err != nil {
			log.Err(err).Msg("error resetting transaction")
			resets++
			continue
		}

		retryAttempts, txErr := executeWithRetries(ctx, tx, fn, maxRetries-retries)
		retries += retryAttempts
		if txErr == nil {
			return
		}
		err = txErr

		if resetable(ctx, err) {
			log.Err(err).Msg("resettable error, will attempt to reset tx")
			resets++
			continue
		}

		return
	}
	err = errors.New(errReachedMaxRetry)
	return
}

// executeWithRetries executes the transaction fn and attempts to retry up to maxRetries.
// adapted from https://github.com/cockroachdb/cockroach-go/blob/05d7aaec086fe3288377923bf9a98648d29c44c6/crdb/tx.go#L95
func executeWithRetries(ctx context.Context, currentTx pgx.Tx, fn transactionFn, maxRetries int) (retries int, err error) {
	releasedFn := func(tx pgx.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}

		// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
		// opportunity to react to retryable errors, whereas tx.Commit() doesn't.

		// RELEASE SAVEPOINT itself can fail, in which case the entire
		// transaction needs to be retried
		if _, err := tx.Exec(ctx, sqlReleaseSavepoint); err != nil {
			return err
		}
		return nil
	}

	var i int
	for i = 0; i <= maxRetries; i++ {
		if err = releasedFn(currentTx); err != nil {
			if retriable(ctx, err) {
				if _, retryErr := currentTx.Exec(ctx, sqlRollback); retryErr != nil {
					return i, RetryError{err: retryErr}
				}
				continue
			}
			return i, err
		}
		return i, nil
	}
	return i, errors.New(errReachedMaxRetry)
}

// resetExecution attempts to rollback the given tx, begins a new tx with a new connection, and creates a savepoint.
func resetExecution(ctx context.Context, conn conn, tx pgx.Tx, txOptions pgx.TxOptions) (newTx pgx.Tx, err error) {
	log.Info().Msg("attempting to initialize new tx")
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
	if _, err = newTx.Exec(ctx, sqlSavepoint); err != nil {
		return nil, err
	}

	return newTx, nil
}

func retriable(ctx context.Context, err error) bool {
	return sqlErrorCode(ctx, err) == crdbRetryErrCode
}

func resetable(ctx context.Context, err error) bool {
	// Ambiguous result error includes connection closed errors
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#result-is-ambiguous
	return sqlErrorCode(ctx, err) == crdbAmbiguousErrorCode
}

// sqlErrorCode attenmpts to extract the crdb error code from the error state.
func sqlErrorCode(ctx context.Context, err error) string {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		log.Info().Err(err).Msg("couldn't determine a sqlstate error code")
		return ""
	}

	return pgerr.SQLState()
}
