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
	crdbRetryErrCode       = "40001"
	crdbAmbiguousErrorCode = "40003"
	errUnableToRetry       = "failed to retry conflicted transaction: %w"
	errReachedMaxRetry     = "maximum retries reached"
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

func executeWithMaxRetries(max int) executeTxRetryFunc {
	return func(ctx context.Context, conn conn, txOptions pgx.TxOptions, fn transactionFn) (err error) {
		return execute(ctx, conn, txOptions, fn, max)
	}
}

// adapted from https://github.com/cockroachdb/cockroach-go
func execute(ctx context.Context, conn conn, txOptions pgx.TxOptions, fn transactionFn, maxRetries int) (err error) {
	var tx pgx.Tx
	tx, err = conn.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			_ = tx.Commit(ctx)
			return
		}
		_ = tx.Rollback(ctx)
	}()

	if _, err = tx.Exec(ctx, "SAVEPOINT cockroach_restart"); err != nil {
		return
	}

	var i, resets int
	defer func() {
		retryHistogram.Observe(float64(i))
		resetHistogram.Observe(float64(resets))
	}()

	releasedFn := func(tx pgx.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}

		// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
		// opportunity to react to retryable errors, whereas tx.Commit() doesn't.

		// RELEASE SAVEPOINT itself can fail, in which case the entire
		// transaction needs to be retried
		if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT cockroach_restart"); err != nil {
			return err
		}
		return nil
	}

	for i = 0; i < maxRetries+1; i++ {
		if err = releasedFn(tx); err != nil {
			if retriable(ctx, err) {
				if _, retryErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT cockroach_restart"); retryErr != nil {
					// Attempt to reset on failed retries
					newTx, resetErr := resetExecution(ctx, conn, &tx, txOptions)
					if resetErr != nil {
						return fmt.Errorf(errUnableToRetry, err)
					}
					tx = newTx
					resets++
				}
				continue
			} else if resetable(ctx, err) {
				newTx, resetErr := resetExecution(ctx, conn, &tx, txOptions)
				if resetErr != nil {
					return fmt.Errorf(errUnableToRetry, err)
				}
				tx = newTx
				resets++
				continue
			}
			return err
		}
		return nil
	}
	return errors.New(errReachedMaxRetry)
}

// tx will be rolled back and a new tx will be started
func resetExecution(ctx context.Context, conn conn, tx *pgx.Tx, txOptions pgx.TxOptions) (newTx pgx.Tx, err error) {
	err = (*tx).Rollback(ctx)
	if err != nil {
		return nil, err
	}
	newTx, err = conn.BeginTx(ctx, txOptions)
	if err != nil {
		return nil, err
	}
	if _, err = newTx.Exec(ctx, "SAVEPOINT cockroach_restart"); err != nil {
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

func sqlErrorCode(ctx context.Context, err error) string {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		log.Ctx(ctx).Info().Err(err).Msg("couldn't determine a sqlstate error code")
		return ""
	}

	return pgerr.SQLState()
}
