package crdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

const (
	crdbRetryErrCode       = "40001"
	crdbAmbiguousErrorCode = "40003"
	errReachedMaxRetry     = "maximum retries reached"
)

type RetryError struct {
	err error
}

func (e RetryError) Error() string {
	return fmt.Errorf("failed to retry conflicted transaction: %w", e.err).Error()
}
func (e RetryError) Unwrap() error {
	return e.err
}

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

// conn is satisfied by both pgx.conn and pgxpool.Pool.
type conn interface {
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type transactionFn func(tx pgx.Tx) error

type executeTxRetryFunc func(context.Context, conn, pgx.TxOptions, transactionFn) error

func executeWithMaxRetries(max int) executeTxRetryFunc {
	return func(ctx context.Context, conn conn, txOptions pgx.TxOptions, fn transactionFn) error {
		_, err := execute(ctx, conn, txOptions, fn, max)
		return err
	}
}

func executeWithMaxResetsAndRetries(maxRetries int) executeTxRetryFunc {
	return func(ctx context.Context, conn conn, txOptions pgx.TxOptions, fn transactionFn) (err error) {
		pool, ok := conn.(*pgxpool.Pool)
		if !ok {
			panic("connections cannot be reset without a pool")
		}
		var tries, resets int
		defer func() {
			retryHistogram.Observe(float64(tries))
			resetHistogram.Observe(float64(resets))
		}()
		var txConn *pgxpool.Conn
		for tries = 0; tries < maxRetries; tries++ {
			txConn, err = pool.Acquire(ctx)
			if err != nil {
				resets++
				continue
			}
			// this ties the overall reset + retry count together
			tries, err = execute(ctx, txConn, txOptions, fn, maxRetries)
			if err == nil {
				return
			}
			if resetable(ctx, err) {
				resets++
				continue
			}
			// non-resettable error
			err = RetryError{err: err}
			return
		}
		err = errors.New(errReachedMaxRetry)
		return
	}
}

// adapted from https://github.com/cockroachdb/cockroach-go
func execute(ctx context.Context, conn conn, txOptions pgx.TxOptions, fn transactionFn, maxRetries int) (tries int, err error) {
	var tx pgx.Tx
	tx, err = conn.BeginTx(ctx, txOptions)
	if err != nil {
		return
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

	for tries = 0; tries < maxRetries; tries++ {
		if err = releasedFn(tx); err != nil {
			if !retriable(ctx, err) {
				return
			}
			if _, retryErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT cockroach_restart"); retryErr != nil {
				err = RetryError{err: err}
				return
			}
			continue
		}
		return
	}
	err = errors.New(errReachedMaxRetry)
	return
}

func retriable(ctx context.Context, err error) bool {
	return sqlErrorCode(ctx, err) == crdbRetryErrCode
}

func resetable(ctx context.Context, err error) bool {
	// errors with underlying retries can be reset
	if errors.As(err, &RetryError{}) {
		return true
	}
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
