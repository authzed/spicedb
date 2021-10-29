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
	crdbRetryErrCode   = "40001"
	errUnableToRetry   = "failed to retry conflicted transaction: %w"
	errReachedMaxRetry = "maximum retries reached"
)

var retryHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "crdb_client_retries",
	Help:    "cockroachdb client-side retry distribution",
	Buckets: []float64{0, 1, 2, 5, 10, 20, 50},
})

func init() {
	prometheus.MustRegister(retryHistogram)
}

// conn is satisfied by both pgx.conn and pgxpool.Pool.
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

	var i int
	defer func() {
		retryHistogram.Observe(float64(i))
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

	for i = 0; i < maxRetries; i++ {
		if err = releasedFn(tx); err != nil {
			if !retriable(err) {
				return err
			}
			if _, retryErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT cockroach_restart"); retryErr != nil {
				return fmt.Errorf(errUnableToRetry, err)
			}
			continue
		}
		return nil
	}
	return errors.New(errReachedMaxRetry)
}

func retriable(err error) bool {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		log.Error().Err(err).Msg("error not retriable")
		return false
	}
	return pgerr.SQLState() == crdbRetryErrCode
}
