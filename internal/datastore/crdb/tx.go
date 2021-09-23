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

var retryCount = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "crdb_client_retry_total",
		Help: "increases each time a crdb transaction is retried client-side",
	},
)

func init() {
	prometheus.MustRegister(retryCount)
}

// Conn is satisfied by both pgx.Conn and pgxpool.Pool.
type Conn interface {
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type TransactionFn func(tx pgx.Tx) error

type ExecuteTxRetryFunc func(context.Context, Conn, pgx.TxOptions, TransactionFn) error

func ExecuteWithMaxRetries(max int) ExecuteTxRetryFunc {
	return func(ctx context.Context, conn Conn, txOptions pgx.TxOptions, fn TransactionFn) (err error) {
		return execute(ctx, conn, txOptions, fn, max)
	}
}

func execute(ctx context.Context, conn Conn, txOptions pgx.TxOptions, fn TransactionFn, maxRetries int) (err error) {
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

	for i := 0; i < maxRetries; i++ {
		if err = fn(tx); err != nil {
			if !retriable(err) {
				return err
			}
			if _, retryErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT cockroach_restart"); retryErr != nil {
				return fmt.Errorf(errUnableToRetry, err)
			}
			retryCount.Inc()
			continue
		}
		if _, err = tx.Exec(ctx, "RELEASE SAVEPOINT cockroach_restart"); err == nil {
			return nil
		}
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
