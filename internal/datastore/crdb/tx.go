package crdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
)

const (
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#restart-transaction
	crdbRetryErrCode = "40001"
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#result-is-ambiguous
	crdbAmbiguousErrorCode = "40003"
	// https://www.cockroachlabs.com/docs/stable/node-shutdown.html#connection-retry-loop
	crdbServerNotAcceptingClients = "57P01"
	// Error when SqlState is unknown
	crdbUnknownSQLState = "XXUUU"
	// Error message encountered when crdb nodes have large clock skew
	crdbClockSkewMessage = "cannot specify timestamp in the future"

	errReachedMaxRetries = "maximum retries reached (%d/%d): %w"
)

var resetHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
	Name:    "crdb_client_resets",
	Help:    "cockroachdb client-side tx reset distribution",
	Buckets: []float64{0, 1, 2, 5, 10, 20, 50},
})

func init() {
	prometheus.MustRegister(resetHistogram)
}

type innerFunc func(ctx context.Context) error

type executeTxRetryFunc func(context.Context, innerFunc) error

func executeWithMaxRetries(max uint8) executeTxRetryFunc {
	return func(ctx context.Context, fn innerFunc) (err error) {
		return executeWithResets(ctx, fn, max)
	}
}

func executeOnce(ctx context.Context, fn innerFunc) (err error) {
	return executeWithResets(ctx, fn, 0)
}

// executeWithResets executes transactionFn and resets the tx when ambiguous crdb errors are encountered.
func executeWithResets(ctx context.Context, fn innerFunc, maxRetries uint8) (err error) {
	var retries uint8
	defer func() {
		resetHistogram.Observe(float64(retries))
	}()

	for retries = 0; retries <= maxRetries; retries++ {
		err = fn(ctx)
		if resettable(ctx, err) {
			log.Ctx(ctx).Warn().Err(err).Msg("retrying resetteable database error")
			continue
		}

		// This can be nil or an un-resettable error.
		return err
	}

	// The last error was resettable but we're out of retries
	return fmt.Errorf(errReachedMaxRetries, retries, maxRetries+1, err)
}

func resettable(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	// detect when a cockroach node is taken out of service
	if strings.Contains(err.Error(), "broken pipe") {
		return true
	}
	// detect when cockroach closed a connection
	if strings.Contains(err.Error(), "unexpected EOF") {
		return true
	}
	sqlState := sqlErrorCode(ctx, err)
	// Ambiguous result error includes connection closed errors
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#result-is-ambiguous
	return sqlState == crdbAmbiguousErrorCode ||
		// Reset for retriable errors
		sqlState == crdbRetryErrCode ||
		// Retry on node draining
		sqlState == crdbServerNotAcceptingClients ||
		// Error encountered when crdb nodes have large clock skew
		(sqlState == crdbUnknownSQLState && strings.Contains(err.Error(), crdbClockSkewMessage))
}

// sqlErrorCode attempts to extract the crdb error code from the error state.
func sqlErrorCode(ctx context.Context, err error) string {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		log.Ctx(ctx).Debug().Err(err).Msg("couldn't determine a sqlstate error code")
		return ""
	}

	return pgerr.SQLState()
}
