package pool

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#restart-transaction
	CrdbRetryErrCode = "40001"
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#result-is-ambiguous
	CrdbAmbiguousErrorCode = "40003"
	// https://www.cockroachlabs.com/docs/stable/node-shutdown.html#connection-retry-loop
	CrdbServerNotAcceptingClients = "57P01"
	// Error when SqlState is unknown
	CrdbUnknownSQLState = "XXUUU"
	// Error message encountered when crdb nodes have large clock skew
	CrdbClockSkewMessage = "cannot specify timestamp in the future"
)

// MaxRetryError is returned when the retry budget is exhausted.
type MaxRetryError struct {
	MaxRetries uint8
	LastErr    error
}

func (e *MaxRetryError) Error() string {
	if e.MaxRetries == 0 {
		return "retries disabled: " + e.LastErr.Error()
	}
	return fmt.Sprintf("max retries reached (%d): %s", e.MaxRetries, e.LastErr.Error())
}

func (e *MaxRetryError) Unwrap() error { return e.LastErr }

// ResettableError is an error that we think may succeed if retried against a new connection.
type ResettableError struct {
	Err error
}

func (e *ResettableError) Error() string { return "resettable error" + ": " + e.Err.Error() }
func (e *ResettableError) Unwrap() error { return e.Err }

// RetryableError is an error that can be retried against the existing connection.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string { return "retryable error" + ": " + e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// sqlErrorCode attempts to extract the crdb error code from the error state.
func sqlErrorCode(err error) string {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		return ""
	}

	return pgerr.SQLState()
}
