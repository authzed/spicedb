package pool

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#restart-transaction
	CrdbRetryErrCode = "40001"
	// CrdbQueryCanceledErrCode is the SQLSTATE returned when a query is
	// canceled, e.g. via CANCEL QUERIES or the pgwire cancel protocol.
	CrdbQueryCanceledErrCode = "57014"
	// https://www.cockroachlabs.com/docs/stable/common-errors.html#result-is-ambiguous
	CrdbAmbiguousErrorCode = "40003"
	// https://www.cockroachlabs.com/docs/stable/node-shutdown.html#connection-retry-loop
	CrdbServerNotAcceptingClients = "57P01"
	// Error when SqlState is unknown
	CrdbUnknownSQLState = "XXUUU"
	// Error message encountered when crdb nodes have large clock skew
	CrdbClockSkewMessage = "cannot specify timestamp in the future"
)

// datastoreUnavailableMessage is the engine-agnostic, client-facing message returned for
// transient CockroachDB failures. The underlying driver error may embed datastore internals
// (e.g. SQLSTATE codes); it is logged server-side rather than surfaced to the client.
const datastoreUnavailableMessage = "the datastore is temporarily unavailable; please retry"

// MaxRetryError is returned when the retry budget is exhausted.
type MaxRetryError struct {
	MaxRetries uint8
	LastErr    error
}

func (e *MaxRetryError) Error() string {
	if e.MaxRetries == 0 {
		if e.LastErr != nil {
			return "retries disabled: " + e.LastErr.Error()
		}
		return "retries disabled"
	}
	if e.LastErr == nil {
		return fmt.Sprintf("max retries reached (%d)", e.MaxRetries)
	}
	return fmt.Sprintf("max retries reached (%d): %s", e.MaxRetries, e.LastErr.Error())
}

func (e *MaxRetryError) Unwrap() error { return e.LastErr }

func (e *MaxRetryError) GRPCStatus() *status.Status {
	s, ok := status.FromError(e.Unwrap())
	if !ok {
		return nil
	}

	return s
}

// ResettableError is an error that we think may succeed if retried against a new connection.
type ResettableError struct {
	Err error
}

func (e *ResettableError) Error() string {
	if e.Err == nil {
		return "resettable error"
	}
	return "resettable error" + ": " + e.Err.Error()
}

func (e *ResettableError) Unwrap() error { return e.Err }

func (e *ResettableError) GRPCStatus() *status.Status {
	// Return unavailable so clients know it's ok to retry, but with an engine-agnostic
	// message: the wrapped driver error may leak datastore internals and is logged
	// server-side rather than returned to the client.
	return status.New(codes.Unavailable, datastoreUnavailableMessage)
}

// RetryableError is an error that can be retried against the existing connection.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	if e.Err == nil {
		return "retryable error"
	}
	return "retryable error" + ": " + e.Err.Error()
}
func (e *RetryableError) Unwrap() error { return e.Err }

func (e *RetryableError) GRPCStatus() *status.Status {
	// Return unavailable so clients know it's ok to retry, but with an engine-agnostic
	// message: the wrapped driver error may leak datastore internals and is logged
	// server-side rather than returned to the client.
	return status.New(codes.Unavailable, datastoreUnavailableMessage)
}

// sqlErrorCode attempts to extract the crdb error code from the error state.
func sqlErrorCode(err error) string {
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) {
		return ""
	}

	return pgerr.SQLState()
}
