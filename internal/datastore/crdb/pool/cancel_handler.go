package pool

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	log "github.com/authzed/spicedb/internal/logging"
)

// deadlineSetter is the subset of net.Conn the handler needs; it exists so
// the handler can be unit tested without a live connection.
type deadlineSetter interface {
	SetDeadline(t time.Time) error
}

// sessionCanceler cancels all queries running on a tracked session. It is
// implemented by *Canceler.
type sessionCanceler interface {
	CancelSessionQueries(pgConn *pgconn.PgConn) error
}

// CancelQueryContextWatcherHandler responds to context cancellation during an
// in-flight pgconn operation by issuing CockroachDB's in-band CANCEL QUERIES
// statement from a sibling connection (via the Canceler), so the query stops
// promptly without destroying this connection.
//
// Sequencing guarantee: HandleUnwatchAfterCancel blocks until the CANCEL
// statement round trip completes. pgconn invokes it before the canceled
// operation returns to the caller, and the pool cannot release the connection
// before the operation returns. A cancellation therefore can never still be
// in flight when a later query runs on this session — unlike the pgwire
// cancel protocol, which CockroachDB applies asynchronously after closing the
// cancel connection.
//
// If the CANCEL statement fails or times out, the connection is poisoned with
// an immediate deadline: its next use errors out, the pool destroys it, and
// the pool-level retry logic transparently moves the work to another
// connection. This degrades to the legacy close-the-connection behavior.
type CancelQueryContextWatcherHandler struct {
	conn          deadlineSetter
	pgConn        *pgconn.PgConn
	canceler      sessionCanceler
	deadlineDelay time.Duration

	cancelFinished  chan struct{}
	cancelSucceeded atomic.Bool
}

// NewCancelQueryContextWatcherHandler builds a handler for a connection.
// deadlineDelay is the fallback net.Conn deadline applied when a context is
// canceled: if in-band cancellation does not resolve the in-flight operation
// before it elapses, the pending read errors out and the connection is
// destroyed. It must comfortably exceed the canceler's statement timeout.
func NewCancelQueryContextWatcherHandler(pgConn *pgconn.PgConn, canceler sessionCanceler, deadlineDelay time.Duration) *CancelQueryContextWatcherHandler {
	return &CancelQueryContextWatcherHandler{
		conn:          pgConn.Conn(),
		pgConn:        pgConn,
		canceler:      canceler,
		deadlineDelay: deadlineDelay,
	}
}

// HandleCancel implements ctxwatch.Handler.
func (h *CancelQueryContextWatcherHandler) HandleCancel(_ context.Context) {
	h.cancelFinished = make(chan struct{})
	h.cancelSucceeded.Store(false)

	// Fallback: if in-band cancellation fails to resolve the in-flight
	// operation, this deadline unblocks the pending read so the connection
	// errors out and is destroyed.
	_ = h.conn.SetDeadline(time.Now().Add(h.deadlineDelay))

	go func() {
		defer close(h.cancelFinished)
		if err := h.canceler.CancelSessionQueries(h.pgConn); err != nil {
			log.Warn().Err(err).Msg("in-band query cancellation failed; connection will be discarded")
			return
		}
		h.cancelSucceeded.Store(true)
	}()
}

// HandleUnwatchAfterCancel implements ctxwatch.Handler.
func (h *CancelQueryContextWatcherHandler) HandleUnwatchAfterCancel() {
	// Block until the CANCEL statement completes. Releasing the connection
	// any earlier would allow a still-in-flight CANCEL to hit whatever query
	// runs next on this session.
	<-h.cancelFinished

	if h.cancelSucceeded.Load() {
		_ = h.conn.SetDeadline(time.Time{})
		return
	}
	// The CANCEL statement failed or timed out, so a cancellation may still
	// be in flight for this session. Poison the connection so it is
	// destroyed instead of reused.
	_ = h.conn.SetDeadline(time.Now())
}
