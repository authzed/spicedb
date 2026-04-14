package pool

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"

	log "github.com/authzed/spicedb/internal/logging"
)

// cancelHandler implements ctxwatch.Handler. When the context is cancelled, it
// sends a PostgreSQL cancel request to CRDB (so CRDB stops executing the query
// early), sets a deadline on the connection's net.Conn, and returns. It does NOT
// sleep 100ms like pgx's default CancelRequestContextWatcherHandler.
//
// The connection is NOT immediately safe to reuse after HandleCancel — it may
// still receive SQLSTATE 57014 on the next query. Callers must drain the
// connection (via drainCancelledConn in withRetries) before returning it to the
// pool.
type cancelHandler struct {
	pgConn        *pgconn.PgConn
	deadlineDelay time.Duration

	// ready is set to 1 by WithCancelHandler's AfterConnect hook once
	// pgxpool confirms the connection is fully established (pid/secretKey
	// written by connectOne). Zero means we are still inside connectOne and
	// must not call CancelRequest.
	ready atomic.Int32

	done   chan struct{}
	stopFn context.CancelFunc
}

var _ ctxwatch.Handler = (*cancelHandler)(nil)

// HandleCancel is called by pgx's context watcher when the context is cancelled.
// It sends a cancel request to CRDB in a goroutine and sets a net.Conn deadline
// so that the blocked Exec/Query returns promptly.
func (h *cancelHandler) HandleCancel(_ context.Context) {
	h.done = make(chan struct{})
	cancelCtx, stop := context.WithCancel(context.Background())
	h.stopFn = stop

	deadline := time.Now().Add(h.deadlineDelay)
	_ = h.pgConn.Conn().SetDeadline(deadline)

	doneCh := h.done // capture the channel value, not the pointer field
	go func() {
		defer close(doneCh)
		// Only send a cancel request once the connection is fully initialized.
		// CancelRequest reads the backend PID and secret key, which are written
		// by pgconn's connectOne during the startup handshake. If the context is
		// cancelled while a connection is still being established (e.g. on pool
		// close), those fields may not be written yet — a concurrent read is a
		// data race. The ready flag is set atomically by WithCancelHandler's
		// AfterConnect hook, which pgxpool calls after connectOne completes
		// successfully, establishing a happens-before: when ready == 1, all
		// startup writes are visible and CancelRequest is safe to call.
		// If not ready, the net.Conn deadline above is sufficient to unblock the
		// caller (connectOne returns an error; there is no server-side query to
		// cancel anyway).
		if h.ready.Load() == 1 {
			reqCtx, cancel := context.WithTimeout(cancelCtx, h.deadlineDelay)
			defer cancel()
			// Error intentionally ignored: if the cancel packet is not delivered,
			// CRDB continues executing. The net.Conn deadline is the fallback.
			// withRetries drains any stale 57014 synchronously before pool release.
			_ = h.pgConn.CancelRequest(reqCtx)
		}
		// Intentionally no sleep here — withRetries drains the cancel synchronously.
	}()
}

// HandleUnwatchAfterCancel is called by pgx's context watcher after the context
// watcher stops watching, once the cancelled query has returned. It stops the
// cancel request goroutine (if still running) and clears the net.Conn deadline.
func (h *cancelHandler) HandleUnwatchAfterCancel() {
	if h.stopFn != nil {
		h.stopFn() // aborts CancelRequest if the query returns before the goroutine finishes
	}
	if h.done != nil {
		<-h.done
	}
	// Clear the deadline so subsequent queries are not bounded by the wall-clock
	// deadline set during the cancel. Log a warning if this fails: a missed
	// deadline-clear would cause the next query to time out unexpectedly.
	if err := h.pgConn.Conn().SetDeadline(time.Time{}); err != nil {
		log.Warn().Err(err).Msg("cancelHandler: failed to clear net.Conn deadline after cancel; next query may timeout prematurely")
	}
	// Reset so that if this handler is reused for a future query (same connection,
	// same handler instance) the nil-guards in the next HandleCancel start clean.
	h.done = nil
	h.stopFn = nil
}
