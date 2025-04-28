package common

import (
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
)

// CancelationContextHandler returns a context watcher handler for canceling requests
// when a context is canceled, rather than closing connections.
func CancelationContextHandler(pgConn *pgconn.PgConn) ctxwatch.Handler {
	return &pgconn.CancelRequestContextWatcherHandler{
		Conn:               pgConn,
		CancelRequestDelay: 50 * time.Millisecond, // Cancel immediately.
		DeadlineDelay:      1 * time.Second,       // If not acknowledged, close the connection after a second.
	}
}
