package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
)

type lockName string

const (
	// gcRunLock is the lock name for the garbage collection run.
	gcRunLock lockName = "gc_run"
)

// sessionLock is a held MySQL advisory lock. MySQL advisory locks are
// session-scoped, so the lock is pinned to a dedicated pooled connection for
// its lifetime; running RELEASE_LOCK on any other pooled connection would
// silently no-op and leak the lock.
type sessionLock struct {
	conn *sql.Conn
	name lockName
}

// tryAcquireLock attempts to acquire the named advisory lock on a connection
// pinned for the lock's lifetime. It returns a non-nil sessionLock when
// acquired, or nil when the lock is held by another session.
func (mds *mysqlDatastore) tryAcquireLock(ctx context.Context, name lockName) (*sessionLock, error) {
	conn, err := mds.db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	// Acquire the lock, with max 1s timeout.
	// A lock obtained with GET_LOCK() is released explicitly by executing RELEASE_LOCK()
	//
	// NOTE: Lock is re-entrant, i.e. the same session can acquire the same lock multiple times.
	// > It is even possible for a given session to acquire multiple locks for the same name.
	// > Other sessions cannot acquire a lock with that name until the acquiring session releases all its locks for the name.
	// See: https://dev.mysql.com/doc/refman/8.4/en/locking-functions.html#function_get-lock
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK(?, 1)`, name).Scan(&acquired); err != nil {
		return nil, errors.Join(err, conn.Close())
	}

	if !acquired.Valid || acquired.Int64 != 1 {
		return nil, conn.Close()
	}

	return &sessionLock{conn: conn, name: name}, nil
}

// Release releases the lock on the session that acquired it and returns the
// pinned connection to the pool. If the release fails, the connection is
// discarded from the pool instead, which closes the session and frees the
// lock server-side.
func (l *sessionLock) Release(ctx context.Context) error {
	var released sql.NullInt64
	err := l.conn.QueryRowContext(ctx, `SELECT RELEASE_LOCK(?)`, l.name).Scan(&released)
	if err == nil && (!released.Valid || released.Int64 != 1) {
		// 0 means held by another session; NULL means the lock does not exist.
		// Neither should happen on the pinned connection.
		// See: https://dev.mysql.com/doc/refman/8.4/en/locking-functions.html#function_release-lock
		err = fmt.Errorf("advisory lock %q was not held by this session", l.name)
	}
	if err != nil {
		// Mark the pinned connection as bad so the pool discards it rather
		// than reusing a session that may still hold the lock.
		_ = l.conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = l.conn.Close()
		return err
	}
	return l.conn.Close()
}
