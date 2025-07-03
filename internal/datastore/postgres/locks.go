package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type lockID uint32

const (
	// gcRunLock is the lock ID for the garbage collection run.
	gcRunLock lockID = 1

	// revisionHeartbeatLock is the lock ID for the leader that will generate the heartbeat revisions.
	revisionHeartbeatLock lockID = 2
)

func (pgd *pgDatastore) tryAcquireLock(ctx context.Context, conn *pgxpool.Conn, lockID lockID) (bool, error) {
	// Acquire the lock.
	//
	// NOTE: The lock is re-entrant, i.e. the same session can acquire the same lock multiple times.
	// > A lock can be acquired multiple times by its owning process; for each completed lock request
	// > there must be a corresponding unlock request before the lock is actually released
	// > If a session already holds a given advisory lock, additional requests by it will always succeed,
	// > even if other sessions are awaiting the lock; this statement is true regardless of whether the
	// > existing lock hold and new request are at session level or transaction level.
	// See: https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS
	row := conn.QueryRow(ctx, `
		SELECT pg_try_advisory_lock($1)
	`, lockID)

	var lockAcquired bool
	if err := row.Scan(&lockAcquired); err != nil {
		return false, err
	}
	return lockAcquired, nil
}

func (pgd *pgDatastore) releaseLock(ctx context.Context, conn *pgxpool.Conn, lockID lockID) error {
	row := conn.QueryRow(ctx, `
		SELECT pg_advisory_unlock($1)
	`, lockID)

	var lockReleased bool
	if err := row.Scan(&lockReleased); err != nil {
		return err
	}

	if !lockReleased {
		return fmt.Errorf("failed to release lock %d", lockID)
	}

	return nil
}
