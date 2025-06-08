package tidb

import "context"

type lockName string

const (
	// gcRunLock is the lock name for the garbage collection run.
	gcRunLock lockName = "gc_run"
)

func (ds *Datastore) tryAcquireLock(ctx context.Context, lockName lockName) (bool, error) {
	// Acquire the lock, with max 1s timeout.
	// A lock obtained with GET_LOCK() is released explicitly by executing RELEASE_LOCK()
	//
	// NOTE: Lock is re-entrant, i.e. the same session can acquire the same lock multiple times.
	// > It is even possible for a given session to acquire multiple locks for the same name.
	// > Other sessions cannot acquire a lock with that name until the acquiring session releases all its locks for the name.
	// See: https://docs.pingcap.com/tidb/stable/locking-functions#get_locklock_str-timeout
	row := ds.db.QueryRowContext(ctx, `
		SELECT GET_LOCK(?, 1)
	`, lockName)

	var acquired int
	if err := row.Scan(&acquired); err != nil {
		return false, err
	}

	return acquired == 1, nil
}

func (ds *Datastore) releaseLock(ctx context.Context, lockName lockName) error {
	// See: https://docs.pingcap.com/tidb/stable/locking-functions#release_locklock_str
	_, err := ds.db.ExecContext(ctx, `
		SELECT RELEASE_LOCK(?)
	`,
		lockName,
	)
	return err
}
