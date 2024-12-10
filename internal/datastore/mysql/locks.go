package mysql

import "context"

type lockName string

const (
	// gcRunLock is the lock name for the garbage collection run.
	gcRunLock lockName = "gc_run"
)

func (mds *Datastore) tryAcquireLock(ctx context.Context, lockName lockName) (bool, error) {
	// Acquire the lock, with max 1s timeout.
	row := mds.db.QueryRowContext(ctx, `
		SELECT GET_LOCK(?, 1)
	`, lockName)

	var acquired int
	if err := row.Scan(&acquired); err != nil {
		return false, err
	}

	return acquired == 1, nil
}

func (mds *Datastore) releaseLock(ctx context.Context, lockName lockName) error {
	_, err := mds.db.ExecContext(ctx, `
		SELECT RELEASE_LOCK(?)
	`,
		lockName,
	)
	return err
}
