package postgres

import "context"

type lockID uint32

const (
	// gcRunLock is the lock ID for the garbage collection run.
	gcRunLock lockID = 1
)

func (pgd *pgDatastore) tryAcquireLock(ctx context.Context, lockID lockID) (bool, error) {
	// Acquire the lock.
	row := pgd.writePool.QueryRow(ctx, `
		SELECT pg_try_advisory_lock($1)
	`, lockID)

	var lockAcquired bool
	if err := row.Scan(&lockAcquired); err != nil {
		return false, err
	}
	return lockAcquired, nil
}

func (pgd *pgDatastore) releaseLock(ctx context.Context, lockID lockID) error {
	_, err := pgd.writePool.Exec(ctx, `
		SELECT pg_advisory_unlock($1)
	`, lockID)
	return err
}
