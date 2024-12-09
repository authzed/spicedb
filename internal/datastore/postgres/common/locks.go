package common

import (
	"time"

	"cirello.io/pglock"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

const (
	locksTableName     = "locks"
	heartbeatFrequency = 2 * time.Second
)

// RunWithLocksClient runs the provided function with a pglock.Client. Should only be used
// for migrations.
func RunWithLocksClient(conn *pgx.Conn, runner func(client *pglock.Client) error) error {
	db := stdlib.OpenDB(*conn.Config())
	defer db.Close()

	client, err := pglock.UnsafeNew(db, pglock.WithCustomTable(locksTableName))
	if err != nil {
		return err
	}

	if err := runner(client); err != nil {
		return err
	}

	return nil
}

// RunWithLocksClientOverPool runs the provided function with a pglock.Client.
func RunWithLocksClientOverPool(pool *pgxpool.Pool, timeout time.Duration, runner func(client *pglock.Client) error) error {
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()

	client, err := pglock.UnsafeNew(db,
		pglock.WithCustomTable(locksTableName),
		pglock.WithLeaseDuration(timeout),
		pglock.WithHeartbeatFrequency(heartbeatFrequency),
	)
	if err != nil {
		return err
	}

	if err := runner(client); err != nil {
		return err
	}

	return nil
}
