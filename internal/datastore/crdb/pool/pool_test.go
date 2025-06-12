package pool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

type fakePgxPool struct {
	Pool
	acquired int
}

func (m *fakePgxPool) Acquire(_ context.Context) (*pgxpool.Conn, error) {
	m.acquired++
	return &pgxpool.Conn{}, nil
}

func TestWithRetries(t *testing.T) {
	originalClosedFunc := isConnClosed
	t.Cleanup(func() {
		isConnClosed = originalClosedFunc
	})

	isConnClosed = func(conn *pgxpool.Conn) bool {
		return true
	}

	originalGetConnFunc := getConn
	t.Cleanup(func() {
		getConn = originalGetConnFunc
	})

	conn1 := &pgx.Conn{}
	getConn = func(conn *pgxpool.Conn) *pgx.Conn {
		return conn1
	}

	tracker := &NodeHealthTracker{
		healthyNodes:  make(map[uint32]struct{}),
		nodesEverSeen: make(map[uint32]*rate.Limiter),
		newLimiter: func() *rate.Limiter {
			return rate.NewLimiter(rate.Every(1*time.Minute), 0)
		},
	}
	fakePool := &fakePgxPool{}
	tracker.SetNodeHealth(1, true)
	rp := RetryPool{
		pool:          fakePool,
		healthTracker: tracker,
		nodeForConn:   map[*pgx.Conn]uint32{conn1: 1},
		gc:            make(map[*pgx.Conn]struct{}),
	}

	// Test that a closed connection caused by context cancellation does not count towards health check
	err := rp.withRetries(context.Background(), func(conn *pgxpool.Conn) error {
		return context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, tracker.IsHealthy(1))
	require.Equal(t, 1, fakePool.acquired)

	// Test that a closed connection due to a non-context error reports to the health check.
	// Given the limiter is set to 0, it should mark the node unhealthy immediately
	fakePool.acquired = 0
	foobarErr := errors.New("foobar")
	err = rp.withRetries(context.Background(), func(conn *pgxpool.Conn) error {
		return foobarErr
	})
	require.Equal(t, err, &MaxRetryError{LastErr: foobarErr})
	require.False(t, tracker.IsHealthy(1))
	require.Equal(t, 1, fakePool.acquired) // It does not try to acquire from a new node because it's not retrying

	// Test that a resettable error with open connection reports to the health check
	fakePool.acquired = 0
	tracker.SetNodeHealth(1, true)
	rp.nodeForConn[conn1] = 1
	resettableErr := ResettableError{Err: foobarErr}
	err = rp.withRetries(context.Background(), func(conn *pgxpool.Conn) error {
		return &resettableErr
	})
	require.Equal(t, err, &MaxRetryError{LastErr: &resettableErr})
	require.False(t, tracker.IsHealthy(1))
	require.Equal(t, 1, fakePool.acquired) // It does not try to acquire from a new node because it's not retrying

	// Test retries
	fakePool.acquired = 0
	tracker.SetNodeHealth(1, true)
	rp.nodeForConn[conn1] = 1
	rp.maxRetries = 1
	err = rp.withRetries(context.Background(), func(conn *pgxpool.Conn) error {
		return &resettableErr
	})
	require.Equal(t, err, &MaxRetryError{MaxRetries: 1, LastErr: &resettableErr})
	require.False(t, tracker.IsHealthy(1))
	require.Equal(t, 2, fakePool.acquired) // attempts to acquire a new connection after the first failure
}
