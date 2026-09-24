package common

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// countingPool is a WarmablePool that records how many times Acquire was
// called, so the warm-up target can be asserted without a database.
type countingPool struct {
	config   *pgxpool.Config
	acquires atomic.Int64
	err      error
}

func (c *countingPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	c.acquires.Add(1)
	if c.err != nil {
		return nil, c.err
	}

	// WarmupPool releases every connection it acquires, and a nil
	// *pgxpool.Conn cannot be released, so this fake never hands one back.
	// Every test here either sets err or has a target of zero.
	return nil, errors.New("countingPool must be given an error to return")
}

func (c *countingPool) Config() *pgxpool.Config { return c.config }

func TestWarmupPoolAcquiresTheConfiguredMinimum(t *testing.T) {
	t.Parallel()

	pool := &countingPool{
		config: &pgxpool.Config{MinConns: 7, MaxConns: 20},
		err:    errors.New("nope"),
	}

	err := WarmupPool(t.Context(), "read", pool)
	require.Error(t, err)

	// Acquire is attempted for every connection in the target. The errgroup
	// cancels its siblings on the first failure, so some of them may fail
	// before ever reaching the pool; what matters is that no more than the
	// target were ever started.
	require.LessOrEqual(t, pool.acquires.Load(), int64(7))
	require.Positive(t, pool.acquires.Load())
}

// TestWarmupPoolClampsToMaxConns covers a configuration that ConfigurePgx
// explicitly permits: a minimum higher than the maximum, which it only warns
// about. MaxConns is the hard size limit of the underlying resource pool, so
// warming up to MinConns would block until the deadline and never succeed.
func TestWarmupPoolClampsToMaxConns(t *testing.T) {
	t.Parallel()

	pool := &countingPool{
		config: &pgxpool.Config{MinConns: 50, MaxConns: 3},
		err:    errors.New("nope"),
	}

	err := WarmupPool(t.Context(), "read", pool)
	require.Error(t, err)
	require.LessOrEqual(t, pool.acquires.Load(), int64(3))
}

func TestWarmupPoolWithNoMinimumDoesNothing(t *testing.T) {
	t.Parallel()

	pool := &countingPool{config: &pgxpool.Config{MinConns: 0, MaxConns: 20}}

	require.NoError(t, WarmupPool(t.Context(), "read", pool))
	require.Zero(t, pool.acquires.Load())
}

// TestWarmupPoolErrorNamesThePoolAndTheTarget checks the operator-facing part
// of the failure: the message has to say which pool fell short and by how
// much, because that is what tells an operator whether to lower the minimum
// connection count or raise the database's connection limit.
func TestWarmupPoolErrorNamesThePoolAndTheTarget(t *testing.T) {
	t.Parallel()

	underlying := errors.New("FATAL: sorry, too many clients already")
	pool := &countingPool{
		config: &pgxpool.Config{MinConns: 20, MaxConns: 20},
		err:    underlying,
	}

	err := WarmupPool(t.Context(), "write", pool)
	require.Error(t, err)
	require.ErrorIs(t, err, underlying)
	require.Contains(t, err.Error(), "write connection pool")
	require.Contains(t, err.Error(), "minimum of 20 connections")
}
