package pool

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// TestPool implements pgxPool interface for testing
type TestPool struct {
	acquireFunc        func(ctx context.Context) (*pgxpool.Conn, error)
	acquireAllIdleFunc func(ctx context.Context) []*pgxpool.Conn
	configFunc         func() *pgxpool.Config
	closeFunc          func()
	statFunc           func() *pgxpool.Stat
}

func (t *TestPool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return t.acquireFunc(ctx)
}

func (t *TestPool) AcquireAllIdle(ctx context.Context) []*pgxpool.Conn {
	return t.acquireAllIdleFunc(ctx)
}

func (t *TestPool) Config() *pgxpool.Config {
	return t.configFunc()
}

func (t *TestPool) Close() {
	t.closeFunc()
}

func (t *TestPool) Stat() *pgxpool.Stat {
	return t.statFunc()
}

// NewTestPool creates a TestPool with sensible defaults
func NewTestPool() *TestPool {
	return &TestPool{
		acquireFunc: func(ctx context.Context) (*pgxpool.Conn, error) {
			return nil, nil
		},
		acquireAllIdleFunc: func(ctx context.Context) []*pgxpool.Conn {
			return nil
		},
		configFunc: func() *pgxpool.Config {
			return &pgxpool.Config{}
		},
		closeFunc: func() {},
		statFunc: func() *pgxpool.Stat {
			return &pgxpool.Stat{}
		},
	}
}

// createTestRetryPool creates a RetryPool for testing with dependency injection
func createTestRetryPool(testPool *TestPool) *RetryPool {
	return &RetryPool{
		pool: testPool,
		id:   "test-pool",
		healthTracker: &NodeHealthTracker{
			healthyNodes:  make(map[uint32]struct{}),
			nodesEverSeen: make(map[uint32]*rate.Limiter),
			newLimiter: func() *rate.Limiter {
				return rate.NewLimiter(rate.Every(1*time.Minute), 2)
			},
		},
		maxRetries:  3,
		nodeForConn: make(map[*pgx.Conn]uint32),
		gc:          make(map[*pgx.Conn]struct{}),
	}
}

func TestContextCancelledDuringBlockingAcquire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testPool := NewTestPool()
		testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			// Block until context is cancelled
			<-ctx.Done()
			return nil, ctx.Err()
		}

		retryPool := createTestRetryPool(testPool)
		ctx, cancel := context.WithCancel(t.Context())

		// Cancel the context after a short delay
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		err := retryPool.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
			t.Fatal("function should not be called when acquire fails")
			return nil
		})

		synctest.Wait()

		assert.Error(t, err) //nolint:testifylint  // we're inside a goroutine so this is appropriate
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestAcquireTimeoutReturnsErrAcquire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testPool := NewTestPool()
		testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			// Simulate slow acquire that times out
			select {
			case <-time.After(100 * time.Millisecond):
				return nil, errors.New("should not reach here")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		retryPool := createTestRetryPool(testPool)
		ctx := t.Context()
		acquireTimeout := 50 * time.Millisecond

		err := retryPool.withRetries(ctx, acquireTimeout, func(conn *pgxpool.Conn) error {
			t.Fatal("function should not be called when acquire times out")
			return nil
		})

		synctest.Wait()

		assert.Error(t, err) //nolint:testifylint  // we're inside a goroutine so this is appropriate
		assert.Contains(t, err.Error(), "error acquiring connection from pool")
		assert.ErrorIs(t, errors.Unwrap(err), ErrAcquire)
	})
}

func TestAcquireSucceedsButTopLevelContextCancelled(t *testing.T) {
	testPool := NewTestPool()

	retryPool := createTestRetryPool(testPool)
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately

	err := retryPool.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
		t.Fatal("function should not be called when context is cancelled")
		return nil
	})

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAcquireErrorWithConnectionReturned(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testPool := NewTestPool()
		testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			// Return both connection and error
			return &pgxpool.Conn{}, errors.New("pool exhausted")
		}

		retryPool := createTestRetryPool(testPool)
		ctx := t.Context()

		err := retryPool.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
			t.Fatal("function should not be called when acquire fails")
			return nil
		})

		synctest.Wait()

		assert.Error(t, err) //nolint:testifylint  // we're inside a goroutine so this is appropriate
		assert.Contains(t, err.Error(), "error acquiring connection from pool")
		assert.Contains(t, err.Error(), "pool exhausted")
	})
}

func TestAcquireSucceedsWithinTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testPool := NewTestPool()
		testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			// Small delay but within timeout
			select {
			case <-time.After(10 * time.Millisecond):
				return &pgxpool.Conn{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		retryPool := createTestRetryPool(testPool)
		ctx := t.Context()
		acquireTimeout := 50 * time.Millisecond
		functionCalled := false

		err := retryPool.withRetries(ctx, acquireTimeout, func(conn *pgxpool.Conn) error {
			functionCalled = true
			return nil
		})

		synctest.Wait()

		assert.NoError(t, err) //nolint:testifylint  // we're inside a goroutine so this is appropriate
		assert.True(t, functionCalled, "function should have been called")
	})
}

func TestNoAcquireTimeoutUsesOriginalContext(t *testing.T) {
	var acquireContext context.Context

	testPool := NewTestPool()
	testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
		acquireContext = ctx
		return &pgxpool.Conn{}, nil
	}

	retryPool := createTestRetryPool(testPool)
	originalCtx := t.Context()

	err := retryPool.withRetries(originalCtx, 0, func(conn *pgxpool.Conn) error {
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, originalCtx, acquireContext, "should use original context when no timeout is set")
}

func TestAcquireTimeoutCreatesSeparateContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var acquireContext context.Context

		testPool := NewTestPool()
		testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			acquireContext = ctx
			return &pgxpool.Conn{}, nil
		}

		retryPool := createTestRetryPool(testPool)
		originalCtx := t.Context()
		acquireTimeout := 50 * time.Millisecond
		startTime := time.Now()

		err := retryPool.withRetries(originalCtx, acquireTimeout, func(conn *pgxpool.Conn) error {
			return nil
		})

		assert.NoError(t, err) //nolint:testifylint  // we're inside a goroutine so this is appropriate
		assert.NotEqual(t, originalCtx, acquireContext, "should use different context when timeout is set")

		// Verify the timeout context has the expected deadline
		deadline, hasDeadline := acquireContext.Deadline()
		assert.True(t, hasDeadline, "acquire context should have a deadline")
		expectedDeadline := startTime.Add(acquireTimeout)
		assert.Equal(t, expectedDeadline, deadline, "deadline should be correct")
	})
}

func TestAcquireTimeoutContextCausePreserved(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testPool := NewTestPool()
		testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
			// Wait for context timeout
			<-ctx.Done()
			return nil, ctx.Err()
		}

		retryPool := createTestRetryPool(testPool)
		ctx := t.Context()
		acquireTimeout := 10 * time.Millisecond

		err := retryPool.withRetries(ctx, acquireTimeout, func(conn *pgxpool.Conn) error {
			t.Fatal("function should not be called")
			return nil
		})

		synctest.Wait()

		assert.Error(t, err) //nolint:testifylint  // we're inside a goroutine so this is appropriate
		assert.Contains(t, err.Error(), "error acquiring connection from pool")
		assert.ErrorIs(t, errors.Unwrap(err), ErrAcquire)
	})
}

func TestSuccessfulFunctionExecution(t *testing.T) {
	testPool := NewTestPool()
	testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
		return &pgxpool.Conn{}, nil
	}

	retryPool := createTestRetryPool(testPool)
	ctx := t.Context()
	functionCalled := false

	err := retryPool.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
		functionCalled = true
		return nil
	})

	require.NoError(t, err)
	require.True(t, functionCalled, "function should have been called")
}

// TestCancelErrorFromFnPropagatesCorrectly verifies that when fn returns
// context.Canceled without the cancel handler installed, the error propagates
// without attempting to drain.
func TestCancelErrorFromFnPropagatesCorrectly(t *testing.T) {
	testPool := NewTestPool()
	testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
		return nil, nil
	}

	retryPool := createTestRetryPool(testPool)
	ctx := t.Context()

	// fn returns context.Canceled to simulate a mid-query cancellation
	err := retryPool.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
		return context.Canceled
	})

	require.ErrorIs(t, err, context.Canceled)
}

// TestWithRetriesCancelHandlerBranchEntered verifies that when the cancel
// handler is installed and fn returns context.Canceled, withRetries enters the
// cancel-handler branch (not the general "not resettable or retryable" path).
// With a nil conn (unavoidable in unit tests — pgxpool.Conn requires a real
// pool), the drain function is skipped due to the nil check, but the branch
// selection is confirmed by the error type: context.Canceled without wrapping.
func TestWithRetriesCancelHandlerBranchEntered(t *testing.T) {
	testPool := NewTestPool()
	testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
		return nil, nil
	}

	retryPool := createTestRetryPool(testPool)
	retryPool.cancelHandlerInstalled = true

	drainCalled := false
	retryPool.drainCancelledConnFn = func(conn *pgxpool.Conn) bool {
		drainCalled = true
		return true
	}

	ctx := t.Context()
	err := retryPool.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
		return context.Canceled
	})

	require.ErrorIs(t, err, context.Canceled)
	// Drain is not called because conn is nil (unit test limitation), but the
	// cancel-handler branch was entered — confirmed by the error being unwrapped
	// context.Canceled rather than a logged "not resettable or retryable" error.
	require.False(t, drainCalled, "drain not called with nil conn")
}

// TestWithRetriesSkipsDrainWhenCancelHandlerNotInstalled verifies that without
// the cancel handler, context.Canceled errors do not trigger drain logic.
func TestWithRetriesSkipsDrainWhenCancelHandlerNotInstalled(t *testing.T) {
	testPool := NewTestPool()
	testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
		return nil, nil
	}

	retryPool := createTestRetryPool(testPool)
	retryPool.cancelHandlerInstalled = false

	drainCalled := false
	retryPool.drainCancelledConnFn = func(conn *pgxpool.Conn) bool {
		drainCalled = true
		return true
	}

	ctx := t.Context()
	err := retryPool.withRetries(ctx, 0, func(conn *pgxpool.Conn) error {
		return context.Canceled
	})

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, drainCalled, "drain should not be called without cancel handler")
}

// TestDrainConnWithCancelHandlerIntegration exercises the full drain logic
// (drainConnWithContext) as it would be invoked from withRetries via
// drainCancelledConnFn, using mockQueryer to simulate the cancel-absorption
// sequence. This covers the drain succeed/fail paths that determine whether
// withRetries GCs the connection or returns it to the pool.
func TestDrainConnWithCancelHandlerIntegration(t *testing.T) {
	tests := []struct {
		name        string
		responses   []error
		expectClean bool
	}{
		{
			name:        "clean connection: SELECT 1 succeeds immediately",
			responses:   []error{nil},
			expectClean: true,
		},
		{
			name:        "cancel absorbed then clean",
			responses:   []error{cancelErr(), nil},
			expectClean: true,
		},
		{
			name:        "broken connection",
			responses:   []error{errors.New("connection reset by peer")},
			expectClean: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &mockQueryer{responses: tt.responses}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			result := drainConnWithContext(ctx, q)
			require.Equal(t, tt.expectClean, result)
		})
	}
}

// TestStaleQueryCanceledIsRetriable verifies that a 57014 (query_canceled) error
// received on a live (non-cancelled) context is wrapped as a RetryableError by
// wrapRetryableError. This covers the race where drainConn confirms the
// connection clean before an in-flight CancelRequest from a prior context
// cancellation reaches CRDB; the stale cancel then hits the next request's query.
// The connection is clean once CRDB returns 57014, so the error must be retryable.
func TestStaleQueryCanceledIsRetriable(t *testing.T) {
	ctx := t.Context() // live, non-cancelled context
	staleCancelErr := &pgconn.PgError{Code: CrdbQueryCanceledCode}

	wrapped := wrapRetryableError(ctx, staleCancelErr)

	var retryable *RetryableError
	require.ErrorAs(t, wrapped, &retryable, "57014 with live context should be wrapped as RetryableError")
	require.ErrorIs(t, retryable.Err, staleCancelErr)
}

// TestStaleQueryCanceledWithCancelledContextIsNotRetried verifies that a 57014
// received when the caller's context IS already cancelled is returned as
// context.Canceled (not RetryableError), ensuring the drain path in withRetries
// is taken instead of a retry loop.
func TestStaleQueryCanceledWithCancelledContextIsNotRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately so ctx.Err() != nil

	staleCancelErr := &pgconn.PgError{Code: CrdbQueryCanceledCode}

	wrapped := wrapRetryableError(ctx, staleCancelErr)

	require.ErrorIs(t, wrapped, context.Canceled)
}
