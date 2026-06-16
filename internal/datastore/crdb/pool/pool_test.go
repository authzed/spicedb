package pool

import (
	"context"
	"errors"
	"fmt"
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
		maxRetries:     3,
		nodeForConn:    make(map[*pgx.Conn]uint32),
		gc:             make(map[*pgx.Conn]struct{}),
		nodeIDFromConn: func(conn *pgx.Conn) uint32 { return 0 },
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

// newConfigForCallbackTest builds a RetryPool whose lifecycle callbacks have
// been installed onto a fresh config, with node-id extraction stubbed to a
// fixed value so the callbacks can run without a live connection.
func newConfigForCallbackTest(t *testing.T, nodeID uint32) (*RetryPool, *pgxpool.Config) {
	t.Helper()
	p := createTestRetryPool(NewTestPool())
	p.nodeIDFromConn = func(conn *pgx.Conn) uint32 { return nodeID }

	config := &pgxpool.Config{ConnConfig: &pgx.ConnConfig{}}
	// a generous rate so the limiter never blocks the test
	p.configureLifecycleCallbacks(config, time.Microsecond)
	return p, config
}

func TestAfterConnectRecordsNodeAndMarksHealthy(t *testing.T) {
	const node = uint32(42)
	p, config := newConfigForCallbackTest(t, node)
	require.NotNil(t, config.AfterConnect)

	// conn may be nil here because node-id extraction is stubbed out.
	err := config.AfterConnect(t.Context(), nil)
	require.NoError(t, err)

	p.RLock()
	got, ok := p.nodeForConn[nil]
	p.RUnlock()
	require.True(t, ok, "connection should be tracked after AfterConnect")
	require.Equal(t, node, got)
	require.True(t, p.healthTracker.IsHealthy(node), "node should be marked healthy")
}

func TestAfterConnectInvokesWrappedHook(t *testing.T) {
	p := createTestRetryPool(NewTestPool())
	p.nodeIDFromConn = func(conn *pgx.Conn) uint32 { return 7 }

	wrappedCalled := false
	config := &pgxpool.Config{
		ConnConfig: &pgx.ConnConfig{},
		AfterConnect: func(ctx context.Context, conn *pgx.Conn) error {
			wrappedCalled = true
			return nil
		},
	}
	p.configureLifecycleCallbacks(config, time.Microsecond)

	require.NoError(t, config.AfterConnect(t.Context(), nil))
	require.True(t, wrappedCalled, "pre-existing AfterConnect hook should be invoked")

	p.RLock()
	_, ok := p.nodeForConn[nil]
	p.RUnlock()
	require.True(t, ok, "connection should still be tracked")
}

func TestAfterConnectWrappedHookErrorShortCircuits(t *testing.T) {
	p := createTestRetryPool(NewTestPool())
	p.nodeIDFromConn = func(conn *pgx.Conn) uint32 { return 7 }

	wrappedErr := errors.New("wrapped failed")
	config := &pgxpool.Config{
		ConnConfig: &pgx.ConnConfig{},
		AfterConnect: func(ctx context.Context, conn *pgx.Conn) error {
			return wrappedErr
		},
	}
	p.configureLifecycleCallbacks(config, time.Microsecond)

	err := config.AfterConnect(t.Context(), nil)
	require.ErrorIs(t, err, wrappedErr)

	p.RLock()
	_, ok := p.nodeForConn[nil]
	p.RUnlock()
	require.False(t, ok, "connection should not be tracked when wrapped hook fails")
}

// --- sqlInstanceID: pure bit decoding ---

func TestSQLInstanceID(t *testing.T) {
	cases := []struct {
		name string
		pid  uint32
		want uint32
	}{
		{"zero", 0, 0},
		{"short id, leading bit clear", (5 << 20) | 0xABCDE, 5},
		{"short id max (bit 31 must stay clear)", (0x7FF << 20) | 0x1, 0x7FF},
		{"long id, leading bit set", (1 << 31) | 1234, 1234},
		{"long id keeps lower 31 bits", (1 << 31) | 0x7FFFFFFF, 0x7FFFFFFF},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, sqlInstanceID(tc.pid))
		})
	}
}

// --- error classification: pure functions ---

func pgErr(code string) error {
	return &pgconn.PgError{Code: code}
}

func TestIsRetryableError(t *testing.T) {
	ctx := t.Context()
	require.True(t, IsRetryableError(ctx, pgErr(CrdbRetryErrCode)))
	require.True(t, IsRetryableError(ctx,
		fmt.Errorf("%w", &pgconn.PgError{Code: CrdbUnknownSQLState, Message: CrdbClockSkewMessage})))
	require.False(t, IsRetryableError(ctx, pgErr(CrdbUnknownSQLState)),
		"unknown sqlstate without clock-skew message is not retryable")
	require.False(t, IsRetryableError(ctx, pgErr(CrdbAmbiguousErrorCode)))
	require.False(t, IsRetryableError(ctx, errors.New("not a pg error")))
}

func TestIsResettableError(t *testing.T) {
	ctx := t.Context()
	for _, msg := range []string{
		"broken pipe", "unexpected EOF", "conn closed",
		"connection refused", "connection reset by peer",
	} {
		require.True(t, IsResettableError(ctx, errors.New("wrapped: "+msg)), msg)
	}
	require.True(t, IsResettableError(ctx, pgErr(CrdbAmbiguousErrorCode)))
	require.True(t, IsResettableError(ctx, pgErr(CrdbServerNotAcceptingClients)))
	require.False(t, IsResettableError(ctx, pgErr(CrdbRetryErrCode)))
	require.False(t, IsResettableError(ctx, errors.New("some other error")))
}

func TestWrapRetryableError(t *testing.T) {
	ctx := t.Context()

	require.NoError(t, wrapRetryableError(ctx, nil))

	var resettable *ResettableError
	require.ErrorAs(t, wrapRetryableError(ctx, pgErr(CrdbAmbiguousErrorCode)), &resettable)

	var retryable *RetryableError
	require.ErrorAs(t, wrapRetryableError(ctx, pgErr(CrdbRetryErrCode)), &retryable)

	plain := errors.New("not classified")
	require.Equal(t, plain, wrapRetryableError(ctx, plain))

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, wrapRetryableError(cancelledCtx, errors.New("x")), context.Canceled,
		"a cancelled context short-circuits to ctx.Err()")
}

func TestWrapRetryableErrorReturnsContextError(t *testing.T) {
	require.ErrorIs(t, wrapRetryableError(t.Context(),
		fmt.Errorf("wrapped: %w", context.DeadlineExceeded)), context.DeadlineExceeded)
	require.ErrorIs(t, wrapRetryableError(t.Context(),
		fmt.Errorf("wrapped: %w", context.Canceled)), context.Canceled)
}

// --- delegating getters via the injected pgxPool ---

func TestGettersDelegateToUnderlyingPool(t *testing.T) {
	testPool := NewTestPool()
	testPool.configFunc = func() *pgxpool.Config {
		return &pgxpool.Config{MaxConns: 10, MinConns: 3}
	}
	closed := false
	testPool.closeFunc = func() { closed = true }
	idle := []*pgxpool.Conn{{}}
	testPool.acquireAllIdleFunc = func(ctx context.Context) []*pgxpool.Conn { return idle }

	p := createTestRetryPool(testPool)

	require.Equal(t, "test-pool", p.ID())
	require.Equal(t, uint32(10), p.MaxConns())
	require.Equal(t, uint32(3), p.MinConns())
	require.NotNil(t, p.Config())
	require.NotNil(t, p.Stat())
	require.Equal(t, idle, p.AcquireAllIdle(t.Context()))

	p.Close()
	require.True(t, closed, "Close should delegate to the underlying pool")
}

// --- Node / Range / GC: map + mutex bookkeeping ---

func TestNodeReturnsZeroForUnknownConn(t *testing.T) {
	p := createTestRetryPool(NewTestPool())
	require.Equal(t, uint32(0), p.Node(&pgx.Conn{}))
}

func TestRangeAndGC(t *testing.T) {
	p := createTestRetryPool(NewTestPool())
	a, b := &pgx.Conn{}, &pgx.Conn{}

	p.Lock()
	p.nodeForConn[a] = 1
	p.nodeForConn[b] = 2
	p.Unlock()

	require.Equal(t, uint32(1), p.Node(a))

	seen := map[*pgx.Conn]uint32{}
	p.Range(func(conn *pgx.Conn, nodeID uint32) { seen[conn] = nodeID })
	require.Equal(t, map[*pgx.Conn]uint32{a: 1, b: 2}, seen)

	p.GC(a)
	require.Equal(t, uint32(0), p.Node(a), "GC'd conn is removed from nodeForConn")
	p.RLock()
	_, marked := p.gc[a]
	p.RUnlock()
	require.True(t, marked, "GC'd conn is recorded in the gc set")
}

// --- BeforeAcquire / AfterRelease / BeforeClose lifecycle callbacks ---

func TestBeforeAcquireDestroysGCdConnection(t *testing.T) {
	p, config := newConfigForCallbackTest(t, 0)
	conn := &pgx.Conn{}

	require.True(t, config.BeforeAcquire(t.Context(), conn), //nolint:staticcheck // need to move BeforeAcquire to PrepareConn
		"a connection not marked for GC should be kept")

	p.GC(conn)
	require.False(t, config.BeforeAcquire(t.Context(), conn), //nolint:staticcheck // need to move BeforeAcquire to PrepareConn
		"a connection marked for GC should be destroyed")

	p.RLock()
	_, stillMarked := p.gc[conn]
	p.RUnlock()
	require.False(t, stillMarked, "BeforeAcquire clears the gc entry once handled")
}

func TestAfterReleaseDestroysGCdConnection(t *testing.T) {
	p, config := newConfigForCallbackTest(t, 0)
	conn := &pgx.Conn{}

	require.True(t, config.AfterRelease(conn))

	p.GC(conn)
	require.False(t, config.AfterRelease(conn))
}

func TestBeforeCloseRemovesBookkeeping(t *testing.T) {
	p, config := newConfigForCallbackTest(t, 0)
	conn := &pgx.Conn{}

	p.Lock()
	p.nodeForConn[conn] = 9
	p.gc[conn] = struct{}{}
	p.Unlock()

	config.BeforeClose(conn)

	require.Equal(t, uint32(0), p.Node(conn))
	p.RLock()
	_, marked := p.gc[conn]
	p.RUnlock()
	require.False(t, marked, "BeforeClose removes the conn from both maps")
}

func TestBeforeAcquireInvokesWrappedHook(t *testing.T) {
	p := createTestRetryPool(NewTestPool())
	wrappedCalled := false
	config := &pgxpool.Config{ConnConfig: &pgx.ConnConfig{}}
	config.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool { //nolint:staticcheck // need to move BeforeAcquire to PrepareConn
		wrappedCalled = true
		return false // a false from the wrapped hook should short-circuit
	}
	p.configureLifecycleCallbacks(config, time.Microsecond)

	require.False(t, config.BeforeAcquire(t.Context(), &pgx.Conn{})) //nolint:staticcheck // need to move BeforeAcquire to PrepareConn
	require.True(t, wrappedCalled)
}

// --- acquireFromDifferentNode ---

func TestAcquireFromDifferentNodeReturnsAcquireError(t *testing.T) {
	testPool := NewTestPool()
	wantErr := errors.New("acquire failed")
	testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
		return nil, wantErr
	}
	p := createTestRetryPool(testPool)

	conn, err := p.acquireFromDifferentNode(t.Context(), 1)
	require.Nil(t, conn)
	require.ErrorIs(t, err, wantErr)
}

func TestAcquireFromDifferentNodeWithOneHealthyNode(t *testing.T) {
	testPool := NewTestPool()
	testPool.acquireFunc = func(ctx context.Context) (*pgxpool.Conn, error) {
		return &pgxpool.Conn{}, nil
	}
	p := createTestRetryPool(testPool)
	p.healthTracker.SetNodeHealth(1, true) // only one healthy node

	conn, err := p.acquireFromDifferentNode(t.Context(), 7)
	require.NoError(t, err)
	require.NotNil(t, conn, "with <=1 healthy node it returns whatever it acquired")
}

// TODO uncomment when fixing https://github.com/authzed/spicedb/issues/3179.
//
// AfterConnect takes the pool's exclusive write lock (p.Lock) and then calls the
// blocking limiter.Wait while still holding it. The same RWMutex guards the
// connection acquire/release hot path (BeforeAcquire/AfterRelease -> gcConnection),
// so a reconnect that is rate-limited stalls all datastore traffic in the pool.
//
// This test fails against the current code (the hot path stalls) and is expected
// to pass once limiter.Wait is hoisted out of the critical section.
// func TestAfterConnectDoesNotBlockAcquireHotPath(t *testing.T) {
//	p := createTestRetryPool(NewTestPool())
//	p.nodeIDFromConn = func(conn *pgx.Conn) uint32 { return 0 }
//
//	config := &pgxpool.Config{ConnConfig: &pgx.ConnConfig{}}
//	// rate.NewLimiter(rate.Every(1h), 1) starts with a single token. The first
//	// AfterConnect consumes it; the next one must wait ~1h for a refill.
//	p.configureLifecycleCallbacks(config, time.Hour)
//
//	ctx := t.Context()
//
//	// Consume the limiter's only initial token so the next reconnect blocks.
//	require.NoError(t, config.AfterConnect(ctx, &pgx.Conn{}))
//
//	// A second reconnect: it grabs p.Lock() and then parks in limiter.Wait,
//	// holding the write lock the entire time.
//	blockedCtx, cancelBlocked := context.WithCancel(ctx)
//	defer cancelBlocked() // release the stuck goroutine when the test ends
//	go func() {
//		_ = config.AfterConnect(blockedCtx, &pgx.Conn{})
//	}()
//
//	// Wait until that goroutine is actually holding the write lock. TryRLock can
//	// only fail because our reconnect goroutine holds (or is acquiring) the write
//	// lock — nothing else in this test writes.
//	require.Eventually(t, func() bool {
//		if p.TryRLock() {
//			p.RUnlock()
//			return false
//		}
//		return true
//	}, 2*time.Second, time.Millisecond, "reconnect goroutine never took the write lock")
//
//	// The acquire hot path must stay responsive. BeforeAcquire -> gcConnection
//	// takes p.RLock(), which is blocked behind the held write lock.
//	done := make(chan struct{})
//	go func() {
//		config.BeforeAcquire(ctx, &pgx.Conn{}) //nolint:staticcheck // need to move BeforeAcquire to PrepareConn
//		close(done)
//	}()
//
//	select {
//	case <-done:
//		// hot path completed promptly -> the lock is not held across the wait
//	case <-time.After(5 * time.Second):
//		t.Fatal("acquire hot path (BeforeAcquire) stalled because AfterConnect held " +
//			"the write lock across the blocking limiter.Wait — see issue #3179")
//	}
//}
