package logging

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// mockCloser is a test double that tracks Close() calls
type mockCloser struct {
	closeCount atomic.Int32
	closeErr   error
	closeDelay time.Duration // Optional delay to simulate slow close
}

func (m *mockCloser) Close() error {
	if m.closeDelay > 0 {
		time.Sleep(m.closeDelay)
	}
	m.closeCount.Add(1)
	return m.closeErr
}

func TestCloserHolder_Set(t *testing.T) {
	t.Run("sets closer", func(t *testing.T) {
		ch := &closerHolder{}
		closer := &mockCloser{}

		ch.Set(closer)

		require.NoError(t, ch.Close())
		require.Equal(t, int32(1), closer.closeCount.Load())
	})

	t.Run("closes previous closer when setting new one", func(t *testing.T) {
		ch := &closerHolder{}
		first := &mockCloser{}
		second := &mockCloser{}

		ch.Set(first)
		ch.Set(second)

		// First should have been closed when second was set
		require.Equal(t, int32(1), first.closeCount.Load())
		// Second should not be closed yet
		require.Equal(t, int32(0), second.closeCount.Load())

		// Now close the holder
		require.NoError(t, ch.Close())
		require.Equal(t, int32(1), second.closeCount.Load())
	})

	t.Run("does not double-close same closer", func(t *testing.T) {
		ch := &closerHolder{}
		closer := &mockCloser{}

		ch.Set(closer)
		ch.Set(closer) // Same closer again

		// Should not have been closed yet (same instance)
		require.Equal(t, int32(0), closer.closeCount.Load())

		require.NoError(t, ch.Close())
		require.Equal(t, int32(1), closer.closeCount.Load())
	})

	t.Run("closes new closer immediately if holder already closed", func(t *testing.T) {
		ch := &closerHolder{}
		first := &mockCloser{}
		second := &mockCloser{}

		ch.Set(first)
		require.NoError(t, ch.Close())

		// Setting after close should immediately close the new closer
		ch.Set(second)
		require.Equal(t, int32(1), second.closeCount.Load())
	})

	t.Run("handles nil closer", func(t *testing.T) {
		ch := &closerHolder{}

		ch.Set(nil)
		require.NoError(t, ch.Close())
	})
}

func TestCloserHolder_Close(t *testing.T) {
	t.Run("is idempotent", func(t *testing.T) {
		ch := &closerHolder{}
		closer := &mockCloser{}

		ch.Set(closer)

		require.NoError(t, ch.Close())
		require.NoError(t, ch.Close())
		require.NoError(t, ch.Close())

		// Should only be closed once
		require.Equal(t, int32(1), closer.closeCount.Load())
	})

	t.Run("returns error from closer", func(t *testing.T) {
		ch := &closerHolder{}
		expectedErr := errors.New("close failed")
		closer := &mockCloser{closeErr: expectedErr}

		ch.Set(closer)

		err := ch.Close()
		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("returns nil when no closer set", func(t *testing.T) {
		ch := &closerHolder{}
		require.NoError(t, ch.Close())
	})

	t.Run("times out if closer blocks too long", func(t *testing.T) {
		ch := &closerHolder{}
		// Create a closer that takes longer than our timeout
		closer := &mockCloser{closeDelay: 500 * time.Millisecond}

		ch.Set(closer)

		// Use a very short timeout to trigger the timeout path
		err := ch.CloseWithTimeout(10 * time.Millisecond)
		require.Error(t, err)
		require.Contains(t, err.Error(), "timeout")
	})

	t.Run("completes before timeout if closer is fast", func(t *testing.T) {
		ch := &closerHolder{}
		closer := &mockCloser{} // No delay

		ch.Set(closer)

		err := ch.CloseWithTimeout(1 * time.Second)
		require.NoError(t, err)
		require.Equal(t, int32(1), closer.closeCount.Load())
	})

	t.Run("zero timeout closes synchronously", func(t *testing.T) {
		ch := &closerHolder{}
		closer := &mockCloser{}

		ch.Set(closer)

		err := ch.CloseWithTimeout(0)
		require.NoError(t, err)
		require.Equal(t, int32(1), closer.closeCount.Load())
	})

	t.Run("negative timeout closes synchronously", func(t *testing.T) {
		ch := &closerHolder{}
		closer := &mockCloser{}

		ch.Set(closer)

		err := ch.CloseWithTimeout(-1 * time.Second)
		require.NoError(t, err)
		require.Equal(t, int32(1), closer.closeCount.Load())
	})
}

func TestCloserHolder_Concurrent(t *testing.T) {
	t.Run("concurrent Set calls are safe", func(t *testing.T) {
		ch := &closerHolder{}
		var wg sync.WaitGroup
		closers := make([]*mockCloser, 100)

		for i := range closers {
			closers[i] = &mockCloser{}
		}

		// Concurrently set different closers
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				ch.Set(closers[idx])
			}(i)
		}

		wg.Wait()
		require.NoError(t, ch.Close())

		// Count total closes - should be 100 (99 replaced + 1 final close)
		var totalCloses int32
		for _, c := range closers {
			totalCloses += c.closeCount.Load()
		}
		require.Equal(t, int32(100), totalCloses)
	})

	t.Run("concurrent Close calls are safe", func(t *testing.T) {
		ch := &closerHolder{}
		closer := &mockCloser{}
		ch.Set(closer)

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = ch.Close()
			}()
		}

		wg.Wait()

		// Should only be closed once despite concurrent calls
		require.Equal(t, int32(1), closer.closeCount.Load())
	})
}

func TestSetGlobalLoggerWithCloser(t *testing.T) {
	// Save original state
	originalLogger := Logger

	t.Cleanup(func() {
		SetGlobalLogger(originalLogger)
		// Reset global closer using thread-safe method
		globalCloser.reset()
	})

	t.Run("sets logger and closer", func(t *testing.T) {
		logger := zerolog.Nop()
		closer := &mockCloser{}

		SetGlobalLoggerWithCloser(logger, closer)

		require.NoError(t, Close())
		require.Equal(t, int32(1), closer.closeCount.Load())
	})
}

func TestClose(t *testing.T) {
	// Save original state
	originalLogger := Logger

	t.Cleanup(func() {
		SetGlobalLogger(originalLogger)
		// Reset global closer using thread-safe method
		globalCloser.reset()
	})

	t.Run("closes global closer", func(t *testing.T) {
		closer := &mockCloser{}
		SetGlobalLoggerWithCloser(zerolog.Nop(), closer)

		require.NoError(t, Close())
		require.Equal(t, int32(1), closer.closeCount.Load())
	})

	t.Run("is safe to call without closer", func(t *testing.T) {
		// Reset to ensure no closer using thread-safe method
		globalCloser.reset()
		require.NoError(t, Close())
	})
}

func TestResetCloserForTesting(t *testing.T) {
	// Save original state
	originalLogger := Logger

	t.Cleanup(func() {
		SetGlobalLogger(originalLogger)
		globalCloser.reset()
	})

	t.Run("resets closed flag allowing new closers to be set", func(t *testing.T) {
		// Start fresh using thread-safe method
		globalCloser.reset()

		// Set and close a closer
		first := &mockCloser{}
		SetGlobalLoggerWithCloser(zerolog.Nop(), first)
		require.NoError(t, Close())
		require.Equal(t, int32(1), first.closeCount.Load())

		// Without reset, new closers would be immediately closed
		// (because closed=true permanently after Close())
		ResetCloserForTesting()

		// After reset, should accept new closers normally
		second := &mockCloser{}
		SetGlobalLoggerWithCloser(zerolog.Nop(), second)

		// Second closer should NOT be closed yet (reset cleared closed flag)
		require.Equal(t, int32(0), second.closeCount.Load())

		// Now close should work on the new closer
		require.NoError(t, Close())
		require.Equal(t, int32(1), second.closeCount.Load())
	})

	t.Run("discards pending closer without closing it", func(t *testing.T) {
		// Start fresh to ensure isolation from previous test
		globalCloser.reset()

		closer := &mockCloser{}
		SetGlobalLoggerWithCloser(zerolog.Nop(), closer)

		// Reset discards the closer without calling Close()
		ResetCloserForTesting()

		// The discarded closer was never closed (this is the dangerous behavior
		// that makes this function unsafe for production use)
		require.Equal(t, int32(0), closer.closeCount.Load())

		// Close() now does nothing since state was reset
		require.NoError(t, Close())
		require.Equal(t, int32(0), closer.closeCount.Load())
	})

	t.Run("concurrent reset and close are safe", func(t *testing.T) {
		// Test only the closerHolder operations, not the Logger global
		// (Logger global has a separate pre-existing race condition)
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(3)
			go func() {
				defer wg.Done()
				globalCloser.reset()
			}()
			go func() {
				defer wg.Done()
				_ = globalCloser.Close()
			}()
			go func() {
				defer wg.Done()
				globalCloser.Set(&mockCloser{})
			}()
		}
		wg.Wait()
		// Test passes if no race detected (run with -race flag)
	})
}
