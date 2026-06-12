package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

type fakeSessionCanceler struct {
	delay time.Duration
	err   error
	calls atomic.Int32
}

func (f *fakeSessionCanceler) CancelSessionQueries(_ *pgconn.PgConn) error {
	f.calls.Add(1)
	time.Sleep(f.delay)
	return f.err
}

type fakeDeadlineConn struct {
	mu        sync.Mutex
	deadlines []time.Time
}

func (f *fakeDeadlineConn) SetDeadline(t time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deadlines = append(f.deadlines, t)
	return nil
}

func (f *fakeDeadlineConn) last() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deadlines) == 0 {
		return time.Time{}
	}
	return f.deadlines[len(f.deadlines)-1]
}

func TestCancelHandlerBlocksUntilCancelCompletesAndClearsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := &fakeSessionCanceler{delay: 50 * time.Millisecond}
		conn := &fakeDeadlineConn{}
		h := &CancelQueryContextWatcherHandler{conn: conn, canceler: fake, deadlineDelay: time.Second}

		h.HandleCancel(context.Background())
		start := time.Now()
		h.HandleUnwatchAfterCancel()

		require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond,
			"HandleUnwatchAfterCancel must block until the CANCEL statement completes")
		require.Equal(t, int32(1), fake.calls.Load())
		require.True(t, conn.last().IsZero(), "deadline must be cleared after a successful cancel")
	})
}

func TestCancelHandlerPoisonsConnectionWhenCancelFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := &fakeSessionCanceler{err: errors.New("cancel pool exhausted")}
		conn := &fakeDeadlineConn{}
		h := &CancelQueryContextWatcherHandler{conn: conn, canceler: fake, deadlineDelay: time.Second}

		h.HandleCancel(context.Background())
		h.HandleUnwatchAfterCancel()

		require.False(t, conn.last().IsZero(), "deadline must be poisoned (non-zero) when the cancel fails")
		require.False(t, conn.last().After(time.Now()), "poison deadline must already be due")
	})
}

func TestCancelHandlerResetsStateBetweenCycles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := &fakeSessionCanceler{}
		conn := &fakeDeadlineConn{}
		h := &CancelQueryContextWatcherHandler{conn: conn, canceler: fake, deadlineDelay: time.Second}

		// Cycle 1: failure poisons.
		fake.err = errors.New("boom")
		h.HandleCancel(context.Background())
		h.HandleUnwatchAfterCancel()
		require.False(t, conn.last().IsZero())

		// Cycle 2: success clears.
		fake.err = nil
		h.HandleCancel(context.Background())
		h.HandleUnwatchAfterCancel()
		require.True(t, conn.last().IsZero())
		require.Equal(t, int32(2), fake.calls.Load())
	})
}
