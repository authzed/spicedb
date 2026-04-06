package pool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// mockQueryer simulates a DB connection's Exec behavior for drain testing.
type mockQueryer struct {
	responses []error
	idx       int
}

func (m *mockQueryer) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	if m.idx >= len(m.responses) {
		// Panic on unexpected extra calls: drainConn should stop after the
		// first nil (clean) response and not loop again.
		panic("mockQueryer: Exec called more times than expected")
	}
	err := m.responses[m.idx]
	m.idx++
	return pgconn.CommandTag{}, err
}

func cancelErr() error {
	return &pgconn.PgError{Code: CrdbQueryCanceledCode}
}

// TestDrainConn_CleanImmediately: cancel was already consumed by original query;
// first SELECT 1 succeeds → connection is clean.
func TestDrainConn_CleanImmediately(t *testing.T) {
	q := &mockQueryer{responses: []error{nil}}
	result := drainConn(q)
	require.True(t, result, "expected clean connection")
	require.Equal(t, 1, q.idx, "expected exactly one Exec call")
}

// TestDrainConn_CancelArrivesDuringDrain: cancel arrives during first SELECT 1
// (returns 57014), then second SELECT 1 succeeds.
func TestDrainConn_CancelArrivesDuringDrain(t *testing.T) {
	q := &mockQueryer{responses: []error{cancelErr(), nil}}
	result := drainConn(q)
	require.True(t, result, "expected clean connection after absorbing cancel")
	require.Equal(t, 2, q.idx, "expected two Exec calls")
}

// TestDrainConn_MultipleCancels: multiple 57014s then success.
func TestDrainConn_MultipleCancels(t *testing.T) {
	q := &mockQueryer{responses: []error{cancelErr(), cancelErr(), cancelErr(), nil}}
	result := drainConn(q)
	require.True(t, result, "expected clean connection after absorbing multiple cancels")
	require.Equal(t, 4, q.idx, "expected four Exec calls")
}

// TestDrainConn_BrokenConnection: non-57014 error (e.g. connection closed) →
// connection should be GC'd.
func TestDrainConn_BrokenConnection(t *testing.T) {
	q := &mockQueryer{responses: []error{errors.New("connection reset by peer")}}
	result := drainConn(q)
	require.False(t, result, "expected broken connection to return false")
	require.Equal(t, 1, q.idx, "expected exactly one Exec call")
}

// TestDrainConn_TimeoutGCsConnection: if the context times out during drain,
// the connection is treated as broken.
func TestDrainConn_TimeoutGCsConnection(t *testing.T) {
	calls := 0
	blocking := &blockingQueryer{
		fn: func(ctx context.Context) error {
			calls++
			<-ctx.Done()
			return ctx.Err()
		},
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	result := drainConnWithContext(ctx, blocking)
	require.False(t, result, "expected timed-out connection to return false")
	require.Equal(t, 1, calls, "expected one Exec attempt before timeout")
}

type blockingQueryer struct {
	fn func(ctx context.Context) error
}

func (b *blockingQueryer) Exec(ctx context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, b.fn(ctx)
}

// TestWithCancelHandler_SetsContextWatcherHandler verifies that WithCancelHandler
// installs a non-nil BuildContextWatcherHandler on the config.
func TestWithCancelHandler_SetsContextWatcherHandler(t *testing.T) {
	config := &pgxpool.Config{
		ConnConfig: &pgx.ConnConfig{},
	}
	opt := WithCancelHandler()
	p := &RetryPool{}
	opt(config, p)
	require.NotNil(t, config.ConnConfig.BuildContextWatcherHandler,
		"expected BuildContextWatcherHandler to be set by WithCancelHandler")
}

// TestWithCancelHandler_ReadyFlagSetByAfterConnect verifies the data-race guard:
// the cancelHandler starts with ready == 0 (so HandleCancel will not call
// CancelRequest while connectOne is still writing pid/secretKey), and
// AfterConnect sets ready == 1 once the connection is fully established.
//
// BuildContextWatcherHandler and AfterConnect use *pgconn.PgConn only as a map
// key before AfterConnect fires, so a zero-value allocation suffices here.
func TestWithCancelHandler_ReadyFlagSetByAfterConnect(t *testing.T) {
	config := &pgxpool.Config{ConnConfig: &pgx.ConnConfig{}}
	opt := WithCancelHandler()
	p := &RetryPool{}
	opt(config, p)

	// Use a zero-value PgConn as the map key — no methods are called on it
	// until after AfterConnect, which we don't reach in this test.
	fakeConn := new(pgconn.PgConn)

	// Simulate pgx calling BuildContextWatcherHandler during connectOne.
	handler := config.ConnConfig.BuildContextWatcherHandler(fakeConn)
	ch, ok := handler.(*cancelHandler)
	require.True(t, ok, "expected handler to be *cancelHandler")
	require.Equal(t, int32(0), ch.ready.Load(),
		"ready must be 0 before AfterConnect: CancelRequest must not be called during connectOne")

	// Simulate pgx calling AfterConnect once connectOne has written pid/secretKey.
	err := config.ConnConfig.AfterConnect(t.Context(), fakeConn)
	require.NoError(t, err)
	require.Equal(t, int32(1), ch.ready.Load(),
		"AfterConnect must set ready to 1 so HandleCancel may call CancelRequest")
}
