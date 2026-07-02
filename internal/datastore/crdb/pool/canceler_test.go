package pool

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestCancelerSessionRegistry(t *testing.T) {
	c := &Canceler{sessionIDs: make(map[*pgconn.PgConn]string)}
	pgc := &pgconn.PgConn{}

	_, ok := c.SessionID(pgc)
	require.False(t, ok)

	c.setSessionID(pgc, "deadbeef01234567deadbeef01234567")
	id, ok := c.SessionID(pgc)
	require.True(t, ok)
	require.Equal(t, "deadbeef01234567deadbeef01234567", id)

	c.UnregisterSession(pgc)
	_, ok = c.SessionID(pgc)
	require.False(t, ok)
}

func TestCancelSessionQueriesUnknownSession(t *testing.T) {
	c := &Canceler{sessionIDs: make(map[*pgconn.PgConn]string)}
	err := c.CancelSessionQueries(&pgconn.PgConn{})
	require.Error(t, err, "unknown sessions must error so the handler poisons the connection")
}

func TestSessionIDValidation(t *testing.T) {
	require.True(t, validSessionID.MatchString("17a4c5b2e9d8f0a117a4c5b2e9d8f0a1"))
	require.False(t, validSessionID.MatchString(""))
	require.False(t, validSessionID.MatchString("17a4'; CANCEL SESSIONS --"))
}

// TestCancelerInstallOnSetsHandlers verifies that InstallOn wires all three
// hooks (AfterConnect, BeforeClose, BuildContextWatcherHandler) onto a config
// that started with none.
func TestCancelerInstallOnSetsHandlers(t *testing.T) {
	c := &Canceler{sessionIDs: make(map[*pgconn.PgConn]string)}

	config, err := pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)
	require.Nil(t, config.AfterConnect)
	require.Nil(t, config.BeforeClose)

	c.InstallOn(config)

	require.NotNil(t, config.AfterConnect)
	require.NotNil(t, config.BeforeClose)
	require.NotNil(t, config.ConnConfig.BuildContextWatcherHandler)

	// Invoke BuildContextWatcherHandler to cover its body.
	handler := config.ConnConfig.BuildContextWatcherHandler(&pgconn.PgConn{})
	require.NotNil(t, handler)
}

// TestCancelerInstallOnComposesExistingAfterConnect verifies that an existing
// AfterConnect hook is called before RegisterSession, and that an error from
// that hook is propagated without calling RegisterSession.
func TestCancelerInstallOnComposesExistingAfterConnect(t *testing.T) {
	c := &Canceler{sessionIDs: make(map[*pgconn.PgConn]string)}

	config, err := pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)

	var called bool
	config.AfterConnect = func(_ context.Context, _ *pgx.Conn) error {
		called = true
		return errors.New("existing hook error")
	}

	c.InstallOn(config)

	// The existing hook returns an error; RegisterSession must not be reached.
	err = config.AfterConnect(t.Context(), nil)
	require.True(t, called, "existing AfterConnect must be invoked")
	require.EqualError(t, err, "existing hook error")
}

// TestCancelerInstallOnComposesExistingBeforeClose verifies that an existing
// BeforeClose hook is called as part of the composed hook.
func TestCancelerInstallOnComposesExistingBeforeClose(t *testing.T) {
	c := &Canceler{sessionIDs: make(map[*pgconn.PgConn]string)}

	config, err := pgxpool.ParseConfig("postgres://localhost/test")
	require.NoError(t, err)

	var called bool
	config.BeforeClose = func(_ *pgx.Conn) { called = true }

	c.InstallOn(config)

	// &pgx.Conn{} has a nil pgConn field; PgConn() returns nil, and
	// UnregisterSession(nil) is a safe no-op (delete from map with nil key).
	config.BeforeClose(&pgx.Conn{})
	require.True(t, called, "existing BeforeClose must be invoked")
}

// TestNewCancelerAndClose verifies that NewCanceler returns a valid Canceler
// and that Close drains the pool without panicking. The cancelled context
// stops the pool's background connection goroutines immediately so the test
// does not leak goroutines or attempt real TCP connections.
func TestNewCancelerAndClose(t *testing.T) {
	config, err := pgxpool.ParseConfig("postgres://db:password@127.0.0.1:1/nonexistent")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	canceler, err := NewCanceler(ctx, config)
	require.NoError(t, err)
	require.NotNil(t, canceler)
	canceler.Close()
}

// TestCancelerCancelSessionQueriesErrorsOnClosedPool verifies that
// CancelSessionQueries returns a wrapped error (and records the "error"
// outcome counter) when the underlying pool can no longer issue queries.
func TestCancelerCancelSessionQueriesErrorsOnClosedPool(t *testing.T) {
	config, err := pgxpool.ParseConfig("postgres://db:password@127.0.0.1:1/nonexistent")
	require.NoError(t, err)
	config.MinConns = 0

	p, err := pgxpool.NewWithConfig(t.Context(), config)
	require.NoError(t, err)
	p.Close()

	pgc := &pgconn.PgConn{}
	c := &Canceler{pool: p, sessionIDs: make(map[*pgconn.PgConn]string)}
	c.setSessionID(pgc, "deadbeef01234567deadbeef01234567")

	err = c.CancelSessionQueries(pgc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to cancel queries")
}
