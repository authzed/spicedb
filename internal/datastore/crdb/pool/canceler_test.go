package pool

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
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
