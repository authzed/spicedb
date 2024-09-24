package pool

import (
	"github.com/jackc/pgx/v5"
)

// The Postgres wire protocol sends a `BackendKey` when it establishes a new
// connection with a client. The `BackendKey` includes two fields, `ProcessID`
// and `SecretKey`.
//
// In postgres, the ProcessID is the PID of the process that is
// handling the current session, and `SecretKey`, which is unique per session
// and is for cancelling a running query (out of band, on a new connection).
//
// In the wire protocol, the BackendKey is a single 64 bit field.
//
// In CockroachDB, a "SqlInstanceID" and random data get encoded into the
// BackendKey in one of two ways:
//
//   - A 12 bit node ID and 52 bits of random data. This is used when talking to
//     standalone (non-multitenant) CockroachDB nodes.
//   - A 32 bit instance ID and 32 bits of random data. This is used when talking
//     to multi-tenant CockroachDB nodes - it is stable for a single sql instance
//     but may not correspond to the physical node ID.
//
// The first bit is a sentinel that indicates which type of encoding is used.
//
// In both cases, random data fills the SecretKey portion of the BackendKey and
// for compatibility with postgres, can also be used to cancel running queries
// (CockroachDB also has a separate first-class CANCEL command).
//
// This diagram shows how Cockroach and Postgres encode/decode the bits of the
// BackendKey field:
//
//	0                   1                   2                   3                   4                   5                   6
//	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|                    PG BackendKey.ProcessID                    |                    PG BackendKey.SecretKey                    |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|0|  Short ID (Node ID) |                                              Random Data                                              |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//	|1|                           Long ID                           |                           Random Data                         |
//	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
// For tracking connections to CRDB nodes this means that the SqlInstanceID can be used to track which nodes have
// connections.
//
// Refs:
// - get_backend_pid(): https://www.postgresql.org/docs/15/functions-info.html
// - BackendKey and query cancellation: https://www.postgresql.org/docs/current/protocol-flow.html
// - BackendKey wire definition: https://github.com/jackc/pgproto3/blob/master/backend_key_data.go
// - CRDB implementation: https://github.com/cockroachdb/cockroach/blob/fd33b0a3f5daeb6045e4c9ec925db8ef8ca38ca8/pkg/sql/pgwire/pgwirecancel/backend_key_data.go#L21

// nodeID returns the sqlInstanceID for a pgxpool.Conn
func nodeID(conn *pgx.Conn) uint32 {
	return sqlInstanceID(conn.PgConn().PID())
}

// sqlInstanceID returns the instance ID encoded into the PID field
func sqlInstanceID(pid uint32) uint32 {
	// If leading bit is 0, we have a "short" sqlInstanceID
	// which should be the nodeID for standalone Cockroach nodes.
	// The first 12 bits are the id, the rest is random
	if pid&(1<<31) == 0 {
		return pid >> 20
	}

	// If the leading bit is 1, we have a "long" sqlInstanceID
	// These are stable for all connections to the same SQL node but
	// may not correspond to the physical node ID.
	// This should only happen when talking to a tenanted CRDB cluster.

	// clear out the sentinel bit - the rest is the instance ID
	return pid &^ (1 << 31)
}
