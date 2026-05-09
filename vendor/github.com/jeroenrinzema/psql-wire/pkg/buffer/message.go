package buffer

import "math"

//go:generate stringer -type=ServerErrFieldType

// ServerErrFieldType represents the error fields.
type ServerErrFieldType byte

// http://www.postgresql.org/docs/current/static/protocol-error-fields.html
const (
	ServerErrFieldSeverity       ServerErrFieldType = 'S'
	ServerErrFieldSQLState       ServerErrFieldType = 'C'
	ServerErrFieldMsgPrimary     ServerErrFieldType = 'M'
	ServerErrFieldDetail         ServerErrFieldType = 'D'
	ServerErrFieldHint           ServerErrFieldType = 'H'
	ServerErrFieldSrcFile        ServerErrFieldType = 'F'
	ServerErrFieldSrcLine        ServerErrFieldType = 'L'
	ServerErrFieldSrcFunction    ServerErrFieldType = 'R'
	ServerErrFieldConstraintName ServerErrFieldType = 'n'
)

//go:generate stringer -type=PrepareType

// PrepareType represents a subtype for prepare messages.
type PrepareType byte

const (
	// PrepareStatement represents a prepared statement.
	PrepareStatement PrepareType = 'S'
	// PreparePortal represents a portal.
	PreparePortal PrepareType = 'P'
)

// MaxPreparedStatementArgs is the maximum number of arguments a prepared
// statement can have when prepared via the Postgres wire protocol. This is not
// documented by Postgres, but is a consequence of the fact that a 16-bit
// integer in the wire format is used to indicate the number of values to bind
// during prepared statement execution.
const MaxPreparedStatementArgs = math.MaxUint16
