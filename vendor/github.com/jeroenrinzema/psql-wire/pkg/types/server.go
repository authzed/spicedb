package types

// ServerStatus indicates the current server status. Possible values are 'I' if
// idle (not in a transaction block); 'T' if in a transaction block; or 'E' if
// in a failed transaction block (queries will be rejected until block is ended).
type ServerStatus byte

// Possible values are 'I' if idle (not in a transaction block); 'T' if in a
// transaction block; or 'E' if in a failed transaction block
// (queries will be rejected until block is ended).
const (
	ServerIdle              = 'I'
	ServerTransactionBlock  = 'T'
	ServerTransactionFailed = 'E'
)
