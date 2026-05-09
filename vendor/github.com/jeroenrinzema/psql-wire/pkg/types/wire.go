package types

// Version represents a connection version presented inside the connection header
type Version uint32

// The below constants can occur during the first message a client
// sends to the server. There are two categories: protocol version and
// request code. The protocol version is (major version number << 16)
// + minor version number. Request codes are (1234 << 16) + 5678 + N,
// where N started at 0 and is increased by 1 for every new request
// code added, which happens rarely during major or minor Postgres
// releases.
//
// See: https://www.postgresql.org/docs/current/protocol-message-formats.html
const (
	Version30         Version = 196608   // (3 << 16) + 0
	VersionCancel     Version = 80877102 // (1234 << 16) + 5678
	VersionSSLRequest Version = 80877103 // (1234 << 16) + 5679
	VersionGSSENC     Version = 80877104 // (1234 << 16) + 5680
)
