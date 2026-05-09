package wire

// sslIdentifier represents the bytes identifying whether the given connection
// supports SSL.
type sslIdentifier []byte

var (
	sslSupported   sslIdentifier = []byte{'S'}
	sslUnsupported sslIdentifier = []byte{'N'}
)
