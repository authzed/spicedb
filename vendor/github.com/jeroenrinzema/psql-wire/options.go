package wire

import (
	"context"
	"crypto/tls"
	"log/slog"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
)

// ParseFn parses the given query and returns a prepared statement which could
// be used to execute at a later point in time.
type ParseFn func(ctx context.Context, query string) (PreparedStatements, error)

// PreparedStatementFn represents a query of which a statement has been
// prepared. The statement could be executed at any point in time with the given
// arguments and data writer.
type PreparedStatementFn func(ctx context.Context, writer DataWriter, parameters []Parameter) error

// Prepared is a small wrapper function returning a list of prepared statements.
// More then one prepared statement could be returned within the simple query
// protocol. An error is returned when more than one prepared statement is
// returned in the [extended query protocol].
//
// [extended query protocol]: https://www.postgresql.org/docs/15/protocol-flow.html#PROTOCOL-FLOW-MULTI-STATEMENT
func Prepared(stmts ...*PreparedStatement) PreparedStatements {
	return stmts
}

// NewStatement constructs a new prepared statement for the given function.
func NewStatement(fn PreparedStatementFn, options ...PreparedOptionFn) *PreparedStatement {
	stmt := &PreparedStatement{
		fn: fn,
	}

	for _, option := range options {
		option(stmt)
	}

	return stmt
}

// PreparedOptionFn options pattern used to define options while preparing a new statement.
type PreparedOptionFn func(*PreparedStatement)

// WithColumns sets the given columns as the columns which are returned by the
// prepared statement.
func WithColumns(columns Columns) PreparedOptionFn {
	return func(stmt *PreparedStatement) {
		stmt.columns = columns
	}
}

// WithParameters sets the given parameters as the parameters which are expected
// by the prepared statement.
func WithParameters(parameters []uint32) PreparedOptionFn {
	return func(stmt *PreparedStatement) {
		stmt.parameters = parameters
	}
}

type PreparedStatements []*PreparedStatement

type PreparedStatement struct {
	fn         PreparedStatementFn
	parameters []uint32
	columns    Columns
}

// SessionHandler represents a wrapper function defining the state of a single
// session. This function allows the user to wrap additional metadata around the
// shared context.
type SessionHandler func(ctx context.Context) (context.Context, error)

// StatementCache represents a cache which could be used to store and retrieve
// prepared statements bound to a name.
type StatementCache interface {
	// Set attempts to bind the given statement to the given name. Any
	// previously defined statement is overridden.
	Set(ctx context.Context, name string, fn *PreparedStatement) error
	// Get attempts to get the prepared statement for the given name. An error
	// is returned when no statement has been found.
	Get(ctx context.Context, name string) (*Statement, error)
	// Close is called at the end of a connection. Close releases all resources
	// held by the statement cache.
	Close()
}

// PortalCache represents a cache which could be used to bind and execute
// prepared statements with parameters.
type PortalCache interface {
	// Bind attempts to bind the given statement to the given name. Any
	// previously defined statement is overridden.
	Bind(ctx context.Context, name string, statement *Statement, parameters []Parameter, columns []FormatCode) error
	// Get attempts to get the portal for the given name. An error is returned
	// when no portal has been found.
	Get(ctx context.Context, name string) (*Portal, error)
	// Execute executes the prepared statement with the given name and parameters.
	Execute(ctx context.Context, name string, limit Limit, reader *buffer.Reader, writer *buffer.Writer) error
	// Close is called at the end of a connection. Close releases all resources
	// held by the portal cache.
	Close()
}

// ParallelPipelineConfig controls whether multiple Execute messages within a pipeline
// can run concurrently. When Enabled is true, the server may process Execute commands
// in parallel before the Sync message. When false, Execute commands are processed
// sequentially. Note that pipelining itself (batching multiple messages before Sync)
// is always supported; this setting only affects parallel execution of those messages.
type ParallelPipelineConfig struct {
	Enabled bool // when true, allows concurrent execution of pipelined Execute messages
}

type FlushFn func(ctx context.Context) error

type CloseFn func(ctx context.Context) error

// CancelRequestFn function called when a cancel request is received.
// The function receives the process ID and secret key from the cancel request.
// It should return an error if the cancel request cannot be processed.
type CancelRequestFn func(ctx context.Context, processID int32, secretKey int32) error

// OptionFn options pattern used to define and set options for the given
// PostgreSQL server.
type OptionFn func(*Server) error

// Statements sets the statement cache used to cache statements for later use. By
// default [DefaultStatementCache] is used.
func Statements(handler func() StatementCache) OptionFn {
	return func(srv *Server) error {
		srv.Statements = handler
		return nil
	}
}

// Portals sets the portals cache used to cache statements for later use. By
// default [DefaultPortalCache] is used.
func Portals(handler func() PortalCache) OptionFn {
	return func(srv *Server) error {
		srv.Portals = handler
		return nil
	}
}

// CloseConn sets the close connection handle inside the given server instance.
func CloseConn(fn CloseFn) OptionFn {
	return func(srv *Server) error {
		srv.CloseConn = fn
		return nil
	}
}

// TerminateConn sets the terminate connection handle inside the given server instance.
func TerminateConn(fn CloseFn) OptionFn {
	return func(srv *Server) error {
		srv.TerminateConn = fn
		return nil
	}
}

// FlushConn registers a handler for Flush messages.
//
// The provided handler is invoked when the frontend sends a Flush command.
// This allows the server to force any pending data in its output buffers
// to be delivered immediately.
//
// Typically, a Flush is sent after an extended-query command (except Sync)
// when the frontend wants to inspect results before issuing more commands.
func FlushConn(fn FlushFn) OptionFn {
	return func(srv *Server) error {
		srv.FlushConn = fn
		return nil
	}
}

// ParallelPipeline sets the parallel pipeline configuration for the server.
// This controls whether Execute events can run concurrently within a session.
func ParallelPipeline(config ParallelPipelineConfig) OptionFn {
	return func(srv *Server) error {
		srv.ParallelPipeline = config
		return nil
	}
}

// MessageBufferSize sets the message buffer size which is allocated once a new
// connection gets constructed. If a negative value or zero value is provided is
// the default message buffer size used.
func MessageBufferSize(size int) OptionFn {
	return func(srv *Server) error {
		srv.BufferedMsgSize = size
		return nil
	}
}

// ClientAuth sets the client authentication type which is used to authenticate
// the client connection. The default value is [tls.NoClientCert] which means
// that no client authentication is performed.
func ClientAuth(auth tls.ClientAuthType) OptionFn {
	return func(srv *Server) error {
		srv.ClientAuth = auth
		return nil
	}
}

// TLSConfig sets the given TLS config to be used to initialize a
// secure connection between the front-end (client) and back-end (server).
func TLSConfig(config *tls.Config) OptionFn {
	return func(srv *Server) error {
		srv.TLSConfig = config
		return nil
	}
}

// SessionAuthStrategy sets the given authentication strategy within the given
// server. The authentication strategy is called when a handshake is initiated.
func SessionAuthStrategy(fn AuthStrategy) OptionFn {
	return func(srv *Server) error {
		srv.Auth = fn
		return nil
	}
}

// BackendKeyData sets the function that generates backend key data for query cancellation.
// The provided function should return a process ID and secret key that can be used by
// clients to cancel queries. If not set, no BackendKeyData message will be sent.
func BackendKeyData(fn BackendKeyDataFunc) OptionFn {
	return func(srv *Server) error {
		srv.BackendKeyData = fn
		return nil
	}
}

// CancelRequest sets the cancel request handler for the server.
// This function is called when a client sends a cancel request with a process ID and secret key.
// The handler should validate the credentials and cancel the appropriate query if valid.
func CancelRequest(fn CancelRequestFn) OptionFn {
	return func(srv *Server) error {
		srv.CancelRequest = fn
		return nil
	}
}

// GlobalParameters sets the server parameters which are send back to the
// front-end (client) once a handshake has been established.
func GlobalParameters(params Parameters) OptionFn {
	return func(srv *Server) error {
		srv.Parameters = params
		return nil
	}
}

// Logger sets the given [slog.Logger] as the logger for the given server.
func Logger(logger *slog.Logger) OptionFn {
	return func(srv *Server) error {
		srv.logger = logger
		return nil
	}
}

// Version sets the PostgreSQL version for the server which is send back to the
// front-end (client) once a handshake has been established.
func Version(version string) OptionFn {
	return func(srv *Server) error {
		srv.Version = version
		return nil
	}
}

// WithShutdownTimeout sets the timeout duration for graceful shutdown.
// When Shutdown is called, the server will wait up to this duration for
// active connections to finish before forcing closure.
// A timeout of 0 means wait indefinitely (no timeout).
func WithShutdownTimeout(timeout time.Duration) OptionFn {
	return func(srv *Server) error {
		srv.ShutdownTimeout = timeout
		return nil
	}
}

// ExtendTypes provides the ability to extend the underlying connection types.
// Types registered inside the given [github.com/jackc/pgx/v5/pgtype.Map] are
// registered to all incoming connections.
func ExtendTypes(fn func(*pgtype.Map)) OptionFn {
	return func(srv *Server) error {
		srv.typeExtension = fn
		return nil
	}
}

// SessionMiddleware sets the given session handler within the underlying server. The
// session handler is called when a new connection is opened and authenticated
// allowing for additional metadata to be wrapped around the connection context.
func SessionMiddleware(fn SessionHandler) OptionFn {
	return func(srv *Server) error {
		if srv.Session == nil {
			srv.Session = fn
			return nil
		}

		wrapper := func(parent SessionHandler) SessionHandler {
			return func(ctx context.Context) (context.Context, error) {
				ctx, err := parent(ctx)
				if err != nil {
					return ctx, err
				}

				return fn(ctx)
			}
		}

		srv.Session = wrapper(srv.Session)
		return nil
	}
}

// QueryParameters represents a regex which could be used to identify and lookup
// parameters defined inside a given query. Parameters could be defined as
// [positional parameters] and non-positional parameters.
//
// [positional parameters]: https://www.postgresql.org/docs/15/sql-expressions.html#SQL-EXPRESSIONS-PARAMETERS-POSITIONAL
var QueryParameters = regexp.MustCompile(`\$(\d+)|\?`)

// ParseParameters attempts to parse the parameters in the given string and
// returns the expected parameters. This is necessary for the query protocol
// where the parameter types are expected to be defined in the extended query protocol.
func ParseParameters(query string) []uint32 {
	// NOTE: we have to lookup all parameters within the given query.
	// Parameters could represent positional parameters or anonymous
	// parameters. We return a zero parameter oid for each parameter
	// indicating that the given parameters could contain any type. We
	// could safely ignore the err check while converting given
	// parameters since ony matches are returned by the positional
	// parameter regex.
	matches := QueryParameters.FindAllStringSubmatch(query, -1)
	parameters := make([]uint32, 0, len(matches))
	for _, match := range matches {
		// NOTE: we have to check whether the returned match is a
		// positional parameter or an un-positional parameter.
		// SELECT * FROM users WHERE id = ?
		if match[1] == "" {
			parameters = append(parameters, 0)
		}

		position, _ := strconv.Atoi(match[1]) //nolint:errcheck
		if position > len(parameters) {
			parameters = parameters[:position]
		}
	}

	return parameters
}
